package com.postgresbank.phase5_indexing;

import static com.postgresbank.testsupport.ExplainSupport.explain;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Posting;
import com.postgresbank.common.PostingRepository;
import com.postgresbank.testsupport.ExplainSupport.QueryPlan;
import com.postgresbank.testsupport.IndexFixture;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

/**
 * Everything this phase asserts is a <b>query plan</b> or a <b>buffer count</b>, never elapsed time — the same
 * discipline as {@code Phase4PerformanceIT.NPlusOneTests}, which counts Hibernate statements rather than timing them. A
 * timing assertion tells you the machine was busy; a plan assertion tells you what the database decided to do and why.
 *
 * <p>The four blocks build on one shared 100k-row fixture and answer four different questions about it: does the
 * planner use the index (and is it right when it declines), does the index order match the {@code ORDER BY}, does a
 * partial index cover the query's predicate, and does a deep page have to read everything before it.
 *
 * <p>Two of them change the index set inside a transaction that is always rolled back — one dropping the real indexes,
 * one adding a deliberately-wrong one — so {@code schema.sql} keeps containing only indexes worth shipping. DDL is
 * transactional in Postgres — unlike MySQL, where it commits implicitly and there is no undo — so this cannot leak into
 * the shared container even if an assertion fails.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class Phase5IndexingIT extends TestContainerConfig {

    @Autowired
    private DataSource dataSource;

    private IndexFixture.Fixture fixture;

    /** Memoized in {@link IndexFixture}, so only the first call of the run touches the database. */
    @BeforeEach
    void seed() throws Exception {
        fixture = IndexFixture.seedOnce(dataSource);
    }

    /**
     * The headline is <b>not</b> "indexes are faster". It is that the planner chooses, on cost, per query — and that
     * <b>a sequential scan is sometimes correct</b>. Both accounts here are on the same table with the same index
     * available; the only difference is how much of the table each one matches.
     */
    @Nested
    class IndexPlanTests {

        @Test
        void aSelectiveAccountLookupUsesTheIndex() throws Exception {
            QueryPlan plan =
                    explain(dataSource, "select * from postings where account_id = ?", fixture.selectiveAccountId());

            assertThat(plan.usesIndex())
                    .as(
                            "2,000 rows out of 100k - fetching them by index beats reading the table. Plan was: %s",
                            plan.nodeTypes())
                    .isTrue();
            assertThat(plan.actualRows()).isEqualTo(IndexFixture.SELECTIVE_ROWS);
        }

        /**
         * The half everyone forgets. Reading 99% of a table <em>through</em> an index is slower than reading the table,
         * because every index hit is a random jump back into the heap — so the planner correctly declines the index. If
         * your answer to "why isn't it using my index?" is always "add a hint", this is the case that should change
         * your mind.
         */
        @Test
        void aBulkAccountLookupCorrectlyPrefersASequentialScan() throws Exception {
            QueryPlan plan =
                    explain(dataSource, "select * from postings where account_id = ?", fixture.bulkAccountId());

            assertThat(plan.rootNodeType())
                    .as("matching most of the table, so the index would cost more than it saves")
                    .isEqualTo("Seq Scan");
            assertThat(plan.actualRows()).isGreaterThanOrEqualTo(IndexFixture.BULK_ROWS);
        }

        /**
         * The counterfactual, run inside a transaction that is always rolled back.
         *
         * <p>Deliberately not {@code SET enable_indexscan = off}: that proves the planner obeys a knob, not that the
         * index is doing anything.
         */
        @Test
        void droppingTheIndexTurnsTheSelectiveLookupIntoASeqScan() throws Exception {
            try (Connection c = dataSource.getConnection()) {
                c.setAutoCommit(false);
                try (Statement st = c.createStatement()) {
                    st.execute("drop index idx_postings_account_id");
                    st.execute("drop index idx_postings_account_created_id");

                    QueryPlan plan =
                            explain(c, "select * from postings where account_id = ?", fixture.selectiveAccountId());

                    assertThat(plan.rootNodeType())
                            .as("same query, same data, no index - the only option left is to read everything")
                            .isEqualTo("Seq Scan");
                    assertThat(plan.sharedBlocks())
                            .as("and it touches far more of the disk to find the same rows")
                            .isGreaterThan(100);
                } finally {
                    c.rollback();
                }
            }

            assertThat(explain(dataSource, "select * from postings where account_id = ?", fixture.selectiveAccountId())
                            .usesIndex())
                    .as("the rollback put both indexes back")
                    .isTrue();
        }
    }

    /**
     * Composite index column order: <b>equality columns first, then the range or sort column</b>. The query is
     * {@code WHERE account_id = ? ORDER BY created_at DESC, id DESC}, and the claim is that
     * {@code (account_id, created_at, id)} serves it while {@code (created_at, account_id)} does not.
     *
     * <p><b>What these tests assert is the absence of a {@code Sort} node, not index-vs-seq-scan — and that distinction
     * matters on Postgres 18.</b> The textbook version of this lesson is "a composite index is unusable unless your
     * predicate includes its leading column". PG18's B-tree <b>skip scan</b> softens that: the planner can now hop
     * through the distinct values of a leading column it has no predicate for, so the "wrong" index may well be
     * <em>usable</em> here. What skip scan cannot do is hand back rows already sorted by a trailing column. So "the
     * right index removes the Sort, the wrong one doesn't" is true on 16 and on 18, and it is the sharper claim anyway:
     * <b>an index earns its keep by satisfying the ORDER BY, not merely by being touched.</b>
     *
     * <p>A {@code Sort} node on a large result is also where the real cost hides — it has to materialise every matching
     * row before returning the first one, which is exactly what a {@code LIMIT} was supposed to avoid.
     */
    @Nested
    class CompositeOrderTests {

        private static final String HISTORY_QUERY =
                "select id, amount_minor, created_at from postings where account_id = ? order by created_at desc, id desc limit 20";

        @Test
        void theRightColumnOrderSatisfiesTheOrderByWithNoSortStep() throws Exception {
            QueryPlan plan = explain(dataSource, HISTORY_QUERY, fixture.selectiveAccountId());

            assertThat(plan.hasSortNode())
                    .as(
                            "(account_id, created_at DESC, id DESC) already returns rows in the required order. Plan: %s",
                            plan.nodeTypes())
                    .isFalse();
            assertThat(plan.indexNames())
                    .as("and the intended index is the one chosen")
                    .contains("idx_postings_account_created_id");
        }

        @Test
        void theWrongColumnOrderForcesPostgresToSort() throws Exception {
            try (Connection c = dataSource.getConnection()) {
                c.setAutoCommit(false);
                try (Statement st = c.createStatement()) {
                    st.execute("drop index idx_postings_account_created_id");
                    st.execute("drop index idx_postings_account_id");
                    st.execute("create index tmp_wrong_order on postings (created_at, account_id)");

                    QueryPlan plan = explain(c, HISTORY_QUERY, fixture.selectiveAccountId());

                    // In practice this comes back as [Limit, Incremental Sort, Index Scan]: the wrong index does supply
                    // created_at order, which is a prefix of what the query wants, so Postgres sorts only within each
                    // group of equal timestamps rather than sorting everything. Cheaper than a full Sort - and still a
                    // sort, which is the point.
                    assertThat(plan.hasSortNode())
                            .as(
                                    "leading with created_at cannot produce rows grouped by account and ordered within it, so Postgres sorts. Plan: %s",
                                    plan.nodeTypes())
                            .isTrue();
                } finally {
                    c.rollback();
                }
            }
        }
    }

    /**
     * A <b>partial index</b> indexes only the rows matching its own {@code WHERE} clause.
     * {@code idx_outbox_unpublished} covers {@code (id) WHERE NOT published}, which is the entire query the relay ever
     * runs.
     *
     * <p><b>Why this shape fits an outbox so exactly.</b> The table grows forever — it is a log — but the interesting
     * set is only ever the handful of rows not yet relayed. A full index on {@code published} would grow with history
     * and be almost entirely made of rows nobody will ever ask about; the partial index stays proportional to the
     * <em>backlog</em>. Same story for the {@code payment_jobs} queue, and for any "pending / unprocessed / not yet
     * sent" flag, which is most of them.
     *
     * <p>The requirement to remember: the planner can only use a partial index when it can prove the query's predicate
     * implies the index's. {@code WHERE NOT published} matches; {@code WHERE published = false} also matches; a query
     * with no predicate on {@code published} at all does not, and silently gets a sequential scan.
     */
    @Nested
    class PartialIndexTests {

        private static final int PUBLISHED_ROWS = 50_000;

        /**
         * Seeds bulk history as <b>published</b> only. Unpublished rows would be swept up by the scheduled
         * {@code OutboxRelay} within two seconds anyway, and — more importantly — leaving a backlog behind would change
         * what other ITs in this module see. The plan choice is what is under test, not the row count.
         */
        @BeforeEach
        void seedPublishedHistory() throws Exception {
            try (Connection c = dataSource.getConnection()) {
                c.setAutoCommit(true);
                if (countPublished(c) >= PUBLISHED_ROWS) {
                    return;
                }
                try (PreparedStatement ps = c.prepareStatement("""
                        insert into outbox (event_id, payload, published)
                        select gen_random_uuid(), 'partial-index-fixture-' || g, true
                        from generate_series(1, ?) g
                        """)) {
                    ps.setInt(1, PUBLISHED_ROWS);
                    ps.executeUpdate();
                }
                try (Statement st = c.createStatement()) {
                    st.execute("analyze outbox");
                }
            }
        }

        @Test
        void theRelaysQueryIsServedByThePartialIndex() throws Exception {
            QueryPlan plan = explain(dataSource, "select id from outbox where not published order by id limit 100");

            assertThat(plan.indexNames())
                    .as(
                            "the predicate matches the index's own WHERE clause, so it is usable. Plan: %s",
                            plan.nodeTypes())
                    .contains("idx_outbox_unpublished");
        }

        /**
         * The other half, and the part that catches people out: the same table, a query that does not mention
         * {@code published}, and the partial index is simply unavailable — it does not contain the rows being asked
         * for.
         */
        @Test
        void aQueryWithoutThePredicateCannotUseThePartialIndex() throws Exception {
            QueryPlan plan = explain(dataSource, "select count(*) from outbox");

            assertThat(plan.indexNames())
                    .as(
                            "no predicate on published means the partial index cannot answer this. Plan: %s",
                            plan.nodeTypes())
                    .doesNotContain("idx_outbox_unpublished");
        }

        private long countPublished(Connection c) throws Exception {
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("select count(*) from outbox where published")) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /**
     * {@code LIMIT/OFFSET} versus keyset pagination, compared on <b>buffers read</b> rather than on wall-clock time.
     *
     * <p><b>Why OFFSET degrades.</b> There is no way to skip rows without producing them. {@code OFFSET 50000} reads
     * fifty thousand rows through the index, throws every one away, and returns the next twenty. The cost is
     * O(offset + size), so page 1 is instant, page 2500 is slow, and the endpoint gets steadily worse in a way that
     * never shows up in testing with 10 rows of data.
     *
     * <p>Keyset instead says "everything strictly after this row", which the B-tree can seek to directly: O(page size)
     * at any depth. The catch is real and worth stating — no page numbers, no total count, no jumping to an arbitrary
     * page — which is why both endpoints exist rather than one replacing the other.
     */
    @Nested
    class KeysetPaginationTests {

        private static final int PAGE_SIZE = 20;
        private static final int DEEP_OFFSET = 50_000;

        @Autowired
        private PostingRepository postings;

        @Test
        void aDeepOffsetPageReadsFarMoreOfTheIndexThanTheEquivalentKeysetPage() throws Exception {
            long accountId = fixture.bulkAccountId();
            Cursor cursor = cursorAtOffset(accountId, DEEP_OFFSET);

            QueryPlan offsetPlan = explain(
                    dataSource,
                    "select id, created_at from postings where account_id = ? order by created_at desc, id desc limit ? offset ?",
                    accountId,
                    PAGE_SIZE,
                    DEEP_OFFSET);

            QueryPlan keysetPlan = explain(
                    dataSource,
                    "select id, created_at from postings where account_id = ? and (created_at, id) < (?, ?) order by created_at desc, id desc limit ?",
                    accountId,
                    Timestamp.from(cursor.createdAt()),
                    cursor.id(),
                    PAGE_SIZE);

            assertThat(keysetPlan.sharedBlocks())
                    .as(
                            "keyset seeks straight to the row; offset reads and discards %d of them first (offset=%d blocks, keyset=%d blocks)",
                            DEEP_OFFSET, offsetPlan.sharedBlocks(), keysetPlan.sharedBlocks())
                    .isLessThan(offsetPlan.sharedBlocks());

            assertThat(keysetPlan.hasSortNode())
                    .as("the composite index already provides the order, so neither plan needs a Sort")
                    .isFalse();
        }

        /**
         * Both sides go through Hibernate on purpose. Reading the cursor with raw JDBC
         * ({@code ResultSet.getTimestamp().toInstant()}) and then feeding that {@code Instant} back through a
         * Hibernate-bound parameter compares two different interpretations of a {@code TIMESTAMP WITHOUT TIME ZONE} —
         * the driver resolves it against the JVM default zone, Hibernate against its own — and the seek silently lands
         * hours away from where the offset page is. Which is itself the argument for {@code TIMESTAMPTZ} over
         * {@code TIMESTAMP} for anything a cursor is built from.
         */
        @Test
        void bothApproachesReturnTheSamePage() {
            long accountId = fixture.bulkAccountId();
            Sort newestFirst = Sort.by(Sort.Order.desc("createdAt"), Sort.Order.desc("id"));

            // The last row of the preceding page is exactly the cursor a client holds.
            List<Posting> precedingPage = postings.findByAccountId(
                            accountId, PageRequest.of(DEEP_OFFSET / PAGE_SIZE - 1, PAGE_SIZE, newestFirst))
                    .getContent();
            Posting cursor = precedingPage.get(precedingPage.size() - 1);

            List<Long> viaOffset = postings
                    .findByAccountId(accountId, PageRequest.of(DEEP_OFFSET / PAGE_SIZE, PAGE_SIZE, newestFirst))
                    .getContent()
                    .stream()
                    .map(Posting::getId)
                    .toList();

            List<Long> viaKeyset =
                    postings.seekByAccountId(accountId, cursor.getCreatedAt(), cursor.getId(), PAGE_SIZE).stream()
                            .map(Posting::getId)
                            .toList();

            assertThat(viaKeyset)
                    .as("same rows, same order - keyset is a cheaper route to an identical answer, not a different one")
                    .isEqualTo(viaOffset);
            assertThat(viaKeyset).hasSize(PAGE_SIZE);
        }

        /**
         * The (created_at, id) of the last row before the page at {@code offset} — i.e. the cursor a client would hold.
         */
        private Cursor cursorAtOffset(long accountId, int offset) throws Exception {
            try (Connection c = dataSource.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "select created_at, id from postings where account_id = ? order by created_at desc, id desc limit 1 offset ?")) {
                ps.setLong(1, accountId);
                ps.setInt(2, offset - 1);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return new Cursor(rs.getTimestamp(1).toInstant(), rs.getLong(2));
                }
            }
        }

        private record Cursor(Instant createdAt, long id) {}
    }
}
