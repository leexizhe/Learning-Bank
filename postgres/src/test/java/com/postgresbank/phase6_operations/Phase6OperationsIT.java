package com.postgresbank.phase6_operations;

import static com.postgresbank.testsupport.ExplainSupport.explain;
import static com.postgresbank.testsupport.TestSupport.openAccount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.LedgerService;
import com.postgresbank.common.Posting;
import com.postgresbank.common.PostingRepository;
import com.postgresbank.testsupport.ExplainSupport.QueryPlan;
import com.postgresbank.testsupport.IndexFixture;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Running the thing in production: checkpointing a hot read, enforcing an invariant that spans rows, surviving the
 * database's own deadlocks, and migrating schema without an outage. Four separate operational concerns with four
 * separate fixtures — this block list is a checklist, not a narrative, and each one stands alone.
 *
 * <p>Three of them assert on a <b>SQLSTATE</b> rather than on a message or a Java exception type, which is the durable
 * way to test database behaviour: {@code 23514} (check violation, raised at COMMIT by a deferred trigger),
 * {@code 40P01} (deadlock detected), and {@code 25001} (active SQL transaction). Those codes are contracts; exception
 * class names and wording are not.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class Phase6OperationsIT extends TestContainerConfig {

    @Autowired
    private AccountRepository accounts;

    @Autowired
    private DataSource dataSource;

    /**
     * The snapshot must be <b>invisible</b>: every assertion here compares it against {@code LedgerService.balanceOf},
     * the full {@code SUM(postings)} it is meant to accelerate. A cache that returns a different answer from the thing
     * it caches is not an optimization, it is a bug, and in a ledger it is a bug that loses money.
     */
    @Nested
    class SnapshotTests {

        @Autowired
        private PostingRepository postings;

        @Autowired
        private LedgerService ledger;

        @Autowired
        private BalanceSnapshotService snapshotService;

        @Test
        void theSnapshotAgreesWithTheFullSumBeforeDuringAndAfterBeingTaken() {
            Account account = openAccount(accounts);
            post(account, 100, 250, -75);

            assertThat(snapshotService.balanceOf(account.getId()))
                    .as("with no snapshot yet it falls back to the full sum, so callers never need to know")
                    .isEqualTo(ledger.balanceOf(account.getId()));

            long snapshotted = snapshotService.takeSnapshot(account.getId());
            assertThat(snapshotted).isEqualTo(275);
            assertThat(snapshotService.balanceOf(account.getId()))
                    .as("immediately after the snapshot the delta is empty")
                    .isEqualTo(ledger.balanceOf(account.getId()));

            post(account, 1_000, -500);

            assertThat(snapshotService.balanceOf(account.getId()))
                    .as("snapshot + only the postings written since it was taken")
                    .isEqualTo(ledger.balanceOf(account.getId()))
                    .isEqualTo(775);

            // Re-taking it must be an upsert, not a second row - the primary key is the account, so a duplicate insert
            // would fail outright.
            snapshotService.takeSnapshot(account.getId());
            assertThat(snapshotService.balanceOf(account.getId())).isEqualTo(775);
        }

        /**
         * The point of the whole exercise, measured rather than asserted from theory: against the 100k-posting fixture
         * account, reading the balance through a snapshot touches a fraction of the buffers that summing the journal
         * does.
         */
        @Test
        void snapshottingCollapsesTheWorkOfReadingALargeAccountsBalance() throws Exception {
            IndexFixture.Fixture fixture = IndexFixture.seedOnce(dataSource);
            long accountId = fixture.bulkAccountId();

            QueryPlan fullSum = explain(
                    dataSource, "select coalesce(sum(amount_minor), 0) from postings where account_id = ?", accountId);

            snapshotService.takeSnapshot(accountId);
            long asOf = highWaterMark(accountId);

            QueryPlan deltaSum = explain(
                    dataSource,
                    "select coalesce(sum(amount_minor), 0) from postings where account_id = ? and id > ?",
                    accountId,
                    asOf);

            assertThat(deltaSum.sharedBlocks())
                    .as(
                            "reading only the postings since the checkpoint (%d blocks) instead of all %d of them (%d blocks)",
                            deltaSum.sharedBlocks(), IndexFixture.BULK_ROWS, fullSum.sharedBlocks())
                    .isLessThan(fullSum.sharedBlocks());
            assertThat(snapshotService.balanceOf(accountId))
                    .as("and it is still exactly the same number")
                    .isEqualTo(ledger.balanceOf(accountId));
        }

        private void post(Account account, long... amounts) {
            List<Posting> rows = IntStream.range(0, amounts.length)
                    .mapToObj(i -> new Posting(account, null, amounts[i], "snapshot-it"))
                    .toList();
            postings.saveAll(rows);
        }

        private long highWaterMark(long accountId) {
            Long max = postings.maxIdByAccountId(accountId);
            return max == null ? 0L : max;
        }
    }

    /**
     * "Double-entry is enforced by construction rather than by a database constraint" invites the obvious follow-up:
     * <b>so how would you enforce it in the database?</b> A plain {@code CHECK} cannot — it sees one row, and the
     * invariant spans the set of rows sharing a {@code transfer_id}. The answer is a <b>deferred constraint
     * trigger</b>.
     *
     * <p><b>Deferred is the entire point.</b> Halfway through writing a transfer the ledger holds one posting and is
     * legitimately unbalanced; a trigger firing at statement time would reject every transfer ever made.
     * {@code DEFERRABLE INITIALLY DEFERRED} moves the check to COMMIT, when the transaction is complete and the
     * invariant is supposed to hold. It is the database equivalent of "check the invariant at the boundary, not in the
     * middle".
     *
     * <p><b>Driven with raw JDBC and {@code autoCommit = false} on purpose.</b> Going through {@code @Transactional}
     * would surface the failure as a Spring {@code TransactionSystemException} thrown from somewhere inside the proxy,
     * which obscures the one thing worth seeing: <b>the INSERT succeeded and the COMMIT failed.</b> That is a genuinely
     * unusual shape — most constraint violations blow up at the statement — and it is what makes deferred constraints
     * worth talking about.
     */
    @Nested
    class DeferredBalanceConstraintTests {

        /** SQLSTATE 23514 — check_violation, raised explicitly by assert_transfer_balanced(). */
        private static final String CHECK_VIOLATION = "23514";

        @Test
        void aHalfWrittenTransferInsertsFineAndIsRejectedAtCommit() throws Exception {
            Account account = openAccount(accounts);

            try (Connection c = dataSource.getConnection()) {
                c.setAutoCommit(false);
                long transferId = insertTransfer(c, account);

                // Only the debit. No matching credit anywhere.
                insertPosting(c, account.getId(), transferId, -500);

                assertThatThrownBy(c::commit)
                        .as("the statement was fine; the constraint runs at COMMIT and finds the transfer unbalanced")
                        .isInstanceOf(SQLException.class)
                        .satisfies(e ->
                                assertThat(((SQLException) e).getSQLState()).isEqualTo(CHECK_VIOLATION));
            }
        }

        @Test
        void abalancedPairCommitsCleanly() throws Exception {
            Account debit = openAccount(accounts);
            Account credit = openAccount(accounts);

            try (Connection c = dataSource.getConnection()) {
                c.setAutoCommit(false);
                long transferId = insertTransfer(c, debit);

                insertPosting(c, debit.getId(), transferId, -500);
                insertPosting(c, credit.getId(), transferId, 500);

                c.commit();
            }
            // Reaching here without an exception is the assertion: the trigger fires on both rows and sees a zero sum
            // by the time it matters.
        }

        /**
         * The regression guard for phase 1. {@code phase1_isolation} posts standalone debits with a null
         * {@code transfer_id} — its whole write-skew demo depends on it — so the trigger's first statement returns
         * early for those. Without that guard this test fails and takes {@code Phase1IsolationIT} with it.
         */
        @Test
        void aStandalonePostingWithNoTransferIsUnaffected() throws Exception {
            Account account = openAccount(accounts);

            try (Connection c = dataSource.getConnection()) {
                c.setAutoCommit(false);
                insertPosting(c, account.getId(), null, -100);
                c.commit();
            }
        }

        private long insertTransfer(Connection c, Account from) throws SQLException {
            try (PreparedStatement ps = c.prepareStatement("""
                    insert into transfers (idempotency_key, from_account_id, to_account_id, amount_minor)
                    values (?, ?, ?, ?) returning id
                    """)) {
                ps.setString(1, "deferred-it-" + UUID.randomUUID());
                ps.setLong(2, from.getId());
                ps.setLong(3, from.getId());
                ps.setLong(4, 500);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        }

        private void insertPosting(Connection c, long accountId, Long transferId, long amountMinor)
                throws SQLException {
            try (PreparedStatement ps = c.prepareStatement(
                    "insert into postings (account_id, transfer_id, amount_minor, note) values (?, ?, ?, 'deferred-it')")) {
                ps.setLong(1, accountId);
                if (transferId == null) {
                    ps.setNull(2, java.sql.Types.BIGINT);
                } else {
                    ps.setLong(2, transferId);
                }
                ps.setLong(3, amountMinor);
                ps.executeUpdate();
            }
        }
    }

    /**
     * The database's own deadlock, completing the story {@code Phase1IsolationIT} starts with SQLSTATE {@code 40001}.
     *
     * <p>The concurrency module deadlocks two Java threads on two {@code ReentrantLock}s and proves it with
     * {@code ThreadMXBean}. This is the same circular wait one layer down, on two database rows — and the interesting
     * difference is what happens next. <b>A JVM deadlock hangs forever; Postgres breaks its own.</b> Every backend that
     * waits longer than {@code deadlock_timeout} (1s by default) runs a wait-graph check, and if it finds a cycle it
     * picks a victim and aborts it with SQLSTATE {@code 40P01}. The survivor commits.
     *
     * <p>That changes what "handle deadlocks" means. In Java you prevent them by lock ordering, because there is no
     * recovery. In Postgres you still prevent them the same way — {@code SELECT ... FOR UPDATE} in ascending id order
     * is the database version of {@code LockOrderedTransferService} — but you must also <em>retry</em>, because the
     * engine will occasionally shoot one of your transactions on purpose. Same retry loop that {@code 40001} needs,
     * which is why both codes belong in the same catch.
     *
     * <p>Which side loses is Postgres's choice, so the test asserts that <b>exactly one</b> transaction failed with
     * {@code 40P01} rather than assuming which. Assuming would make this flaky for no reason.
     */
    @Nested
    class PgDeadlockTests {

        /** SQLSTATE 40P01 — deadlock_detected. */
        private static final String DEADLOCK_DETECTED = "40P01";

        private final ExecutorService pool = Executors.newFixedThreadPool(2);

        @Test
        void twoTransactionsLockingTwoRowsInOppositeOrdersDeadlockAndPostgresKillsOne() throws Exception {
            Account first = openAccount(accounts);
            Account second = openAccount(accounts);

            // Both take their first row lock, meet at the barrier, then reach for the other's - so the cycle is
            // guaranteed rather than raced for.
            CountDownLatch bothHoldFirstLock = new CountDownLatch(2);
            AtomicReference<String> failureState = new AtomicReference<>();

            Future<?> forward =
                    pool.submit(lockInOrder(first.getId(), second.getId(), bothHoldFirstLock, failureState));
            Future<?> reverse =
                    pool.submit(lockInOrder(second.getId(), first.getId(), bothHoldFirstLock, failureState));

            forward.get(30, TimeUnit.SECONDS);
            reverse.get(30, TimeUnit.SECONDS);

            assertThat(failureState.get())
                    .as("Postgres detected the cycle and aborted exactly one of the two - it does not hang")
                    .isEqualTo(DEADLOCK_DETECTED);
        }

        private Runnable lockInOrder(
                long firstId, long secondId, CountDownLatch bothHoldFirstLock, AtomicReference<String> failureState) {
            return () -> {
                try (Connection c = dataSource.getConnection()) {
                    c.setAutoCommit(false);
                    try {
                        lockAccount(c, firstId);
                        bothHoldFirstLock.countDown();
                        bothHoldFirstLock.await();

                        lockAccount(c, secondId);
                        c.commit();
                    } catch (SQLException e) {
                        // Only one side gets here; record its SQLSTATE.
                        failureState.compareAndSet(null, e.getSQLState());
                        c.rollback();
                    }
                } catch (SQLException | InterruptedException e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    throw new IllegalStateException(e);
                }
            };
        }

        private void lockAccount(Connection c, long accountId) throws SQLException {
            try (PreparedStatement ps = c.prepareStatement("select id from accounts where id = ? for update")) {
                ps.setLong(1, accountId);
                ps.executeQuery().close();
            }
        }
    }

    /**
     * Zero-downtime schema migration, in the one case small enough to demonstrate.
     *
     * <p>Plain {@code CREATE INDEX} takes a {@code SHARE} lock: reads continue, but <b>every write to the table blocks
     * for the entire build</b>. On a large table that is an outage, and it is the difference between "the migration was
     * slow" and "the migration took the site down". {@code CREATE INDEX CONCURRENTLY} takes only
     * {@code SHARE UPDATE EXCLUSIVE}, so writes carry on throughout.
     *
     * <p>What it costs: two passes over the table instead of one, so it is slower in wall-clock terms; it cannot run
     * inside a transaction block; and if it fails partway it leaves behind an <b>invalid</b> index that is still
     * maintained on every write but never used for reads — the trap being that the migration looks finished and the
     * index looks present. {@code pg_index.indisvalid} is where you find out, and dropping and rebuilding is the fix.
     *
     * <p>The rest of the toolkit, which these tests are too small to show: {@code ADD CONSTRAINT ... NOT VALID}
     * followed by {@code VALIDATE CONSTRAINT}, so the expensive full-table check happens without holding a strong lock;
     * and always {@code SET lock_timeout} before DDL, so a migration that cannot get its lock fails fast instead of
     * queueing — with every query arriving behind it queueing too, which is how a lock wait becomes an outage.
     */
    @Nested
    class ConcurrentIndexTests {

        private static final String INDEX_NAME = "idx_concurrent_index_it_note";

        /** SQLSTATE 25001 — active_sql_transaction. */
        private static final String ACTIVE_SQL_TRANSACTION = "25001";

        private final ExecutorService pool = Executors.newFixedThreadPool(1);

        @Test
        void writesContinueWhileTheIndexIsBuiltConcurrently() throws Exception {
            Account account = openAccount(accounts);
            CountDownLatch buildStarted = new CountDownLatch(1);

            try {
                Future<Integer> writes = pool.submit(() -> {
                    buildStarted.await();
                    int inserted = 0;
                    for (int i = 0; i < 50; i++) {
                        insertPosting(account.getId(), i);
                        inserted++;
                    }
                    return inserted;
                });

                try (Connection c = dataSource.getConnection()) {
                    // Autocommit is mandatory, not stylistic - see the other test.
                    c.setAutoCommit(true);
                    try (Statement st = c.createStatement()) {
                        buildStarted.countDown();
                        st.execute("create index concurrently if not exists " + INDEX_NAME + " on postings (note)");
                    }
                }

                assertThat(writes.get(60, TimeUnit.SECONDS))
                        .as("inserts ran to completion during the build rather than blocking on it")
                        .isEqualTo(50);

                assertThat(isValid(INDEX_NAME))
                        .as("the build finished cleanly; an interrupted one would leave indisvalid = false")
                        .isTrue();
            } finally {
                dropConcurrently();
            }
        }

        /**
         * The first thing that bites anyone trying this from application code, and the reason it cannot simply be
         * dropped into a {@code @Transactional} migration method: {@code CONCURRENTLY} needs to commit between its own
         * passes, so it refuses to run inside a transaction block at all.
         */
        @Test
        void createIndexConcurrentlyRefusesToRunInsideATransaction() throws Exception {
            try (Connection c = dataSource.getConnection()) {
                c.setAutoCommit(false);
                try (Statement st = c.createStatement()) {
                    assertThatThrownBy(
                                    () -> st.execute("create index concurrently idx_never_created on postings (note)"))
                            .isInstanceOf(SQLException.class)
                            .satisfies(e ->
                                    assertThat(((SQLException) e).getSQLState()).isEqualTo(ACTIVE_SQL_TRANSACTION));
                } finally {
                    c.rollback();
                }
            }
        }

        private void insertPosting(long accountId, int i) throws SQLException {
            try (Connection c = dataSource.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "insert into postings (account_id, transfer_id, amount_minor, note) values (?, null, 1, ?)")) {
                ps.setLong(1, accountId);
                ps.setString(2, "concurrent-build-" + i);
                ps.executeUpdate();
            }
        }

        private boolean isValid(String indexName) throws SQLException {
            try (Connection c = dataSource.getConnection();
                    PreparedStatement ps = c.prepareStatement(
                            "select i.indisvalid from pg_index i join pg_class r on r.oid = i.indexrelid where r.relname = ?")) {
                ps.setString(1, indexName);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() && rs.getBoolean(1);
                }
            }
        }

        /** Dropping concurrently is also non-transactional, for the same reason. */
        private void dropConcurrently() throws SQLException {
            try (Connection c = dataSource.getConnection()) {
                c.setAutoCommit(true);
                try (Statement st = c.createStatement()) {
                    st.execute("drop index concurrently if exists " + INDEX_NAME);
                }
            }
        }
    }
}
