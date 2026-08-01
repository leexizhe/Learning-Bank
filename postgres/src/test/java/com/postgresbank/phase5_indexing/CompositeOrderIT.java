package com.postgresbank.phase5_indexing;

import static com.postgresbank.testsupport.ExplainSupport.explain;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.testsupport.ExplainSupport.QueryPlan;
import com.postgresbank.testsupport.IndexFixture;
import java.sql.Connection;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Composite index column order: <b>equality columns first, then the range or sort column</b>. The
 * query is {@code WHERE account_id = ? ORDER BY created_at DESC, id DESC}, and the claim is that
 * {@code (account_id, created_at, id)} serves it while {@code (created_at, account_id)} does not.
 *
 * <p><b>What these tests assert is the absence of a {@code Sort} node, not index-vs-seq-scan — and
 * that distinction matters on Postgres 18.</b> The textbook version of this lesson is "a composite
 * index is unusable unless your predicate includes its leading column". PG18's B-tree <b>skip
 * scan</b> softens that: the planner can now hop through the distinct values of a leading column it
 * has no predicate for, so the "wrong" index may well be <em>usable</em> here. What skip scan
 * cannot do is hand back rows already sorted by a trailing column. So "the right index removes the
 * Sort, the wrong one doesn't" is true on 16 and on 18, and it is the sharper claim anyway: <b>an
 * index earns its keep by satisfying the ORDER BY, not merely by being touched.</b>
 *
 * <p>A {@code Sort} node on a large result is also where the real cost hides — it has to
 * materialise every matching row before returning the first one, which is exactly what a {@code
 * LIMIT} was supposed to avoid.
 *
 * <p>The deliberately-wrong index is created inside a transaction that is always rolled back, so
 * {@code schema.sql} keeps containing only indexes worth shipping.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CompositeOrderIT extends TestContainerConfig {

  private static final String HISTORY_QUERY =
      "select id, amount_minor, created_at from postings where account_id = ? order by created_at desc, id desc limit 20";

  @Autowired private DataSource dataSource;

  private IndexFixture.Fixture fixture;

  @BeforeEach
  void seed() throws Exception {
    fixture = IndexFixture.seedOnce(dataSource);
  }

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

        // In practice this comes back as [Limit, Incremental Sort, Index
        // Scan]: the wrong index does supply created_at order, which is a
        // prefix of what the query wants, so Postgres sorts only within
        // each group of equal timestamps rather than sorting everything.
        // Cheaper than a full Sort - and still a sort, which is the point.
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
