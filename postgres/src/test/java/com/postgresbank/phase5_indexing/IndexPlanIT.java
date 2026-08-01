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
 * Asserts on the query plan itself, not on elapsed time — the same discipline as {@code
 * NPlusOneIT}, which counts Hibernate statements rather than timing them. A timing assertion tells
 * you the machine was busy; a plan assertion tells you what the database decided to do and why.
 *
 * <p>The headline is <b>not</b> "indexes are faster". It is that the planner chooses, on cost, per
 * query — and that <b>a sequential scan is sometimes correct</b>. Both accounts here are on the
 * same table with the same index available; the only difference is how much of the table each one
 * matches.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class IndexPlanIT extends TestContainerConfig {

  @Autowired private DataSource dataSource;

  private IndexFixture.Fixture fixture;

  /** Idempotent: after the first call this is a single count query and returns. */
  @BeforeEach
  void seed() throws Exception {
    fixture = IndexFixture.seedOnce(dataSource);
  }

  @Test
  void aSelectiveAccountLookupUsesTheIndex() throws Exception {
    QueryPlan plan =
        explain(
            dataSource,
            "select * from postings where account_id = ?",
            fixture.selectiveAccountId());

    assertThat(plan.usesIndex())
        .as(
            "a couple of hundred rows out of 100k - fetching them by index beats reading the table. Plan was: %s",
            plan.nodeTypes())
        .isTrue();
    assertThat(plan.actualRows()).isEqualTo(IndexFixture.SELECTIVE_ROWS);
  }

  /**
   * The half everyone forgets. Reading 99% of a table <em>through</em> an index is slower than
   * reading the table, because every index hit is a random jump back into the heap — so the planner
   * correctly declines the index. If your answer to "why isn't it using my index?" is always "add a
   * hint", this is the case that should change your mind.
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
   * The counterfactual, run inside a transaction that is always rolled back. DDL is transactional
   * in Postgres — unlike MySQL, where it commits implicitly and there is no undo — so dropping the
   * index to prove it matters cannot leak into the shared container even if the assertion fails.
   *
   * <p>Deliberately not {@code SET enable_indexscan = off}: that proves the planner obeys a knob,
   * not that the index is doing anything.
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
            .as("and it touches far more of the disk to find the same 200 rows")
            .isGreaterThan(100);
      } finally {
        c.rollback();
      }
    }

    assertThat(
            explain(
                    dataSource,
                    "select * from postings where account_id = ?",
                    fixture.selectiveAccountId())
                .usesIndex())
        .as("the rollback put both indexes back")
        .isTrue();
  }
}
