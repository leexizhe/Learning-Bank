package com.postgresbank.phase6_operations;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Zero-downtime schema migration, in the one case small enough to demonstrate.
 *
 * <p>Plain {@code CREATE INDEX} takes a {@code SHARE} lock: reads continue, but <b>every write to
 * the table blocks for the entire build</b>. On a large table that is an outage, and it is the
 * difference between "the migration was slow" and "the migration took the site down". {@code CREATE
 * INDEX CONCURRENTLY} takes only {@code SHARE UPDATE EXCLUSIVE}, so writes carry on throughout.
 *
 * <p>What it costs: two passes over the table instead of one, so it is slower in wall-clock terms;
 * it cannot run inside a transaction block; and if it fails partway it leaves behind an
 * <b>invalid</b> index that is still maintained on every write but never used for reads — the trap
 * being that the migration looks finished and the index looks present. {@code pg_index.indisvalid}
 * is where you find out, and dropping and rebuilding is the fix.
 *
 * <p>The rest of the toolkit, which this test is too small to show: {@code ADD CONSTRAINT ... NOT
 * VALID} followed by {@code VALIDATE CONSTRAINT}, so the expensive full-table check happens without
 * holding a strong lock; and always {@code SET lock_timeout} before DDL, so a migration that cannot
 * get its lock fails fast instead of queueing — with every query arriving behind it queueing too,
 * which is how a lock wait becomes an outage.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ConcurrentIndexIT extends TestContainerConfig {

  private static final String INDEX_NAME = "idx_concurrent_index_it_note";

  /** SQLSTATE 25001 — active_sql_transaction. */
  private static final String ACTIVE_SQL_TRANSACTION = "25001";

  private final ExecutorService pool = Executors.newFixedThreadPool(1);

  @Autowired private AccountRepository accounts;

  @Autowired private DataSource dataSource;

  @Test
  void writesContinueWhileTheIndexIsBuiltConcurrently() throws Exception {
    Account account = openAccount(accounts);
    CountDownLatch buildStarted = new CountDownLatch(1);

    try {
      Future<Integer> writes =
          pool.submit(
              () -> {
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
          st.execute(
              "create index concurrently if not exists " + INDEX_NAME + " on postings (note)");
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
   * The first thing that bites anyone trying this from application code, and the reason it cannot
   * simply be dropped into a {@code @Transactional} migration method: {@code CONCURRENTLY} needs to
   * commit between its own passes, so it refuses to run inside a transaction block at all.
   */
  @Test
  void createIndexConcurrentlyRefusesToRunInsideATransaction() throws Exception {
    try (Connection c = dataSource.getConnection()) {
      c.setAutoCommit(false);
      try (Statement st = c.createStatement()) {
        assertThatThrownBy(
                () -> st.execute("create index concurrently idx_never_created on postings (note)"))
            .isInstanceOf(SQLException.class)
            .satisfies(
                e ->
                    assertThat(((SQLException) e).getSQLState()).isEqualTo(ACTIVE_SQL_TRANSACTION));
      } finally {
        c.rollback();
      }
    }
  }

  private void insertPosting(long accountId, int i) throws SQLException {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps =
            c.prepareStatement(
                "insert into postings (account_id, transfer_id, amount_minor, note) values (?, null, 1, ?)")) {
      ps.setLong(1, accountId);
      ps.setString(2, "concurrent-build-" + i);
      ps.executeUpdate();
    }
  }

  private boolean isValid(String indexName) throws SQLException {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps =
            c.prepareStatement(
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
