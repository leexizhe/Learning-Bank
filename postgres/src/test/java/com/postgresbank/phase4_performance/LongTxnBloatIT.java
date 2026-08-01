package com.postgresbank.phase4_performance;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static com.postgresbank.testsupport.TestSupport.readStatCounter;
import static com.postgresbank.testsupport.TestSupport.updateAccountOwner;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import javax.sql.DataSource;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The vacuum-horizon pitfall, made concrete: {@code VACUUM} can only reclaim a dead tuple once no
 * open transaction could still need to see it. A single long-running transaction anywhere in the
 * system - even one that never touches the table being vacuumed - holds that horizon back for
 * everyone. This opens one connection with a transaction that never commits, updates a row from a
 * second connection, runs {@code VACUUM} while the first transaction is still open (dead tuples
 * survive), then commits the first transaction and vacuums again (they're finally reclaimed).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LongTxnBloatIT extends TestContainerConfig {

  @Autowired private AccountRepository accounts;

  @Autowired private DataSource dataSource;

  @Test
  void openTransactionBlocksVacuumFromReclaimingDeadTuples() throws Exception {
    Account account = openAccount(accounts);

    try (Connection longRunning = dataSource.getConnection();
        Connection worker = dataSource.getConnection()) {

      longRunning.setAutoCommit(false);
      try (Statement openSnapshot = longRunning.createStatement()) {
        // Registers this connection's xmin in the procarray - a BEGIN with
        // no statement executed yet holds nothing back.
        openSnapshot.execute("select 1");
      }

      for (int i = 0; i < 50; i++) {
        updateAccountOwner(worker, account.getId(), "owner-" + i);
      }

      try (Statement st = worker.createStatement()) {
        st.execute("vacuum accounts");
      }

      long deadWhileBlocked = readStatCounter(worker, "accounts", "n_dead_tup");
      assertThat(deadWhileBlocked)
          .as(
              "VACUUM cannot reclaim tuples that might still be visible to the still-open transaction")
          .isGreaterThan(0);

      longRunning.commit();

      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () -> {
                try (Statement st = worker.createStatement()) {
                  st.execute("vacuum accounts");
                }
                assertThat(readStatCounter(worker, "accounts", "n_dead_tup"))
                    .as("with the long transaction gone, VACUUM can now reclaim the dead tuples")
                    .isLessThan(deadWhileBlocked);
              });
    }
  }
}
