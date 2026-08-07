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
 * MVCC's other half: every {@code UPDATE} leaves the old tuple version behind as a "dead tuple" (see {@code
 * phase2_ledger.TupleVersionIT}) until something reclaims it. Repeated updates to one row visibly grow {@code
 * pg_stat_user_tables.n_dead_tup}; {@code VACUUM} is what brings it back down. Skip {@code VACUUM} (or block it - see
 * {@link LongTxnBloatIT}) and a hot row's table just keeps growing on disk even though its logical row count never
 * changes - that's table bloat.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class BloatIT extends TestContainerConfig {

    @Autowired
    private AccountRepository accounts;

    @Autowired
    private DataSource dataSource;

    @Test
    void repeatedUpdatesGrowDeadTuplesAndVacuumReclaimsThem() throws Exception {
        Account account = openAccount(accounts);

        try (Connection c = dataSource.getConnection()) {
            long deadBefore = readStatCounter(c, "accounts", "n_dead_tup");

            for (int i = 0; i < 100; i++) {
                updateAccountOwner(c, account.getId(), "owner-" + i);
            }

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(
                            readStatCounter(c, "accounts", "n_dead_tup"))
                    .as("100 updates to the same row should leave dead tuples behind")
                    .isGreaterThan(deadBefore));

            try (Statement st = c.createStatement()) {
                st.execute("vacuum accounts");
            }

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(
                            readStatCounter(c, "accounts", "n_dead_tup"))
                    .as("VACUUM should reclaim the dead tuples")
                    .isLessThanOrEqualTo(deadBefore));
        }
    }
}
