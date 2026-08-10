package com.postgresbank.phase4_performance;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static com.postgresbank.testsupport.TestSupport.readStatCounter;
import static com.postgresbank.testsupport.TestSupport.updateAccountOwner;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.Posting;
import com.postgresbank.common.PostingRepository;
import jakarta.persistence.EntityManagerFactory;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.awaitility.Awaitility;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;

/**
 * Four ways an application that works correctly can still be slow: dead tuples nobody reclaims, dead tuples nobody
 * <em>can</em> reclaim, a query count that scales with the result set, and an endpoint that loads a whole table to
 * return ten rows.
 *
 * <p>{@link BloatTests} and {@link LongTxnBloatTests} are the same experiment with one variable changed, and are worth
 * reading in that order: both update a row in a loop, vacuum, and watch {@code n_dead_tup}. The second simply leaves a
 * transaction open while doing it, and that alone is enough to stop {@code VACUUM} reclaiming anything.
 *
 * <p>Note what none of these blocks assert: elapsed time. Bloat is measured in dead tuples, N+1 in prepared statements,
 * pagination in row counts — each is the mechanism itself rather than a stopwatch reading that would flake in CI.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class Phase4PerformanceIT extends TestContainerConfig {

    @Autowired
    private AccountRepository accounts;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PostingRepository postings;

    /**
     * MVCC's other half: every {@code UPDATE} leaves the old tuple version behind as a "dead tuple" (see
     * {@code phase2_ledger.Phase2LedgerIT.TupleVersionTests}) until something reclaims it. Repeated updates to one row
     * visibly grow {@code pg_stat_user_tables.n_dead_tup}; {@code VACUUM} is what brings it back down. Skip
     * {@code VACUUM} (or block it - see {@link LongTxnBloatTests}) and a hot row's table just keeps growing on disk
     * even though its logical row count never changes - that's table bloat.
     */
    @Nested
    class BloatTests {

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

    /**
     * The vacuum-horizon pitfall, made concrete: {@code VACUUM} can only reclaim a dead tuple once no open transaction
     * could still need to see it. A single long-running transaction anywhere in the system - even one that never
     * touches the table being vacuumed - holds that horizon back for everyone. This opens one connection with a
     * transaction that never commits, updates a row from a second connection, runs {@code VACUUM} while the first
     * transaction is still open (dead tuples survive), then commits the first transaction and vacuums again (they're
     * finally reclaimed).
     */
    @Nested
    class LongTxnBloatTests {

        @Test
        void openTransactionBlocksVacuumFromReclaimingDeadTuples() throws Exception {
            Account account = openAccount(accounts);

            try (Connection longRunning = dataSource.getConnection();
                    Connection worker = dataSource.getConnection()) {

                longRunning.setAutoCommit(false);
                try (Statement openSnapshot = longRunning.createStatement()) {
                    // Registers this connection's xmin in the procarray - a BEGIN with no statement executed yet holds
                    // nothing back.
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
                        .as("VACUUM cannot reclaim tuples that might still be visible to the still-open transaction")
                        .isGreaterThan(0);

                longRunning.commit();

                Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
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

    /**
     * Same data, same result, two very different query counts. This doesn't trust "JOIN FETCH should be faster" as a
     * rule of thumb - it reads Hibernate's own {@code Statistics} and counts the actual JDBC statements each path
     * executes: 1 (accounts) + N (one lazy postings load per account) for the naive path, exactly 1 for the
     * fetch-joined path.
     */
    @Nested
    class NPlusOneTests {

        private static final int ACCOUNT_COUNT = 5;
        private static final int POSTINGS_PER_ACCOUNT = 3;

        @Autowired
        private AccountHistoryService historyService;

        @Autowired
        private EntityManagerFactory entityManagerFactory;

        @Test
        void eagerLazyLoadingIssuesOneQueryPerAccountOnTopOfTheInitialSelect() {
            List<Long> ids = seedAccountsWithPostings();
            Statistics stats = statistics();
            stats.clear();

            int totalPostings = historyService.loadNPlusOne(ids);

            assertThat(totalPostings).isEqualTo(ACCOUNT_COUNT * POSTINGS_PER_ACCOUNT);
            assertThat(stats.getPrepareStatementCount())
                    .as("1 query for the accounts + 1 lazy-load per account = N+1")
                    .isEqualTo(1 + ACCOUNT_COUNT);
        }

        @Test
        void joinFetchLoadsEverythingInOneQuery() {
            List<Long> ids = seedAccountsWithPostings();
            Statistics stats = statistics();
            stats.clear();

            int totalPostings = historyService.loadFetchJoined(ids);

            assertThat(totalPostings).isEqualTo(ACCOUNT_COUNT * POSTINGS_PER_ACCOUNT);
            assertThat(stats.getPrepareStatementCount())
                    .as("JOIN FETCH pulls accounts and postings in a single round trip")
                    .isEqualTo(1);
        }

        private List<Long> seedAccountsWithPostings() {
            List<Account> newAccounts = IntStream.range(0, ACCOUNT_COUNT)
                    .mapToObj(i -> new Account("owner-" + UUID.randomUUID()))
                    .toList();
            List<Account> savedAccounts = accounts.saveAll(newAccounts);

            List<Posting> newPostings = savedAccounts.stream()
                    .flatMap(account -> IntStream.range(0, POSTINGS_PER_ACCOUNT)
                            .mapToObj(p -> new Posting(account, null, 10, "seed-" + p)))
                    .toList();
            postings.saveAll(newPostings);

            return savedAccounts.stream().map(Account::getId).toList();
        }

        private Statistics statistics() {
            SessionFactory sessionFactory = entityManagerFactory.unwrap(SessionFactory.class);
            Statistics stats = sessionFactory.getStatistics();
            stats.setStatisticsEnabled(true);
            return stats;
        }
    }

    /**
     * A transaction-history endpoint that returns every posting an account has ever made doesn't scale - this asserts
     * the endpoint actually respects {@code page}/{@code size} rather than just accepting the parameters and ignoring
     * them, which is an easy way for pagination to look wired up while quietly still loading everything.
     */
    @Nested
    class PaginationTests {

        private static final int TOTAL_POSTINGS = 25;
        private static final int PAGE_SIZE = 10;

        @Autowired
        private TestRestTemplate rest;

        @Test
        void historyEndpointRespectsPageSizeAndReportsCorrectTotals() {
            Account account = openAccount(accounts);
            postings.saveAll(IntStream.range(0, TOTAL_POSTINGS)
                    .mapToObj(i -> new Posting(account, null, 1, "history-" + i))
                    .toList());

            PostingHistoryController.PagedPostings body = rest.getForObject(
                    "/api/accounts/" + account.getId() + "/postings?page=0&size=" + PAGE_SIZE,
                    PostingHistoryController.PagedPostings.class);

            assertThat(body).isNotNull();
            assertThat(body.content()).hasSize(PAGE_SIZE);
            assertThat(body.totalElements()).isEqualTo(TOTAL_POSTINGS);
            assertThat(body.totalPages()).isEqualTo(3);
        }
    }
}
