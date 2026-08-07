package com.postgresbank.phase4_performance;

import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.Posting;
import com.postgresbank.common.PostingRepository;
import jakarta.persistence.EntityManagerFactory;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Same data, same result, two very different query counts. This doesn't trust "JOIN FETCH should be faster" as a rule
 * of thumb - it reads Hibernate's own {@code Statistics} and counts the actual JDBC statements each path executes: 1
 * (accounts) + N (one lazy postings load per account) for the naive path, exactly 1 for the fetch-joined path.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class NPlusOneIT extends TestContainerConfig {

    private static final int ACCOUNT_COUNT = 5;
    private static final int POSTINGS_PER_ACCOUNT = 3;

    @Autowired
    private AccountRepository accounts;

    @Autowired
    private PostingRepository postings;

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
