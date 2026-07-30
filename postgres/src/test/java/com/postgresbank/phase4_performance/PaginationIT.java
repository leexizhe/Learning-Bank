package com.postgresbank.phase4_performance;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.Posting;
import com.postgresbank.common.PostingRepository;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;

/**
 * A transaction-history endpoint that returns every posting an account has
 * ever made doesn't scale - this asserts the endpoint actually respects
 * {@code page}/{@code size} rather than just accepting the parameters and
 * ignoring them, which is an easy way for pagination to look wired up while
 * quietly still loading everything.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PaginationIT extends TestContainerConfig {

    private static final int TOTAL_POSTINGS = 25;
    private static final int PAGE_SIZE = 10;

    @Autowired
    private AccountRepository accounts;

    @Autowired
    private PostingRepository postings;

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
