package com.postgresbank.phase2_ledger;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.PostingRepository;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The UNIQUE constraint on {@code transfers.idempotency_key} is the actual
 * guarantee here, not application logic - two requests racing to insert the
 * same key can only ever have one winner at the database level, no matter
 * how the JVM schedules the two threads. This fires the exact same transfer
 * twice, concurrently, and proves it posts exactly once.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class IdempotencyIT extends TestContainerConfig {

    @Autowired
    private AccountRepository accounts;

    @Autowired
    private PostingRepository postings;

    @Autowired
    private TransferService transferService;

    private final ExecutorService pool = Executors.newFixedThreadPool(2);

    @Test
    void concurrentRetriesOfTheSameRequestPostOnlyOnce() throws Exception {
        Account from = openAccount(accounts);
        Account to = openAccount(accounts);
        String idempotencyKey = UUID.randomUUID().toString();

        CountDownLatch bothStarting = new CountDownLatch(2);
        Callable<TransferResult> attempt = () -> {
            bothStarting.countDown();
            bothStarting.await(2, TimeUnit.SECONDS);
            return transferService.transfer(idempotencyKey, from.getId(), to.getId(), 250);
        };

        Future<TransferResult> first = pool.submit(attempt);
        Future<TransferResult> second = pool.submit(attempt);

        TransferResult r1 = first.get(10, TimeUnit.SECONDS);
        TransferResult r2 = second.get(10, TimeUnit.SECONDS);

        assertThat(r1.transferId()).isEqualTo(r2.transferId());
        assertThat(List.of(r1.alreadyApplied(), r2.alreadyApplied()))
                .as("exactly one attempt should be the original, the other should observe it already applied")
                .containsExactlyInAnyOrder(false, true);

        assertThat(postings.countByTransferId(r1.transferId()))
                .as("double-entry: exactly one debit + one credit, never four")
                .isEqualTo(2);
    }
}
