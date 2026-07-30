package com.postgresbank.phase3_coordination;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.OutboxRepository;
import com.postgresbank.phase2_ledger.TransferService;
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
 * TransferTransactionalOps writes the outbox row <em>first</em>, then the
 * {@code transfers} row that owns the idempotency-key uniqueness check (see
 * that class's javadoc). That ordering is what makes this test meaningful:
 * the losing attempt in a duplicate-key race gets as far as inserting its
 * outbox row before its transfer insert fails - if the outbox write weren't
 * in the same transaction as everything else, that row would survive the
 * rollback as an orphan event describing a transfer that never happened.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OutboxIT extends TestContainerConfig {

    @Autowired
    private AccountRepository accounts;

    @Autowired
    private TransferService transferService;

    @Autowired
    private OutboxRepository outbox;

    @Autowired
    private OutboxRelay relay;

    private final ExecutorService pool = Executors.newFixedThreadPool(2);

    @Test
    void successfulTransferLeavesExactlyOneOutboxRow() {
        Account from = openAccount(accounts);
        Account to = openAccount(accounts);
        String idempotencyKey = UUID.randomUUID().toString();

        transferService.transfer(idempotencyKey, from.getId(), to.getId(), 100);

        assertThat(outbox.countByPayloadContaining(idempotencyKey)).isEqualTo(1);
    }

    @Test
    void losingAttemptInADuplicateKeyRaceLeavesNoOrphanOutboxRow() throws Exception {
        Account from = openAccount(accounts);
        Account to = openAccount(accounts);
        String idempotencyKey = UUID.randomUUID().toString();

        CountDownLatch bothStarting = new CountDownLatch(2);
        Callable<Void> attempt = () -> {
            bothStarting.countDown();
            bothStarting.await(2, TimeUnit.SECONDS);
            transferService.transfer(idempotencyKey, from.getId(), to.getId(), 100);
            return null;
        };

        Future<Void> first = pool.submit(attempt);
        Future<Void> second = pool.submit(attempt);
        first.get(10, TimeUnit.SECONDS);
        second.get(10, TimeUnit.SECONDS);

        assertThat(outbox.countByPayloadContaining(idempotencyKey))
                .as("the loser's outbox insert should have rolled back with its failed transfer insert")
                .isEqualTo(1);
    }

    @Test
    void relayMarksPendingEventsPublished() {
        Account from = openAccount(accounts);
        Account to = openAccount(accounts);
        transferService.transfer(UUID.randomUUID().toString(), from.getId(), to.getId(), 100);

        int relayed = relay.relayOnce();

        assertThat(relayed).isGreaterThanOrEqualTo(1);
        assertThat(outbox.findByPublishedFalse().stream().filter(o -> o.getPayload().contains(from.getId().toString())))
                .isEmpty();
    }
}
