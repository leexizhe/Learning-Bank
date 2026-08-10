package com.postgresbank.phase3_coordination;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.OutboxRepository;
import com.postgresbank.phase2_ledger.TransferService;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Three ways to coordinate work across processes using nothing but the database: an advisory lock that refuses rather
 * than queues, a transactional outbox, and a {@code SKIP LOCKED} work queue. They are separate patterns solving
 * separate problems — read the blocks independently — but they share one premise worth naming up front: the database
 * is already a consistent, highly-available coordination service, so reaching for a second one is often unnecessary.
 *
 * <p>Each block keeps its own thread pool, deliberately sized for what it is demonstrating (two contenders for the
 * lock and outbox races; eight workers draining the queue).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class Phase3CoordinationIT extends TestContainerConfig {

    /**
     * Two concurrent refund attempts for the same order: the first to call {@code pg_try_advisory_xact_lock} wins and
     * holds it for its whole transaction; the second observes the lock unavailable and returns {@code false}
     * immediately - it never blocks or queues, which is the entire reason to reach for {@code try_advisory_lock} over a
     * row lock ({@code SELECT ... FOR UPDATE} would instead make the second caller wait).
     */
    @Nested
    class AdvisoryLockTests {

        @Autowired
        private RefundService refunds;

        private final ExecutorService pool = Executors.newFixedThreadPool(2);

        @Test
        void secondConcurrentRefundForSameOrderIsRejectedWithoutBlocking() throws Exception {
            long orderId = 42L;
            CountDownLatch lockHeld = new CountDownLatch(1);
            CountDownLatch challengerDone = new CountDownLatch(1);

            Future<Boolean> holder = pool.submit(() -> refunds.tryRefund(orderId, () -> {
                lockHeld.countDown();
                await(challengerDone);
            }));

            Future<Boolean> challenger = pool.submit(() -> {
                lockHeld.await(5, TimeUnit.SECONDS);
                Instant start = Instant.now();
                boolean result = refunds.tryRefund(orderId);
                assertThat(Duration.between(start, Instant.now()))
                        .as("a try-lock must never block waiting for the other transaction")
                        .isLessThan(Duration.ofSeconds(2));
                challengerDone.countDown();
                return result;
            });

            assertThat(holder.get(10, TimeUnit.SECONDS))
                    .as("first caller acquires the lock")
                    .isTrue();
            assertThat(challenger.get(10, TimeUnit.SECONDS))
                    .as("second caller for the same order id must be rejected while the first still holds it")
                    .isFalse();
        }

        private void await(CountDownLatch latch) {
            try {
                latch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * TransferTransactionalOps writes the outbox row <em>first</em>, then the {@code transfers} row that owns the
     * idempotency-key uniqueness check (see that class's javadoc). That ordering is what makes these tests meaningful:
     * the losing attempt in a duplicate-key race gets as far as inserting its outbox row before its transfer insert
     * fails - if the outbox write weren't in the same transaction as everything else, that row would survive the
     * rollback as an orphan event describing a transfer that never happened.
     */
    @Nested
    class OutboxTests {

        @Autowired
        private AccountRepository accounts;

        @Autowired
        private TransferService transferService;

        @Autowired
        private OutboxRepository outbox;

        /**
         * The transactional half, not {@link OutboxRelay} itself. Note that calling this from a test is an
         * <b>external</b> call and therefore goes through the {@code @Transactional} proxy — which is exactly why this
         * test kept passing while the scheduled path, which used to self-invoke, silently did nothing. See
         * {@link OutboxRelayTransactionalOps} for the full story.
         */
        @Autowired
        private OutboxRelayTransactionalOps relay;

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
            assertThat(outbox.findByPublishedFalse().stream()
                            .filter(o -> o.getPayload().contains(from.getId().toString())))
                    .isEmpty();
        }
    }

    /**
     * Seeds a batch of jobs and lets several "worker" threads race to claim them via {@code SELECT ... FOR UPDATE SKIP
     * LOCKED}. Proves the two properties that make this pattern scale to multiple service instances: every job is
     * claimed by exactly one worker (no two workers ever process the same row), and no worker ever blocks waiting on a
     * row another worker already holds - the whole batch drains quickly instead of serializing behind lock waits.
     */
    @Nested
    class SkipLockedTests {

        @Autowired
        private PaymentJobRepository jobRepository;

        @Autowired
        private JobRunner jobRunner;

        private final ExecutorService pool = Executors.newFixedThreadPool(8);

        @Test
        void everyJobIsClaimedExactlyOnceAcrossConcurrentWorkers() throws Exception {
            String batch = UUID.randomUUID().toString();
            int jobCount = 30;
            jobRepository.saveAll(IntStream.range(0, jobCount)
                    .mapToObj(i -> new PaymentJob(batch + "-job-" + i))
                    .toList());

            Set<Long> claimed = ConcurrentHashMap.newKeySet();
            Callable<Void> worker = () -> {
                Optional<Long> id;
                while ((id = jobRunner.claimNext()).isPresent()) {
                    boolean firstClaim = claimed.add(id.get());
                    assertThat(firstClaim)
                            .as("job %s claimed by more than one worker", id.get())
                            .isTrue();
                }
                return null;
            };

            List<Future<Void>> workers =
                    IntStream.range(0, 8).mapToObj(i -> pool.submit(worker)).toList();
            for (Future<Void> worker1 : workers) {
                worker1.get(20, TimeUnit.SECONDS);
            }

            assertThat(claimed).as("every seeded job should have been claimed").hasSize(jobCount);
            assertThat(jobRepository.countByStatus("DONE")).isGreaterThanOrEqualTo(jobCount);
        }
    }
}
