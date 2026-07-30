package com.postgresbank.phase3_coordination;

import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Two concurrent refund attempts for the same order: the first to call
 * {@code pg_try_advisory_xact_lock} wins and holds it for its whole
 * transaction; the second observes the lock unavailable and returns
 * {@code false} immediately - it never blocks or queues, which is the
 * entire reason to reach for {@code try_advisory_lock} over a row lock
 * ({@code SELECT ... FOR UPDATE} would instead make the second caller wait).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AdvisoryLockIT extends TestContainerConfig {

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

        assertThat(holder.get(10, TimeUnit.SECONDS)).as("first caller acquires the lock").isTrue();
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
