package com.concurrencybank.phase7_primitives;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BorrowablePoolTest {

    @Test
    void anObjectIsHandedOutAgainOnceItsLeaseIsClosed() throws Exception {
        try (BorrowablePool<Object> pool = new BorrowablePool<>(1, Object::new)) {
            Object first;
            try (BorrowablePool<Object>.Lease lease = pool.borrow(Duration.ofSeconds(1))) {
                first = lease.get();
                assertThat(pool.available()).as("the only permit is held").isZero();
            }

            assertThat(pool.available())
                    .as("closing the lease gave the permit back")
                    .isEqualTo(1);

            try (BorrowablePool<Object>.Lease lease = pool.borrow(Duration.ofSeconds(1))) {
                assertThat(lease.get())
                        .as("the pool reuses the object rather than building a new one")
                        .isSameAs(first);
            }
        }
    }

    /**
     * The failure mode that matters. An unbounded {@code acquire()} would turn a saturated pool into a pile of parked
     * request threads and the service would simply stop responding, with no error naming the cause.
     */
    @Test
    void anExhaustedPoolTimesOutInsteadOfBlockingForever() throws Exception {
        try (BorrowablePool<Object> pool = new BorrowablePool<>(1, Object::new)) {
            BorrowablePool<Object>.Lease held = pool.borrow(Duration.ofSeconds(1));

            Instant start = Instant.now();
            assertThatThrownBy(() -> pool.borrow(Duration.ofMillis(200)))
                    .isInstanceOf(TimeoutException.class)
                    .hasMessageContaining("PT0.2S");

            assertThat(Duration.between(start, Instant.now()))
                    .as("it waited for the budget rather than failing instantly")
                    .isGreaterThanOrEqualTo(Duration.ofMillis(150));

            held.close();
            assertThat(pool.available()).isEqualTo(1);
        }
    }

    /**
     * The mirror-image bug to leaking a permit, and the more dangerous one. A leak shrinks the pool until it stops
     * working, which you notice. A double release <em>inflates</em> it past the size you configured, so the ceiling you
     * built the pool to enforce quietly stops existing and the database's {@code max_connections} tells you instead.
     */
    @Test
    void closingALeaseTwiceDoesNotInflateThePool() throws Exception {
        try (BorrowablePool<Object> pool = new BorrowablePool<>(2, Object::new)) {
            BorrowablePool<Object>.Lease lease = pool.borrow(Duration.ofSeconds(1));
            assertThat(pool.available()).isEqualTo(1);

            lease.close();
            lease.close();
            lease.close();

            assertThat(pool.available())
                    .as("three closes, one permit returned - never more than the pool's size")
                    .isEqualTo(2);
        }
    }

    @Test
    void aReturnedLeaseCannotStillBeUsed() throws Exception {
        try (BorrowablePool<Object> pool = new BorrowablePool<>(1, Object::new)) {
            BorrowablePool<Object>.Lease lease = pool.borrow(Duration.ofSeconds(1));
            lease.close();

            assertThatThrownBy(lease::get)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already returned");
        }
    }

    @Test
    void concurrentBorrowersNeverExceedThePoolSize() throws InterruptedException {
        int size = 4;
        AtomicInteger live = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();

        try (BorrowablePool<Object> pool = new BorrowablePool<>(size, Object::new)) {
            runConcurrently(200, () -> {
                try (BorrowablePool<Object>.Lease lease = pool.borrow(Duration.ofSeconds(10))) {
                    assertThat(lease.get()).isNotNull();
                    peak.accumulateAndGet(live.incrementAndGet(), Math::max);
                    Thread.yield();
                    live.decrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (TimeoutException e) {
                    throw new AssertionError("borrow timed out despite a 10s budget", e);
                }
            });

            assertThat(peak.get())
                    .as("200 threads contending, never more than %d objects live at once", size)
                    .isLessThanOrEqualTo(size);
            assertThat(pool.available()).as("every lease was returned").isEqualTo(size);
        }
    }
}
