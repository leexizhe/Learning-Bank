package com.concurrencybank.phase7_primitives;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class SlidingWindowRateLimiterTest {

    private final AtomicLong clock = new AtomicLong();

    @Test
    void admitsUpToTheLimitInsideOneWindow() {
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(5, 1_000, clock::get);

        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire()).as("grant %d of 5", i + 1).isTrue();
        }

        assertThat(limiter.tryAcquire()).as("sixth request in the same window").isFalse();
    }

    @Test
    void admitsAgainOnlyAsIndividualGrantsAgeOut() {
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(3, 1_000, clock::get);

        limiter.tryAcquire();
        clock.set(millis(100));
        limiter.tryAcquire();
        clock.set(millis(200));
        limiter.tryAcquire();

        clock.set(millis(1_000));
        assertThat(limiter.tryAcquire())
                .as("only the grant at t=0 has expired, so exactly one slot opened")
                .isTrue();
        assertThat(limiter.tryAcquire())
                .as("the grants at 100ms and 200ms are still inside the window")
                .isFalse();
    }

    /**
     * The reason this class exists alongside {@link TokenBucketRateLimiter}, and
     * the answer to "why isn't a fixed window enough?".
     *
     * <p>A fixed-window counter ("5 per second, reset on the second") would let
     * all five of these through at 999ms and five more at 1001ms — ten requests
     * in two milliseconds, every one of them inside the stated policy. The
     * sliding window still counts the first batch, so it cannot.
     */
    @Test
    void cannotProduceTheDoubleRateBurstThatAFixedWindowAllows() {
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(5, 1_000, clock::get);

        clock.set(millis(999));
        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire()).isTrue();
        }

        clock.set(millis(1_001));
        assertThat(limiter.tryAcquire())
                .as("a fixed-window counter would have reset here; the trailing window has not")
                .isFalse();

        clock.set(millis(1_999));
        assertThat(limiter.tryAcquire())
                .as("the batch from 999ms is now a full window old and has expired")
                .isTrue();
    }

    @Test
    void concurrentCallersCannotExceedTheLimit() throws InterruptedException {
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(100, 1_000, clock::get);
        AtomicInteger granted = new AtomicInteger();

        runConcurrently(500, () -> {
            if (limiter.tryAcquire()) {
                granted.incrementAndGet();
            }
        });

        assertThat(granted.get()).isEqualTo(100);
        assertThat(limiter.usedInWindow()).isEqualTo(100);
    }

    private static long millis(long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }
}
