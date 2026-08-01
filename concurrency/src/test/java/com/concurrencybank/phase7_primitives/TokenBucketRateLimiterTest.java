package com.concurrencybank.phase7_primitives;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Every test here drives a <b>fake clock</b> instead of sleeping. A rate limiter tested with {@code
 * Thread.sleep} asserts on the scheduler's mood and fails on a loaded CI box; an injected clock
 * makes refill behaviour exact and instant. Same idea as the postgres module's {@code afterRead} /
 * {@code duringHold} seams — make the race deterministic rather than probable.
 */
class TokenBucketRateLimiterTest {

  private static final long ONE_SECOND = TimeUnit.SECONDS.toNanos(1);

  private final AtomicLong clock = new AtomicLong();

  @Test
  void aRestedBucketAllowsOneFullBurstThenThrottles() {
    TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(5, 5, clock::get);

    for (int i = 0; i < 5; i++) {
      assertThat(limiter.tryAcquire()).as("burst token %d of 5", i + 1).isTrue();
    }

    assertThat(limiter.tryAcquire()).as("bucket is drained and the clock has not moved").isFalse();
  }

  @Test
  void tokensReturnAtTheConfiguredRateAndNotFaster() {
    TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(5, 5, clock::get);
    drain(limiter, 5);

    clock.addAndGet(ONE_SECOND / 5 - 1);
    assertThat(limiter.tryAcquire()).as("one nano short of a whole token").isFalse();

    clock.addAndGet(1);
    assertThat(limiter.tryAcquire()).as("exactly one token's worth has elapsed").isTrue();
    assertThat(limiter.tryAcquire()).as("and only one").isFalse();
  }

  @Test
  void anIdleBucketNeverAccumulatesPastItsCapacity() {
    TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(5, 5, clock::get);
    drain(limiter, 5);

    clock.addAndGet(TimeUnit.HOURS.toNanos(1));

    assertThat(limiter.availableTokens())
        .as("an hour of credit is still capped at the burst size")
        .isEqualTo(5);
  }

  /**
   * The regression test for the remainder bug described in the class javadoc. Callers poll far more
   * often than one token-interval, and every one of those polls runs {@code refill()} on a
   * sub-token amount of elapsed time. If {@code refill()} snapped its marker forward to {@code now}
   * on those calls it would discard a tenth of a second each time here and the token would never
   * arrive at all.
   */
  @Test
  void pollingFasterThanTheRefillRateDoesNotLeakElapsedTime() {
    TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1, 1, clock::get);
    assertThat(limiter.tryAcquire()).as("spend the token it starts with").isTrue();

    for (int i = 0; i < 10; i++) {
      clock.addAndGet(ONE_SECOND / 10);
      limiter.availableTokens();
    }

    assertThat(limiter.availableTokens())
        .as("a full second has passed, in ten sub-token steps")
        .isEqualTo(1);
  }

  @Test
  void concurrentCallersCannotTakeMoreThanTheBucketHolds() throws InterruptedException {
    // The clock never moves, so no refill is possible and capacity is a hard
    // ceiling on total grants however many threads race for them.
    TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(100, 100, clock::get);
    AtomicInteger granted = new AtomicInteger();

    runConcurrently(
        500,
        () -> {
          if (limiter.tryAcquire()) {
            granted.incrementAndGet();
          }
        });

    assertThat(granted.get())
        .as("500 threads, 100 tokens, no double-spend and no lost grant")
        .isEqualTo(100);
  }

  private static void drain(TokenBucketRateLimiter limiter, int tokens) {
    for (int i = 0; i < tokens; i++) {
      limiter.tryAcquire();
    }
  }
}
