package com.concurrencybank.phase7_primitives;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The building blocks: two rate limiters, an LRU cache, a bounded buffer and an object pool. This is the phase where
 * the tests stop demonstrating a hazard and start pinning down a contract, so each block below is a specification
 * rather than a comparison — read them for the properties they assert, not for a broken-versus-fixed pair.
 *
 * <p>Each block keeps its own fixtures, deliberately. The two limiters in particular each drive their own fake clock,
 * and both rely on JUnit's default per-method lifecycle handing every test a fresh instance.
 *
 * <p>The one thing worth reading across blocks is how each avoids timing flakiness: the limiters inject a clock rather
 * than sleeping, the buffer uses latches and bounded joins, and only {@link BorrowablePoolTests} times anything at all
 * — with a deliberately loose margin, and only because "it waited" is the actual property under test.
 */
class Phase7PrimitivesTest {

    /**
     * Every test here drives a <b>fake clock</b> instead of sleeping. A rate limiter tested with {@code Thread.sleep}
     * asserts on the scheduler's mood and fails on a loaded CI box; an injected clock makes refill behaviour exact and
     * instant. Same idea as the postgres module's {@code afterRead} / {@code duringHold} seams — make the race
     * deterministic rather than probable.
     */
    @Nested
    class TokenBucketRateLimiterTests {

        private static final long ONE_SECOND = TimeUnit.SECONDS.toNanos(1);

        private final AtomicLong clock = new AtomicLong();

        @Test
        void aRestedBucketAllowsOneFullBurstThenThrottles() {
            TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(5, 5, clock::get);

            for (int i = 0; i < 5; i++) {
                assertThat(limiter.tryAcquire())
                        .as("burst token %d of 5", i + 1)
                        .isTrue();
            }

            assertThat(limiter.tryAcquire())
                    .as("bucket is drained and the clock has not moved")
                    .isFalse();
        }

        @Test
        void tokensReturnAtTheConfiguredRateAndNotFaster() {
            TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(5, 5, clock::get);
            drain(limiter, 5);

            clock.addAndGet(ONE_SECOND / 5 - 1);
            assertThat(limiter.tryAcquire())
                    .as("one nano short of a whole token")
                    .isFalse();

            clock.addAndGet(1);
            assertThat(limiter.tryAcquire())
                    .as("exactly one token's worth has elapsed")
                    .isTrue();
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
         * The regression test for the remainder bug described in the class javadoc. Callers poll far more often than
         * one token-interval, and every one of those polls runs {@code refill()} on a sub-token amount of elapsed time.
         * If {@code refill()} snapped its marker forward to {@code now} on those calls it would discard a tenth of a
         * second each time here and the token would never arrive at all.
         */
        @Test
        void pollingFasterThanTheRefillRateDoesNotLeakElapsedTime() {
            TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1, 1, clock::get);
            assertThat(limiter.tryAcquire())
                    .as("spend the token it starts with")
                    .isTrue();

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
            // The clock never moves, so no refill is possible and capacity is a hard ceiling on total grants however
            // many threads race for them.
            TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(100, 100, clock::get);
            AtomicInteger granted = new AtomicInteger();

            runConcurrently(500, () -> {
                if (limiter.tryAcquire()) {
                    granted.incrementAndGet();
                }
            });

            assertThat(granted.get())
                    .as("500 threads, 100 tokens, no double-spend and no lost grant")
                    .isEqualTo(100);
        }

        private void drain(TokenBucketRateLimiter limiter, int tokens) {
            for (int i = 0; i < tokens; i++) {
                limiter.tryAcquire();
            }
        }
    }

    /** The same fake-clock discipline as the bucket above, against a limiter with a different shape of memory. */
    @Nested
    class SlidingWindowRateLimiterTests {

        private final AtomicLong clock = new AtomicLong();

        @Test
        void admitsUpToTheLimitInsideOneWindow() {
            SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(5, 1_000, clock::get);

            for (int i = 0; i < 5; i++) {
                assertThat(limiter.tryAcquire()).as("grant %d of 5", i + 1).isTrue();
            }

            assertThat(limiter.tryAcquire())
                    .as("sixth request in the same window")
                    .isFalse();
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
         * The reason this class exists alongside {@link TokenBucketRateLimiter}, and the answer to "why isn't a fixed
         * window enough?".
         *
         * <p>A fixed-window counter ("5 per second, reset on the second") would let all five of these through at 999ms
         * and five more at 1001ms — ten requests in two milliseconds, every one of them inside the stated policy. The
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

    @Nested
    class ConcurrentLruCacheTests {

        @Test
        void evictsTheOldestEntryOnceCapacityIsReached() {
            ConcurrentLruCache<String, Integer> cache = new ConcurrentLruCache<>(3);

            cache.put("a", 1);
            cache.put("b", 2);
            cache.put("c", 3);
            cache.put("d", 4);

            assertThat(cache.size()).as("capacity is a hard ceiling").isEqualTo(3);
            assertThat(cache.get("a")).as("a was the oldest and was evicted").isNull();
            assertThat(cache.keysInAccessOrder()).containsExactly("b", "c", "d");
        }

        /**
         * The property that makes this an LR<b>U</b> cache rather than an LR<b>I</b> one, and the reason {@code get}
         * has to take the write lock: reading {@code a} re-links it as the freshest entry, which changes who gets
         * evicted next. With {@code accessOrder=false} this test fails and {@code a} disappears instead of {@code b}.
         */
        @Test
        void readingAnEntryRescuesItFromTheNextEviction() {
            ConcurrentLruCache<String, Integer> cache = new ConcurrentLruCache<>(3);
            cache.put("a", 1);
            cache.put("b", 2);
            cache.put("c", 3);

            assertThat(cache.get("a")).isEqualTo(1);
            assertThat(cache.keysInAccessOrder())
                    .as("the read moved a to the most-recent end")
                    .containsExactly("b", "c", "a");

            cache.put("d", 4);

            assertThat(cache.get("b")).as("b became the least recently used").isNull();
            assertThat(cache.get("a")).as("a survived because it had been read").isEqualTo(1);
        }

        @Test
        void concurrentWritersNeverPushItPastCapacity() throws InterruptedException {
            int capacity = 50;
            ConcurrentLruCache<Integer, Integer> cache = new ConcurrentLruCache<>(capacity);
            AtomicInteger nextKey = new AtomicInteger();

            runConcurrently(100, () -> {
                for (int i = 0; i < 200; i++) {
                    int key = nextKey.getAndIncrement();
                    cache.put(key, key);
                    cache.get(key);
                }
            });

            assertThat(cache.size())
                    .as("20,000 writes from 100 threads, still exactly at capacity")
                    .isEqualTo(capacity);
            assertThat(cache.keysInAccessOrder()).doesNotHaveDuplicates();
        }
    }

    /**
     * Deliberately the same shape and the same first test name as
     * {@code phase6_async_patterns.Phase6AsyncPatternsTest.TellerQueueTests} — the two are meant to be diffed.
     * Identical guarantees, identical assertions, and the only difference is the mechanism underneath.
     */
    @Nested
    class BoundedBufferWithConditionTests {

        /**
         * Producers and consumers have to run genuinely concurrently (the buffer is smaller than the total item count,
         * so producers block on a full buffer until consumers drain it) - {@code testutil.ConcurrencyHarness} doesn't
         * fit here, it's built for "N threads doing the same independent task", not two cooperating groups, so this
         * test manages its own threads.
         */
        @Test
        void everyItemIsConsumedExactlyOnceNoLossNoDuplication() throws InterruptedException {
            BoundedBufferWithCondition<Integer> buffer = new BoundedBufferWithCondition<>(10);
            int producers = 5;
            int itemsPerProducer = 200;
            int totalItems = producers * itemsPerProducer;
            int consumers = 5;
            int itemsPerConsumer = totalItems / consumers;

            AtomicInteger nextId = new AtomicInteger();
            ConcurrentLinkedQueue<Integer> consumedItems = new ConcurrentLinkedQueue<>();

            List<Thread> threads = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                threads.add(new Thread(() -> {
                    for (int i = 0; i < itemsPerProducer; i++) {
                        submitQuietly(buffer, nextId.getAndIncrement());
                    }
                }));
            }
            for (int c = 0; c < consumers; c++) {
                threads.add(new Thread(() -> {
                    for (int i = 0; i < itemsPerConsumer; i++) {
                        consumedItems.add(takeQuietly(buffer));
                    }
                }));
            }

            threads.forEach(Thread::start);
            for (Thread t : threads) {
                t.join();
            }

            assertThat(consumedItems)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(0, totalItems).boxed().toList());
            assertThat(buffer.size()).isZero();
        }

        @Test
        void takeParksUntilAnItemArrives() throws InterruptedException {
            BoundedBufferWithCondition<String> buffer = new BoundedBufferWithCondition<>(4);
            AtomicReference<String> taken = new AtomicReference<>();
            CountDownLatch consumerStarted = new CountDownLatch(1);

            Thread consumer = new Thread(() -> {
                consumerStarted.countDown();
                taken.set(takeQuietly(buffer));
            });
            consumer.start();
            consumerStarted.await();

            assertThat(taken.get()).as("nothing has been submitted yet").isNull();

            buffer.submit("deposit");
            consumer.join(Duration.ofSeconds(5));

            assertThat(taken.get())
                    .as("notEmpty.signal() woke the waiting consumer")
                    .isEqualTo("deposit");
        }

        @Test
        void submitParksWhileTheBufferIsFull() throws InterruptedException {
            BoundedBufferWithCondition<String> buffer = new BoundedBufferWithCondition<>(1);
            buffer.submit("first");

            AtomicBoolean secondLanded = new AtomicBoolean();
            CountDownLatch producerStarted = new CountDownLatch(1);

            Thread producer = new Thread(() -> {
                producerStarted.countDown();
                submitQuietly(buffer, "second");
                secondLanded.set(true);
            });
            producer.start();
            producerStarted.await();

            assertThat(secondLanded.get()).as("capacity is 1 and it is taken").isFalse();

            assertThat(buffer.take()).isEqualTo("first");
            producer.join(Duration.ofSeconds(5));

            assertThat(secondLanded.get())
                    .as("notFull.signal() woke the blocked producer")
                    .isTrue();
            assertThat(buffer.take()).isEqualTo("second");
        }

        private <T> void submitQuietly(BoundedBufferWithCondition<T> buffer, T item) {
            try {
                buffer.submit(item);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private <T> T takeQuietly(BoundedBufferWithCondition<T> buffer) {
            try {
                return buffer.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    @Nested
    class BorrowablePoolTests {

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
         * The failure mode that matters. An unbounded {@code acquire()} would turn a saturated pool into a pile of
         * parked request threads and the service would simply stop responding, with no error naming the cause.
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
         * working, which you notice. A double release <em>inflates</em> it past the size you configured, so the ceiling
         * you built the pool to enforce quietly stops existing and the database's {@code max_connections} tells you
         * instead.
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
}
