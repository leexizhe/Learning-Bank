package com.concurrencybank.phase6_async_patterns;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Four independent async patterns rather than one lesson told four ways: carrier-thread pinning, scoped values across
 * forked subtasks, a hand-rolled blocking queue, and striped versus CAS counters. They share a package because they
 * are all things you hit once virtual threads are in play, not because any of them explains another.
 *
 * <p>What they do share is a discipline worth noticing across the blocks: none of them asserts on a duration. Where
 * timing is the interesting part it gets printed, and the assertion is on something exact instead.
 */
class Phase6AsyncPatternsTest {

    /**
     * Pinning affects <em>scalability</em>, not correctness - a pinned virtual thread still runs the critical section
     * under full mutual exclusion, it just hogs a carrier thread while doing it. So these tests can't observe pinning
     * itself (see the class javadoc on {@link PinningDemo} for how to); they only confirm both locking strategies are
     * still race-free.
     */
    @Nested
    class PinningTests {

        @Test
        void synchronizedVersionStaysCorrectUnderConcurrentVirtualThreads() throws InterruptedException {
            PinningDemo demo = new PinningDemo();
            int callers = 100;

            runConcurrently(callers, () -> {
                try {
                    demo.withSynchronized(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            assertThat(demo.getSynchronizedCallCount()).isEqualTo(callers);
        }

        @Test
        void reentrantLockVersionStaysCorrectUnderConcurrentVirtualThreads() throws InterruptedException {
            PinningDemo demo = new PinningDemo();
            int callers = 100;

            runConcurrently(callers, () -> {
                try {
                    demo.withReentrantLock(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            assertThat(demo.getLockCallCount()).isEqualTo(callers);
        }
    }

    /** A correlation id that crosses a fork without being threaded through every signature. */
    @Nested
    class AuditContextTests {

        @Test
        void isUnboundOutsideAnyScope() {
            assertThat(AuditContext.currentCorrelationId()).isEmpty();
        }

        @Test
        void correlationIdPropagatesAutomaticallyIntoForkedSubtasks() throws InterruptedException {
            List<String> results = AuditContext.runAuditedAccountChecks("REQ-42");

            assertThat(results)
                    .containsExactlyInAnyOrder(
                            "fraud-check saw: REQ-42", "credit-check saw: REQ-42", "sanctions-check saw: REQ-42");
        }

        @Test
        void differentCallsGetIndependentCorrelationIds() throws InterruptedException {
            List<String> first = AuditContext.runAuditedAccountChecks("REQ-1");
            List<String> second = AuditContext.runAuditedAccountChecks("REQ-2");

            assertThat(first).allMatch(r -> r.endsWith("REQ-1"));
            assertThat(second).allMatch(r -> r.endsWith("REQ-2"));
        }
    }

    /** Producers and consumers that genuinely have to cooperate, so the shared harness can't be used. */
    @Nested
    class TellerQueueTests {

        /**
         * Producers and consumers have to run genuinely concurrently (the queue is smaller than the total item count,
         * so producers block on a full queue until consumers drain it) - {@code testutil.ConcurrencyHarness} doesn't
         * fit here, it's built for "N threads doing the same independent task", not two cooperating groups, so this
         * test manages its own threads.
         */
        @Test
        void everyItemIsConsumedExactlyOnceNoLossNoDuplication() throws InterruptedException {
            TellerQueue<Integer> queue = new TellerQueue<>(10);
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
                        submitQuietly(queue, nextId.getAndIncrement());
                    }
                }));
            }
            for (int c = 0; c < consumers; c++) {
                threads.add(new Thread(() -> {
                    for (int i = 0; i < itemsPerConsumer; i++) {
                        consumedItems.add(takeQuietly(queue));
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
            assertThat(queue.size()).isZero();
        }

        private void submitQuietly(TellerQueue<Integer> queue, int item) {
            try {
                queue.submit(item);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private Integer takeQuietly(TellerQueue<Integer> queue) {
            try {
                return queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    /** {@code AtomicLong} versus {@code LongAdder} under contention hard enough to tell them apart. */
    @Nested
    class CounterContentionTests {

        private static final int THREADS = 64;
        private static final int INCREMENTS_PER_THREAD = 50_000;
        private static final long EXPECTED = (long) THREADS * INCREMENTS_PER_THREAD;

        /**
         * Both counters are exact. That is the assertion; the timings are a printout, deliberately. What the relative
         * numbers actually are depends on core count, JIT warmup and machine load, so a threshold here would flake in
         * CI without teaching anything - the same "assert the mechanism, don't time it" rule the postgres module's
         * {@code Phase4PerformanceIT.NPlusOneTests} follows when it counts statements instead of milliseconds.
         */
        @Test
        void bothCountersAgreeOnTheTotalUnderHeavyContention() throws InterruptedException {
            CounterContention counters = new CounterContention();

            Duration atomicTime = timed(() -> runConcurrently(THREADS, () -> {
                for (int i = 0; i < INCREMENTS_PER_THREAD; i++) {
                    counters.incrementAtomic();
                }
            }));

            Duration adderTime = timed(() -> runConcurrently(THREADS, () -> {
                for (int i = 0; i < INCREMENTS_PER_THREAD; i++) {
                    counters.incrementAdder();
                }
            }));

            System.out.printf(
                    "CounterContention (%d threads x %,d increments): AtomicLong %s, LongAdder %s%n",
                    THREADS, INCREMENTS_PER_THREAD, atomicTime, adderTime);

            assertThat(counters.atomicValue())
                    .as("AtomicLong's CAS retry loop loses nothing, however hard it is contended")
                    .isEqualTo(EXPECTED);
            assertThat(counters.adderValue())
                    .as("LongAdder's striped cells sum to exactly the same total once writers have stopped")
                    .isEqualTo(EXPECTED);
        }

        private Duration timed(InterruptibleRun work) throws InterruptedException {
            Instant start = Instant.now();
            work.run();
            return Duration.between(start, Instant.now());
        }

        @FunctionalInterface
        private interface InterruptibleRun {
            void run() throws InterruptedException;
        }
    }
}
