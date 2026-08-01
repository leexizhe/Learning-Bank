package com.concurrencybank.phase6_async_patterns;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class CounterContentionTest {

    private static final int THREADS = 64;
    private static final int INCREMENTS_PER_THREAD = 50_000;
    private static final long EXPECTED = (long) THREADS * INCREMENTS_PER_THREAD;

    /**
     * Both counters are exact. That is the assertion; the timings are a
     * printout, deliberately. What the relative numbers actually are depends on
     * core count, JIT warmup and machine load, so a threshold here would flake
     * in CI without teaching anything - the same "assert the mechanism, don't
     * time it" rule the postgres module's {@code NPlusOneIT} follows when it
     * counts statements instead of milliseconds.
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

    private static Duration timed(InterruptibleRun work) throws InterruptedException {
        Instant start = Instant.now();
        work.run();
        return Duration.between(start, Instant.now());
    }

    @FunctionalInterface
    private interface InterruptibleRun {
        void run() throws InterruptedException;
    }
}
