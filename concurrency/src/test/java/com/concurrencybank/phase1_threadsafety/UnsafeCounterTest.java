package com.concurrencybank.phase1_threadsafety;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class UnsafeCounterTest {

    /**
     * Demonstrates the bug before showing the fix: {@code balance += amount} is
     * not atomic, so with enough concurrent depositors some increments get lost
     * and the final balance ends up lower than the arithmetic sum. This is the
     * "before" picture — {@link SynchronizedAccountTest} and
     * {@link AtomicAccountTest} run the identical stress shape and prove the
     * fixed versions never lose an update.
     */
    @Test
    void concurrentDepositsLoseUpdates() throws InterruptedException {
        UnsafeCounter counter = new UnsafeCounter();
        int threads = 50;
        int depositsPerThread = 10_000;

        runConcurrently(threads, () -> {
            for (int i = 0; i < depositsPerThread; i++) {
                counter.deposit(1);
            }
        });

        long expected = (long) threads * depositsPerThread;
        assertThat(counter.getBalance())
                .as("unsynchronized balance++ should drop updates under contention")
                .isLessThan(expected);
    }
}
