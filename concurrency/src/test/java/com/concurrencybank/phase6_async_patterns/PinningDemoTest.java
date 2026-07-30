package com.concurrencybank.phase6_async_patterns;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Pinning affects <em>scalability</em>, not correctness - a pinned virtual
 * thread still runs the critical section under full mutual exclusion, it
 * just hogs a carrier thread while doing it. So this test can't observe
 * pinning itself (see the class javadoc on {@link PinningDemo} for how to);
 * it only confirms both locking strategies are still race-free.
 */
class PinningDemoTest {

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
