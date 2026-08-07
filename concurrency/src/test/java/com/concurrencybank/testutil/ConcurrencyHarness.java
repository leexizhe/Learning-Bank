package com.concurrencybank.testutil;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Fires {@code threadCount} virtual threads at the same {@code task} as simultaneously as the JVM allows: every thread
 * signals it's ready, all of them block on one shared latch, then the latch is released at once. This maximizes actual
 * interleaving/contention compared to just submitting tasks to a pool one after another.
 */
public final class ConcurrencyHarness {

    private ConcurrencyHarness() {}

    public static void runConcurrently(int threadCount, Runnable task) throws InterruptedException {
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount; i++) {
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            start.countDown();
            done.await();
        }
    }
}
