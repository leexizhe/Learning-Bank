package com.concurrencybank.phase6_async_patterns;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class TellerQueueTest {

    /**
     * Producers and consumers have to run genuinely concurrently (the queue
     * is smaller than the total item count, so producers block on a full
     * queue until consumers drain it) - {@code testutil.ConcurrencyHarness}
     * doesn't fit here, it's built for "N threads doing the same independent
     * task", not two cooperating groups, so this test manages its own
     * threads.
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
