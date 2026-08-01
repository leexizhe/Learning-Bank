package com.concurrencybank.phase7_primitives;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Deliberately the same shape and the same first test name as
 * {@code phase6_async_patterns.TellerQueueTest} — the two files are meant to be
 * diffed. Identical guarantees, identical assertions, and the only difference is
 * the mechanism underneath.
 */
class BoundedBufferWithConditionTest {

    /**
     * Producers and consumers have to run genuinely concurrently (the buffer is
     * smaller than the total item count, so producers block on a full buffer
     * until consumers drain it) - {@code testutil.ConcurrencyHarness} doesn't fit
     * here, it's built for "N threads doing the same independent task", not two
     * cooperating groups, so this test manages its own threads.
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

        assertThat(taken.get()).as("notEmpty.signal() woke the waiting consumer").isEqualTo("deposit");
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

        assertThat(secondLanded.get()).as("notFull.signal() woke the blocked producer").isTrue();
        assertThat(buffer.take()).isEqualTo("second");
    }

    private static <T> void submitQuietly(BoundedBufferWithCondition<T> buffer, T item) {
        try {
            buffer.submit(item);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static <T> T takeQuietly(BoundedBufferWithCondition<T> buffer) {
        try {
            return buffer.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
