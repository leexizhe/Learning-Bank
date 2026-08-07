package com.concurrencybank.phase6_async_patterns;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * A bounded buffer of pending requests, built on the intrinsic lock plus {@code wait()}/{@code notifyAll()} directly —
 * no {@code BlockingQueue}. A teller can only serve one customer at a time; if the queue is full, customers ({@code
 * submit}) wait for room, and if it's empty, tellers ({@code take}) wait for a customer.
 *
 * <p>{@code wait()} is always in a {@code while} loop, never an {@code if}: the JVM is allowed to wake a waiting thread
 * for no reason at all (a "spurious wakeup"), and even a genuine {@code notifyAll()} only means "the condition might
 * have changed" — by the time this thread re-acquires the lock, another thread could already have grabbed the slot it
 * was waiting for. The {@code while} re-checks the actual condition every time control returns from {@code wait()},
 * instead of trusting that a wakeup means it's safe to proceed.
 *
 * <p>{@code notifyAll()}, not {@code notify()}: this queue has two different kinds of waiters on the same monitor —
 * producers waiting for space, consumers waiting for an item. {@code notify()} wakes one arbitrary waiter, chosen by
 * the JVM, with no way to target "a consumer" specifically. Wake the wrong kind (e.g. another producer, when a consumer
 * just took an item and freed space for a producer) and that thread checks its condition, finds it's still false, and
 * goes right back to waiting — the real waiter that should have been woken never gets a chance. {@code notifyAll()}
 * wakes everyone; each re-checks its own condition in its {@code while} loop and only the ones that can actually
 * proceed do.
 */
public class TellerQueue<T> {

    private final Queue<T> items = new ArrayDeque<>();
    private final int capacity;

    public TellerQueue(int capacity) {
        this.capacity = capacity;
    }

    public synchronized void submit(T item) throws InterruptedException {
        while (items.size() == capacity) {
            wait();
        }
        items.add(item);
        notifyAll();
    }

    public synchronized T take() throws InterruptedException {
        while (items.isEmpty()) {
            wait();
        }
        T item = items.remove();
        notifyAll();
        return item;
    }

    public synchronized int size() {
        return items.size();
    }
}
