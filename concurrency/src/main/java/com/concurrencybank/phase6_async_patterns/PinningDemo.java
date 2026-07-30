package com.concurrencybank.phase6_async_patterns;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Two ways to guard the same critical section from a virtual thread, one of
 * which quietly defeats the point of using virtual threads at all.
 *
 * <p>{@link #withSynchronized} blocks ({@code Thread.sleep}, standing in for
 * blocking I/O) while holding a {@code synchronized} monitor. Before JDK 24,
 * this <b>pins</b> the virtual thread to its carrier platform thread for the
 * whole block: the carrier can't be released to run any other virtual thread
 * in the meantime, so the scalability virtual threads are supposed to buy you
 * quietly evaporates for exactly the code path that most needs it (blocking
 * I/O under a lock). JDK 24 (JEP 491) removed pinning for {@code synchronized}
 * specifically, but plenty of production JVMs still run pre-24, and this
 * distinction is a real, current interview topic.
 *
 * <p>{@link #withReentrantLock} guards the identical critical section with a
 * {@link ReentrantLock} instead. {@code lock()}/{@code unlock()} are ordinary
 * method calls, not a JVM monitor operation, so they never pin the carrier -
 * this has always been true, on every JDK version.
 *
 * <p>Pinning isn't something a fast unit test can assert on reliably without
 * parsing JFR events or stderr output, so the test here only proves both
 * methods are still correctness-safe under concurrent virtual-thread load.
 * To actually <em>see</em> the difference, run with
 * {@code -Djdk.tracePinnedThreads=short} and watch stderr: the
 * {@code synchronized} version prints a pinned-thread warning for every call,
 * the {@code ReentrantLock} version prints nothing.
 */
public class PinningDemo {

    private final Object monitor = new Object();
    private final ReentrantLock lock = new ReentrantLock();

    private long synchronizedCallCount;
    private long lockCallCount;

    public void withSynchronized(long simulatedIoMillis) throws InterruptedException {
        synchronized (monitor) {
            Thread.sleep(simulatedIoMillis);
            synchronizedCallCount++;
        }
    }

    public void withReentrantLock(long simulatedIoMillis) throws InterruptedException {
        lock.lock();
        try {
            Thread.sleep(simulatedIoMillis);
            lockCallCount++;
        } finally {
            lock.unlock();
        }
    }

    public long getSynchronizedCallCount() {
        return synchronizedCallCount;
    }

    public long getLockCallCount() {
        return lockCallCount;
    }
}
