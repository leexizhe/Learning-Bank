package com.concurrencybank.phase7_primitives;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * A fixed-size pool of expensive objects — think database connections — that you
 * borrow, use, and return. The classic 45-minute task, and the one where the
 * interesting failures are all in the <em>return</em> path rather than the
 * borrow path.
 *
 * <p><b>The {@link Semaphore} is the pool, the queue is just storage.</b> The
 * permit count is what bounds concurrency; {@code idle} merely holds the objects
 * not currently lent out. Getting this backwards — bounding on the queue and
 * treating the semaphore as bookkeeping — is how you end up with a pool that
 * hands out a fresh object whenever the queue happens to be empty, which is to
 * say a pool that doesn't bound anything.
 *
 * <p><b>Borrowing must time out.</b> {@code semaphore.acquire()} with no deadline
 * turns a saturated pool into an unbounded queue of blocked threads, and the
 * symptom in production is not "the pool is full" but "every request thread in
 * the service is parked and the app is dead". Bounded waiting converts that into
 * a fast, attributable failure — the same reasoning as {@code tryLock} with a
 * timeout in {@code phase2_deadlock}, and the same reasoning behind
 * HikariCP's {@code connectionTimeout}.
 *
 * <p><b>Returning is where the bugs are, so returning is not the caller's job.</b>
 * A hand-rolled {@code borrow()}/{@code release()} pair leaks a permit on every
 * path that forgets its {@code finally}, and a leaked permit is permanent — the
 * pool shrinks by one, silently, until it reaches zero and the service stops.
 * So {@link #borrow} hands back an {@link AutoCloseable} {@link Lease} instead
 * of a bare object, which makes try-with-resources the natural spelling and the
 * {@code finally} impossible to omit.
 *
 * <p><b>The sharp edge worth knowing cold:</b> {@code Lease.close()} is
 * <b>idempotent</b>. The opposite mistake to leaking is double-releasing — a
 * caller that closes the lease and then closes it again in a {@code finally}
 * adds a permit that was never taken, and the pool's ceiling silently rises
 * above its configured size. A leaked permit shrinks the pool and you eventually
 * notice; a double release <em>inflates</em> it, so you quietly exceed the limit
 * you built the pool to enforce, and you find out from the database's
 * {@code max_connections} instead. The {@code returned} flag is the whole
 * defence, and "semaphores don't check that the releaser was the acquirer" is
 * the fact that makes it necessary.
 *
 * <p>Objects are created lazily on first demand rather than all N up front,
 * so a pool sized for peak doesn't pay for peak while idle.
 */
public class BorrowablePool<T> implements AutoCloseable {

    private final Semaphore permits;
    private final Queue<T> idle = new ConcurrentLinkedQueue<>();
    private final Supplier<T> factory;

    private volatile boolean closed;

    public BorrowablePool(int size, Supplier<T> factory) {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be positive, was " + size);
        }
        this.permits = new Semaphore(size);
        this.factory = factory;
    }

    /**
     * @throws TimeoutException when no object became available in time — a
     *         deliberate, attributable failure rather than an unbounded wait
     */
    public Lease borrow(Duration timeout) throws InterruptedException, TimeoutException {
        if (closed) {
            throw new IllegalStateException("pool is closed");
        }
        if (!permits.tryAcquire(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
            throw new TimeoutException("no object available within " + timeout);
        }
        // A permit is held from here on, so every path out of this method must
        // either hand it to a Lease or give it back.
        T item = idle.poll();
        return new Lease(item != null ? item : factory.get());
    }

    /** How many objects could still be handed out right now. */
    public int available() {
        return permits.availablePermits();
    }

    @Override
    public void close() {
        closed = true;
        idle.clear();
    }

    /** A borrowed object plus the obligation to give it back. Close it, ideally via try-with-resources. */
    public final class Lease implements AutoCloseable {

        private final T item;
        private boolean returned;

        private Lease(T item) {
            this.item = item;
        }

        public T get() {
            if (returned) {
                throw new IllegalStateException("lease already returned to the pool");
            }
            return item;
        }

        @Override
        public void close() {
            // Idempotent on purpose: see the class javadoc. A second close is a
            // no-op rather than an extra permit.
            if (returned) {
                return;
            }
            returned = true;
            if (!closed) {
                idle.add(item);
            }
            permits.release();
        }
    }
}
