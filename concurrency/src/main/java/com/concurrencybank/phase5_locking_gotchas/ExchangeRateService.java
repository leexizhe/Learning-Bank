package com.concurrencybank.phase5_locking_gotchas;

/**
 * Classic double-checked locking. The first {@code if} check outside the
 * lock is an optimization — after the singleton is created, every later call
 * returns without ever taking the lock. The second check inside the lock
 * guards against two threads both passing the first check and both trying to
 * construct the instance.
 *
 * <p>{@code instance} <b>must</b> be {@code volatile}. Without it, a reading
 * thread could see a non-null reference to a <em>partially constructed</em>
 * object: the JVM/CPU is free to reorder the constructor's writes and the
 * reference assignment, since nothing forbids it without a memory barrier. A
 * second thread could then read the non-null field, skip both checks, and
 * start using an object whose fields aren't fully initialized yet.
 * {@code volatile} inserts the memory barrier that prevents that reordering.
 *
 * <p>See {@link ExchangeRateServiceHolder} for the simpler alternative most
 * engineers reach for instead: the JVM already guarantees a class is
 * initialized at most once, exactly-once, and safely-published, so nested
 * static holder class does all of this for free with less to get wrong.
 */
public class ExchangeRateService {

    private static volatile ExchangeRateService instance;

    private final long createdAtNanos = System.nanoTime();

    private ExchangeRateService() {}

    public static ExchangeRateService getInstance() {
        ExchangeRateService result = instance;
        if (result == null) {
            synchronized (ExchangeRateService.class) {
                result = instance;
                if (result == null) {
                    instance = result = new ExchangeRateService();
                }
            }
        }
        return result;
    }

    public long createdAtNanos() {
        return createdAtNanos;
    }
}
