package com.concurrencybank.phase5_locking_gotchas;

/**
 * Initialization-on-demand holder idiom: the preferred alternative to
 * {@link ExchangeRateService}'s hand-written double-checked locking.
 *
 * <p>{@code Holder} isn't loaded (and so {@code INSTANCE} isn't created)
 * until the first call to {@link #getInstance()} touches the class -
 * classes are loaded lazily. The JVM spec already guarantees class
 * initialization is thread-safe, happens at most once, and safely publishes
 * the result to every thread - exactly what the manual {@code volatile} +
 * double-checked-locking dance above is doing by hand. No {@code volatile},
 * no nested null checks, no way to get the memory-visibility subtlety wrong.
 */
public class ExchangeRateServiceHolder {

    private final long createdAtNanos = System.nanoTime();

    private ExchangeRateServiceHolder() {}

    private static final class Holder {
        static final ExchangeRateServiceHolder INSTANCE = new ExchangeRateServiceHolder();
    }

    public static ExchangeRateServiceHolder getInstance() {
        return Holder.INSTANCE;
    }

    public long createdAtNanos() {
        return createdAtNanos;
    }
}
