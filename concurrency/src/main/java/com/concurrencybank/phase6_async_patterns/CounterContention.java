package com.concurrencybank.phase6_async_patterns;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * {@link AtomicLong} and {@link LongAdder} counting the same events, so the trade-off between them can be pointed at
 * rather than asserted from memory.
 *
 * <p>"For a single counter, {@code AtomicLong} usually beats a lock" is true and incomplete. {@code AtomicLong} is one
 * memory location, so every incrementing thread CASes the <em>same</em> cache line; under heavy write contention most
 * of those CASes fail and retry, the line ping-pongs between cores, and throughput collapses precisely when you need
 * it. {@code LongAdder} spreads the count over an internal array of cells, one per contending thread in the good case,
 * so threads mostly stop colliding at all — an order of magnitude better under write contention, and it grows that
 * array only when it detects collisions, so the uncontended case stays cheap.
 *
 * <p><b>The trade-off, and the reason this class lives in a banking repo:</b> {@link LongAdder#sum()} is <b>not
 * atomic</b> with respect to concurrent updates. It walks the cells and adds them up, so a value returned while writes
 * are in flight corresponds to no single instant in time — and there is no compare-and-set, no {@code getAndIncrement},
 * no way to make a decision based on the current value. {@code AtomicLong} gives you all three.
 *
 * <p>Which lands on a rule worth saying out loud: <b>{@code LongAdder} for a request counter, never for an account
 * balance.</b> Metrics are written constantly, read rarely, and nothing branches on them — a count that is momentarily
 * approximate costs nothing. A balance is read in order to decide whether a withdrawal may proceed, which needs an
 * exact value and an atomic read-modify-write; that is why {@code AtomicAccount.withdraw} can use a CAS retry loop and
 * why the same trick is unavailable here.
 *
 * <p>The test asserts <b>correctness, not speed</b> — both counters must reach the identical exact total — and merely
 * logs the elapsed times. Timing assertions belong in a JMH benchmark, not a build: the ratio depends on core count,
 * JIT warmup and what else the machine is doing, so a threshold that passes here would flake in CI and teach nothing
 * either way.
 */
public class CounterContention {

    private final AtomicLong atomic = new AtomicLong();
    private final LongAdder adder = new LongAdder();

    public void incrementAtomic() {
        atomic.incrementAndGet();
    }

    public void incrementAdder() {
        adder.increment();
    }

    public long atomicValue() {
        return atomic.get();
    }

    /** Accurate once writers have stopped; approximate while they haven't. */
    public long adderValue() {
        return adder.sum();
    }
}
