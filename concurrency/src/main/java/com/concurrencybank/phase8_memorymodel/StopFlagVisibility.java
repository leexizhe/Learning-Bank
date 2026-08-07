package com.concurrencybank.phase8_memorymodel;

/**
 * Visibility, isolated from atomicity. Two identical spin loops watching two identical {@code boolean} flags; the only
 * difference is one keyword, and the one without it may never notice the flag was cleared.
 *
 * <p><b>What actually goes wrong.</b> A plain field carries no happens-before edge, so the JIT is entitled to assume
 * nothing else changes it and hoist the read clean out of the loop — turning {@code while (running) spins++} into
 * {@code if (running) while (true) spins++}. That's not a bug in the JIT; it is a legal transformation given what the
 * JMM promises, and it's why the loop can run forever after another thread has plainly set the field to {@code false}.
 * People reach for "the value is cached in a CPU register or an L1 line", which is the right intuition but the wrong
 * mechanism — the optimisation usually happens in the compiler, long before the hardware gets involved.
 *
 * <p>{@code volatile} forbids the hoist, and adds the happens-before edge that makes the write visible: everything the
 * writer did before setting the flag is visible to whoever reads it as {@code false}.
 *
 * <p><b>What {@code volatile} does not buy you</b> — the follow-up question, and the reason this class holds only flags
 * and not a counter. It gives visibility, never atomicity. A flag has a single writer and a single meaningful
 * transition, so visibility is the whole problem and {@code volatile} solves it. {@code count++} is a read-modify-write
 * over three bytecodes, and no amount of {@code volatile} makes those three steps indivisible — that needs {@code
 * AtomicLong}'s CAS or a lock. <b>Use {@code volatile} for flags, never for counters</b> is the compressed version.
 *
 * <p>{@code StopFlagVisibilityTest} asserts the guarantee (the {@code volatile} loop always terminates) and merely
 * <em>observes</em> the anomaly (the plain loop is run on a daemon thread and whether it hangs is logged, not asserted)
 * — the same discipline as {@link UnsafePublication}, and for the same reason: whether the JIT chooses to hoist depends
 * on how long it warmed up, which is not something a build should depend on.
 */
public class StopFlagVisibility {

    private boolean plainRunning = true;
    private volatile boolean volatileRunning = true;

    /** May never return, even long after {@link #stopPlain()} has been called. */
    public long spinUntilPlainFlagClears() {
        long spins = 0;
        while (plainRunning) {
            spins++;
        }
        return spins;
    }

    /** Always returns promptly once {@link #stopVolatile()} has been called. */
    public long spinUntilVolatileFlagClears() {
        long spins = 0;
        while (volatileRunning) {
            spins++;
        }
        return spins;
    }

    public void stopPlain() {
        plainRunning = false;
    }

    public void stopVolatile() {
        volatileRunning = false;
    }
}
