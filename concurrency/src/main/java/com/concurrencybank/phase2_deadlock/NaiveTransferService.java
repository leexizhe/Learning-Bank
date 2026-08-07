package com.concurrencybank.phase2_deadlock;

/**
 * The bug that {@link LockOrderedTransferService} exists to fix, kept as running code so the deadlock can be
 * <b>demonstrated</b> rather than described. Locks {@code from} and then {@code to} — in the order the caller happened
 * to name them, which is to say in no order at all.
 *
 * <p>Two concurrent transfers in opposite directions is all it takes. {@code transfer(A, B)} takes A's lock and reaches
 * for B's; {@code transfer(B, A)} takes B's lock and reaches for A's. Each thread now holds exactly what the other is
 * waiting for, neither will ever release it, and both are parked forever: <b>circular wait</b>, the fourth of the
 * Coffman conditions. Ordering by account id — what {@link LockOrderedTransferService} does — breaks that one condition
 * structurally, which is why it is enough on its own.
 *
 * <p>Note the two things this class does that the fixed version deliberately doesn't, because both make the trap worse:
 *
 * <ul>
 *   <li>it uses a blocking {@code lock()} rather than {@code tryLock} with a timeout, so there is no upper bound on the
 *       wait and therefore no diagnostic — the thread simply stops, forever, with no exception;
 *   <li>it locks in caller-supplied order, so the lock order is a property of the <em>call site</em> rather than of the
 *       data. That's what makes this class of bug so hard to spot in review: no single method looks wrong.
 * </ul>
 *
 * <p><b>Not for production use</b> — this deadlocks on purpose, and {@code NaiveTransferServiceDeadlockTest} asserts
 * that it does, using the JVM's own deadlock detector as the witness.
 */
public class NaiveTransferService {

    public void transfer(LockedAccount from, LockedAccount to, long amount) {
        transfer(from, to, amount, () -> {});
    }

    /**
     * @param betweenLocks a test seam, run while the first lock is held and before the second is requested. Production
     *     callers use the two-argument overload; the demo passes a rendezvous here so that both threads are guaranteed
     *     to hold their first lock before either asks for its second. That makes the deadlock <b>deterministic</b>
     *     rather than a race the test has to hope wins — the same "make the interleaving explicit" trick the postgres
     *     module uses with its {@code afterRead} and {@code duringHold} seams.
     */
    void transfer(LockedAccount from, LockedAccount to, long amount, Runnable betweenLocks) {
        from.getLock().lock();
        try {
            betweenLocks.run();
            // Everything past this line is unreachable once the other direction is in flight: the other thread already
            // holds `to`.
            to.getLock().lock();
            try {
                if (from.getBalance() < amount) {
                    throw new InsufficientFundsException(from.getId(), from.getBalance(), amount);
                }
                from.debit(amount);
                to.credit(amount);
            } finally {
                to.getLock().unlock();
            }
        } finally {
            from.getLock().unlock();
        }
    }
}
