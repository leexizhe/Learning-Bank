package com.concurrencybank.phase2_deadlock;

import java.util.concurrent.TimeUnit;

/**
 * Deadlock trap: transfer(A, B) locks A then B; a concurrent transfer(B, A) locks B then A. If both threads win their
 * first lock and then wait on the second, each holds what the other wants — a circular wait, one of the four Coffman
 * conditions for deadlock.
 *
 * <p>Fix: a global lock order. Every transfer, regardless of which account is "from" and which is "to", always locks
 * the account with the lower id first. Two accounts can then never be locked in opposite orders by two different
 * threads, so circular wait is structurally impossible.
 *
 * <p>Belt and suspenders: {@link java.util.concurrent.locks.ReentrantLock#tryLock} with a timeout is used instead of a
 * blocking {@code lock()}. If a lock is held by something unexpectedly slow, this fails fast with {@link
 * TransferTimeoutException} instead of tying up a thread forever.
 */
public class LockOrderedTransferService {

    private static final long LOCK_TIMEOUT_MILLIS = 500;

    public void transfer(LockedAccount from, LockedAccount to, long amount) {
        boolean fromIsLower = from.getId() < to.getId();
        LockedAccount first = fromIsLower ? from : to;
        LockedAccount second = fromIsLower ? to : from;

        boolean firstLocked = false;
        boolean secondLocked = false;
        try {
            tryLock(first);
            firstLocked = true;
            tryLock(second);
            secondLocked = true;

            if (from.getBalance() < amount) {
                throw new InsufficientFundsException(from.getId(), from.getBalance(), amount);
            }
            from.debit(amount);
            to.credit(amount);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for an account lock", e);
        } finally {
            // Unlock in reverse acquisition order; harmless either way here since these are two independent locks, but
            // it's the habit that matters.
            if (secondLocked) {
                second.getLock().unlock();
            }
            if (firstLocked) {
                first.getLock().unlock();
            }
        }
    }

    private void tryLock(LockedAccount account) throws InterruptedException {
        boolean locked = account.getLock().tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        if (!locked) {
            throw new TransferTimeoutException(account.getId());
        }
    }
}
