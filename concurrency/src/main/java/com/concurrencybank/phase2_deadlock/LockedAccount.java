package com.concurrencybank.phase2_deadlock;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Each account owns its own {@link ReentrantLock}. A transfer between two
 * accounts must hold both locks at once — which is exactly how deadlocks are
 * born if two transfers lock the same pair of accounts in opposite orders. See
 * {@link LockOrderedTransferService} for the fix.
 */
public class LockedAccount {

    private final long id;
    private long balance;
    private final ReentrantLock lock = new ReentrantLock();

    public LockedAccount(long id, long balance) {
        this.id = id;
        this.balance = balance;
    }

    public long getId() {
        return id;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    /** Only meaningful while the caller holds {@link #getLock()}. */
    public long getBalance() {
        return balance;
    }

    void debit(long amount) {
        balance -= amount;
    }

    void credit(long amount) {
        balance += amount;
    }
}
