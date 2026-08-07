package com.concurrencybank.phase5_locking_gotchas;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An in-memory ledger with no explicit locks anywhere. The classic interview trap this answers: reaching for a plain
 * {@code HashMap} (or synchronizing by hand around one) when {@link ConcurrentHashMap} already solves it.
 *
 * <p>{@code compute}/{@code merge} on a {@code ConcurrentHashMap} hold an internal per-bin lock for the duration of the
 * remapping function — that's the "per-bucket synchronization" that lets different keys update concurrently while
 * updates to the <em>same</em> key are still atomic. It's the same guarantee {@code
 * phase2_deadlock.LockOrderedTransferService} gets from an explicit {@code ReentrantLock} per account, just handed to
 * you by the collection instead of hand-written.
 *
 * <p>The catch, worth naming out loud: the remapping lambda runs while that per-bin lock is held, so it must be fast
 * and must never call back into the same map (risk of deadlock) or block on I/O.
 */
public class ConcurrentLedger {

    private final ConcurrentHashMap<String, Long> balances = new ConcurrentHashMap<>();

    public void deposit(String accountId, long amountMinor) {
        balances.merge(accountId, amountMinor, Long::sum);
    }

    public boolean withdraw(String accountId, long amountMinor) {
        AtomicBoolean sufficientFunds = new AtomicBoolean(false);
        balances.compute(accountId, (id, balance) -> {
            long current = balance == null ? 0 : balance;
            if (current < amountMinor) {
                sufficientFunds.set(false);
                return current; // unchanged - the whole point of compute() over merge() here
            }
            sufficientFunds.set(true);
            return current - amountMinor;
        });
        return sufficientFunds.get();
    }

    public long getBalance(String accountId) {
        return balances.getOrDefault(accountId, 0L);
    }
}
