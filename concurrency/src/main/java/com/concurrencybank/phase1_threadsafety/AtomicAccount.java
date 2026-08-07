package com.concurrencybank.phase1_threadsafety;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lock-free alternative to {@link SynchronizedAccount}. {@link AtomicLong} makes {@code deposit} atomic for free via
 * {@code addAndGet}. {@code withdraw} still has a check-then-act shape (read balance, decide, write balance), so it
 * uses a compare-and-swap retry loop instead of a lock: read the current balance, compute the new one, and only commit
 * with {@code compareAndSet} if nobody else changed the balance in between. If another thread won the race, the CAS
 * fails and the loop retries with the fresh value — no thread ever blocks, but a heavily-contended account will spin.
 */
public class AtomicAccount {

    private final AtomicLong balance = new AtomicLong();

    public void deposit(long amount) {
        balance.addAndGet(amount);
    }

    public boolean withdraw(long amount) {
        while (true) {
            long current = balance.get();
            if (current < amount) {
                return false;
            }
            long next = current - amount;
            if (balance.compareAndSet(current, next)) {
                return true;
            }
            // CAS lost the race to another thread; retry with the fresh value.
        }
    }

    public long getBalance() {
        return balance.get();
    }
}
