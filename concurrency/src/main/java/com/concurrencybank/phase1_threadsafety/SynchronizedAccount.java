package com.concurrencybank.phase1_threadsafety;

/**
 * Object-level locking: every {@code synchronized} instance method acquires this object's monitor
 * (equivalent to {@code synchronized(this) { ... }}), so deposit/withdraw/getBalance on the same
 * instance can never interleave. Two different {@code SynchronizedAccount} instances do not contend
 * with each other — the lock is per-object, not per-class.
 *
 * <p>{@code withdraw} also guards against the classic "check-then-act" race: checking {@code
 * balance >= amount} and then mutating it must happen as one atomic step, otherwise two concurrent
 * withdrawals can both pass the check and drive the balance negative.
 */
public class SynchronizedAccount {

  private long balance;

  public synchronized void deposit(long amount) {
    balance += amount;
  }

  public synchronized boolean withdraw(long amount) {
    if (balance < amount) {
      return false;
    }
    balance -= amount;
    return true;
  }

  public synchronized long getBalance() {
    return balance;
  }
}
