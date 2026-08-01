package com.concurrencybank.phase1_threadsafety;

/**
 * Deliberately broken. {@code balance++} is read-modify-write: under concurrent calls, threads can
 * both read the same value before either writes it back, so increments are lost. Exists only so a
 * test can demonstrate the bug before the fixed versions ({@link SynchronizedAccount}, {@link
 * AtomicAccount}) are shown.
 */
public class UnsafeCounter {

  private long balance;

  public void deposit(long amount) {
    balance += amount;
  }

  public long getBalance() {
    return balance;
  }
}
