package com.concurrencybank.phase2_deadlock;

public class InsufficientFundsException extends RuntimeException {

  public InsufficientFundsException(long accountId, long balance, long amount) {
    super("Account " + accountId + " has " + balance + " but needs " + amount);
  }
}
