package com.postgresbank.phase1_isolation;

/** A business rejection, not a concurrency failure - never retried. */
public class InsufficientOverdraftException extends RuntimeException {

  public InsufficientOverdraftException(long combinedBalanceMinor, long amountMinor) {
    super(
        "combined balance " + combinedBalanceMinor + " cannot cover withdrawal of " + amountMinor);
  }
}
