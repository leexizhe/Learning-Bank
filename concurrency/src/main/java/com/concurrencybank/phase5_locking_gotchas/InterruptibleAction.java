package com.concurrencybank.phase5_locking_gotchas;

@FunctionalInterface
public interface InterruptibleAction {
  void run() throws InterruptedException;
}
