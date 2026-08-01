package com.concurrencybank.phase5_locking_gotchas;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ConcurrentLedgerTest {

  @Test
  void concurrentDepositsAreAllConserved() throws InterruptedException {
    ConcurrentLedger ledger = new ConcurrentLedger();
    int threads = 50;
    int depositsPerThread = 10_000;

    runConcurrently(
        threads,
        () -> {
          for (int i = 0; i < depositsPerThread; i++) {
            ledger.deposit("acc-1", 1);
          }
        });

    assertThat(ledger.getBalance("acc-1")).isEqualTo((long) threads * depositsPerThread);
  }

  @Test
  void concurrentWithdrawalsNeverOverdraw() throws InterruptedException {
    int threads = 200;
    ConcurrentLedger ledger = new ConcurrentLedger();
    ledger.deposit("acc-1", threads); // exactly enough for one successful withdrawal each
    AtomicInteger successes = new AtomicInteger();

    runConcurrently(
        threads,
        () -> {
          if (ledger.withdraw("acc-1", 1)) {
            successes.incrementAndGet();
          }
        });

    assertThat(successes.get()).isEqualTo(threads);
    assertThat(ledger.getBalance("acc-1")).isZero();
  }

  @Test
  void differentAccountsDontInterfereWithEachOther() throws InterruptedException {
    ConcurrentLedger ledger = new ConcurrentLedger();
    int threads = 100;

    runConcurrently(
        threads,
        () -> {
          String accountId = "acc-" + (Thread.currentThread().hashCode() % 10);
          ledger.deposit(accountId, 5);
        });

    long total = 0;
    for (int i = 0; i < 10; i++) {
      total += ledger.getBalance("acc-" + i);
    }
    assertThat(total).isEqualTo(threads * 5L);
  }
}
