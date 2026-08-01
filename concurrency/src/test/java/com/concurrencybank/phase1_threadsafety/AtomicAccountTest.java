package com.concurrencybank.phase1_threadsafety;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AtomicAccountTest {

  @Test
  void concurrentDepositsNeverLoseAnUpdate() throws InterruptedException {
    AtomicAccount account = new AtomicAccount();
    int threads = 50;
    int depositsPerThread = 10_000;

    runConcurrently(
        threads,
        () -> {
          for (int i = 0; i < depositsPerThread; i++) {
            account.deposit(1);
          }
        });

    assertThat(account.getBalance()).isEqualTo((long) threads * depositsPerThread);
  }

  @Test
  void concurrentWithdrawalsNeverOverdraw() throws InterruptedException {
    int threads = 200;
    AtomicAccount account = new AtomicAccount();
    account.deposit(threads);
    AtomicInteger successes = new AtomicInteger();

    runConcurrently(
        threads,
        () -> {
          if (account.withdraw(1)) {
            successes.incrementAndGet();
          }
        });

    assertThat(successes.get()).isEqualTo(threads);
    assertThat(account.getBalance()).isZero();
  }
}
