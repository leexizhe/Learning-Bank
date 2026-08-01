package com.concurrencybank.phase5_locking_gotchas;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class BankRegistryTest {

  private static final long WORK_MILLIS = 100;

  @Test
  void twoInstancesInstanceMethodsDontBlockEachOther() throws InterruptedException {
    BankRegistry branchOne = new BankRegistry();
    BankRegistry branchTwo = new BankRegistry();

    Duration elapsed =
        runConcurrently(
            () -> branchOne.recordActivity(WORK_MILLIS),
            () -> branchTwo.recordActivity(WORK_MILLIS));

    assertThat(elapsed).isLessThan(Duration.ofMillis(WORK_MILLIS * 2));
  }

  @Test
  void twoThreadsCallingTheStaticMethodDoSerialize() throws InterruptedException {
    Duration elapsed =
        runConcurrently(
            () -> BankRegistry.nextTransactionId(WORK_MILLIS),
            () -> BankRegistry.nextTransactionId(WORK_MILLIS));

    // Same class -> same lock -> the second caller waits for the first.
    assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(WORK_MILLIS * 2));
  }

  @Test
  void theStaticLockAndAnInstanceLockOnTheSameObjectAreIndependent() throws InterruptedException {
    BankRegistry registry = new BankRegistry();

    Duration elapsed =
        runConcurrently(
            () -> BankRegistry.nextTransactionId(WORK_MILLIS),
            () -> registry.recordActivity(WORK_MILLIS));

    // Class-level lock and this same object's instance-level lock are two
    // different monitors, so these don't block each other either.
    assertThat(elapsed).isLessThan(Duration.ofMillis(WORK_MILLIS * 2));
  }

  private Duration runConcurrently(Runnable first, Runnable second) throws InterruptedException {
    Instant start = Instant.now();
    Thread t1 = new Thread(first);
    Thread t2 = new Thread(second);
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    return Duration.between(start, Instant.now());
  }
}
