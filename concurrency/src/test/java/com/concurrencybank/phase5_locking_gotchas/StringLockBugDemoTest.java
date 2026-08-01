package com.concurrencybank.phase5_locking_gotchas;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class StringLockBugDemoTest {

  private static final long WORK_MILLIS = 100;

  @Test
  void unrelatedOperationsSerializeBecauseTheySharedAnInternedStringLock()
      throws InterruptedException {
    StringLockBugDemo demo = new StringLockBugDemo();

    Instant start = Instant.now();
    Thread auditThread =
        new Thread(
            () -> {
              try {
                demo.writeAuditLog("entry", WORK_MILLIS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    Thread ledgerThread =
        new Thread(
            () -> {
              try {
                demo.postLedgerEntry(500, WORK_MILLIS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    auditThread.start();
    ledgerThread.start();
    auditThread.join();
    ledgerThread.join();
    Duration elapsed = Duration.between(start, Instant.now());

    // Two logically-unrelated 100ms operations should overlap and take
    // ~100ms. Because both synchronize on the same interned literal, they
    // instead serialize and take ~200ms.
    assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(WORK_MILLIS * 2));
  }

  @Test
  void lockStripedRegistryLetsUnrelatedKeysRunConcurrently() throws InterruptedException {
    LockStripedRegistry registry = new LockStripedRegistry();

    Instant start = Instant.now();
    Thread auditThread =
        new Thread(
            () ->
                runQuietly(
                    () -> registry.runExclusively("audit-log", () -> Thread.sleep(WORK_MILLIS))));
    Thread ledgerThread =
        new Thread(
            () ->
                runQuietly(
                    () -> registry.runExclusively("ledger", () -> Thread.sleep(WORK_MILLIS))));
    auditThread.start();
    ledgerThread.start();
    auditThread.join();
    ledgerThread.join();
    Duration elapsed = Duration.between(start, Instant.now());

    // Different keys -> different lock objects -> genuinely concurrent.
    assertThat(elapsed).isLessThan(Duration.ofMillis(WORK_MILLIS * 2));
  }

  private void runQuietly(InterruptibleAction action) {
    try {
      action.run();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
