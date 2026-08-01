package com.concurrencybank.phase5_locking_gotchas;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class ExchangeRateServiceTest {

  @Test
  void doubleCheckedLockingSingletonIsCreatedExactlyOnceUnderConcurrentCallers()
      throws InterruptedException {
    Set<ExchangeRateService> seenInstances = ConcurrentHashMap.newKeySet();

    runConcurrently(200, () -> seenInstances.add(ExchangeRateService.getInstance()));

    assertThat(seenInstances).hasSize(1);
  }

  @Test
  void holderIdiomSingletonIsAlsoCreatedExactlyOnceUnderConcurrentCallers()
      throws InterruptedException {
    Set<ExchangeRateServiceHolder> seenInstances = ConcurrentHashMap.newKeySet();

    runConcurrently(200, () -> seenInstances.add(ExchangeRateServiceHolder.getInstance()));

    assertThat(seenInstances).hasSize(1);
  }
}
