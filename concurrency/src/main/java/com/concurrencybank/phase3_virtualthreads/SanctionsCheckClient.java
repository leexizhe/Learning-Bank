package com.concurrencybank.phase3_virtualthreads;

import java.time.Duration;
import org.springframework.stereotype.Component;

@Component
public class SanctionsCheckClient {

  private final Duration latency;

  public SanctionsCheckClient() {
    this(Duration.ofMillis(300));
  }

  public SanctionsCheckClient(Duration latency) {
    this.latency = latency;
  }

  public ValidationResult check(String transactionId)
      throws InterruptedException, SanctionsCheckException {
    Thread.sleep(latency);
    if (transactionId.startsWith("SANCTIONS-ERROR-")) {
      // The provider itself is unreachable/erroring - not a decline.
      throw new SanctionsCheckException("sanctions provider timed out");
    }
    return SimulatedExternalCheck.decide(
        "sanctions", transactionId, "SANCTIONED-", "not listed", "matched sanctions list");
  }
}
