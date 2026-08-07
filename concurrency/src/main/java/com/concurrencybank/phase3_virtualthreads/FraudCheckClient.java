package com.concurrencybank.phase3_virtualthreads;

import java.time.Duration;
import org.springframework.stereotype.Component;

/**
 * Stand-in for a slow external fraud-scoring API. {@code Thread.sleep} here is exactly the point: on a virtual thread
 * it parks the thread cheaply instead of pinning a precious OS thread for the whole call, which is what makes it safe
 * to fan out many of these concurrently.
 */
@Component
public class FraudCheckClient {

    private final Duration latency;

    public FraudCheckClient() {
        this(Duration.ofMillis(200));
    }

    public FraudCheckClient(Duration latency) {
        this.latency = latency;
    }

    public ValidationResult check(String transactionId) throws InterruptedException {
        Thread.sleep(latency);
        return SimulatedExternalCheck.decide("fraud", transactionId, "FRAUD-", "clear", "flagged as likely fraud");
    }
}
