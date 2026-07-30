package com.concurrencybank.phase3_virtualthreads;

import java.time.Duration;
import org.springframework.stereotype.Component;

@Component
public class CreditCheckClient {

    private final Duration latency;

    public CreditCheckClient() {
        this(Duration.ofMillis(150));
    }

    public CreditCheckClient(Duration latency) {
        this.latency = latency;
    }

    public ValidationResult check(String transactionId) throws InterruptedException {
        Thread.sleep(latency);
        return SimulatedExternalCheck.decide(
                "credit", transactionId, "INSUFFICIENT-", "sufficient limit", "over credit limit");
    }
}
