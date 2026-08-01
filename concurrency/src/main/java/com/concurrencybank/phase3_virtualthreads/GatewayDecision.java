package com.concurrencybank.phase3_virtualthreads;

import java.util.List;

public record GatewayDecision(
    String transactionId, boolean approved, List<ValidationResult> checks) {}
