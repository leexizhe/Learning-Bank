package com.kafkabank.order;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record InitiatePaymentRequest(
    @NotNull Long accountId, @Positive long amountMinor, String description) {}
