package com.kafkabank.common;

/**
 * The outcome of processing a {@link PaymentInitiated}. Published to {@link
 * Topics#PAYMENT_RESULTS}, keyed by {@code accountId} like its counterpart so both topics partition
 * identically.
 *
 * @param reason human-readable "why", populated on REJECTED so the customer (and the audit log) get
 *     more than a bare status.
 */
public record PaymentResult(
    String eventId,
    String paymentId,
    Long accountId,
    long amountMinor,
    PaymentStatus status,
    String reason,
    long balanceAfterMinor) {}
