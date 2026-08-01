package com.kafkabank.common;

/**
 * "A customer hit send." Published to {@link Topics#PAYMENT_EVENTS}.
 *
 * <p>A record, so the event is immutable — an event is a statement about something that already
 * happened, and rewriting it in flight makes no sense.
 *
 * @param eventId unique per <em>message</em>. This is the idempotency key: if the same event is
 *     delivered twice, both copies carry the same eventId, which is how the consumer recognises the
 *     duplicate.
 * @param paymentId unique per <em>payment</em>. Ties the initiated event to its later result event
 *     so reconciliation can match the two halves.
 * @param accountId the account being debited. Also used as the Kafka message KEY, which is what
 *     guarantees all events for one account land on one partition and are therefore processed in
 *     order relative to each other.
 */
public record PaymentInitiated(
    String eventId, String paymentId, Long accountId, long amountMinor, String description) {}
