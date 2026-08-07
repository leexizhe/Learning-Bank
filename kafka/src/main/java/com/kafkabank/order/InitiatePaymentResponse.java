package com.kafkabank.order;

/**
 * Deliberately does <b>not</b> say "payment succeeded" — at the moment this is returned, nothing has been debited. All
 * that's happened is a durable write to Kafka. That's why the endpoint answers 202 Accepted, not 201 Created, and why
 * the client is handed a {@code paymentId} to poll or subscribe on.
 */
public record InitiatePaymentResponse(String paymentId, String eventId, int partition, long offset) {}
