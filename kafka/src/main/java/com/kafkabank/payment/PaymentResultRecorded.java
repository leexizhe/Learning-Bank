package com.kafkabank.payment;

/**
 * "An outbox row was just committed" — an in-JVM Spring application event, used to nudge the relay awake so the common
 * case doesn't wait out a poll interval.
 *
 * <p>Deliberately <b>not</b> in {@code com.kafkabank.common}. That package is the allowlist named by {@code
 * spring.json.trusted.packages}, which is to say it is the set of types that cross the wire as Kafka payloads. This one
 * never leaves the JVM, and putting it there would blur a boundary that is doing real security work — {@code
 * JsonDeserializer}'s trusted-packages list exists precisely so a hostile producer can't name an arbitrary class for
 * the consumer to instantiate.
 *
 * <p>Carries only the row id, not the row. The listener runs after commit, so it re-reads through the relay's own claim
 * query rather than trusting a detached entity — and if the row has already been picked up by the poller in the
 * meantime, the claim simply skips it.
 */
public record PaymentResultRecorded(Long outboxId) {}
