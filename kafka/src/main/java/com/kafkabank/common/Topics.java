package com.kafkabank.common;

import org.springframework.kafka.retrytopic.RetryTopicConstants;

/**
 * Topic names in one place, because a typo in a topic string is a silent failure: the producer
 * happily auto-creates {@code paymnet-events} and the consumer sits on {@code payment-events}
 * forever, receiving nothing, with no error anywhere.
 */
public final class Topics {

  /**
   * Where a payment starts life. Produced by the order role, consumed by payment + reconciliation.
   */
  public static final String PAYMENT_EVENTS = "payment-events";

  /**
   * The outcome of a payment (ACCEPTED/REJECTED). Produced by the payment role, consumed by
   * reconciliation.
   */
  public static final String PAYMENT_RESULTS = "payment-results";

  /**
   * Where records land after retries are exhausted. Spring's {@code @RetryableTopic} derives this
   * name by suffixing the main topic, so the suffix is taken from Spring's own constant rather than
   * hardcoded as {@code "-dlt"} — that way the name we consume from in tests can never drift from
   * the name Spring publishes to.
   */
  public static final String PAYMENT_EVENTS_DLT =
      PAYMENT_EVENTS + RetryTopicConstants.DEFAULT_DLT_SUFFIX;

  private Topics() {}
}
