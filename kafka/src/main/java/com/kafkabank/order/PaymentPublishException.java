package com.kafkabank.order;

public class PaymentPublishException extends RuntimeException {

  public PaymentPublishException(String paymentId, Throwable cause) {
    // A bare TimeoutException carries no message, so fall back to its type name -
    // otherwise the customer-visible detail reads "...: null", which tells nobody
    // anything about what went wrong.
    super(
        "Failed to publish payment "
            + paymentId
            + ": "
            + (cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName()),
        cause);
  }
}
