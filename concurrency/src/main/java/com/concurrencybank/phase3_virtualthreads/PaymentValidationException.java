package com.concurrencybank.phase3_virtualthreads;

public class PaymentValidationException extends RuntimeException {

  public PaymentValidationException(String transactionId, Throwable cause) {
    super("Validation failed for transaction " + transactionId + ": " + cause.getMessage(), cause);
  }
}
