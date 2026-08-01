package com.concurrencybank.phase3_virtualthreads;

/**
 * Represents the sanctions provider itself failing to answer (timeout, 5xx, malformed response) —
 * distinct from the provider answering "not approved". A thrown exception is what makes {@code
 * allSuccessfulOrThrow()} cancel the sibling checks; a plain {@code approved=false} result does
 * not.
 */
public class SanctionsCheckException extends Exception {

  public SanctionsCheckException(String message) {
    super(message);
  }
}
