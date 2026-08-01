package com.kafkabank.payment;

/**
 * Marker base type for "retrying this will never help."
 *
 * <p>{@code @RetryableTopic} excludes this one type rather than listing every individual
 * non-retryable exception. That difference matters: with an explicit list, adding a new permanent
 * failure means remembering to also edit the annotation, and forgetting silently costs three
 * pointless retries before the inevitable dead-letter. Here a new permanent failure just extends
 * this class and is correctly classified the moment it's written.
 *
 * <p>The rule of thumb: <b>transient</b> failures (a database blip, a timeout, a downstream service
 * restarting) are worth retrying. <b>Permanent</b> ones (a malformed payload, an account that
 * doesn't exist) are not — they're unprocessable no matter how many times you try, so they should
 * reach a human as fast as possible.
 */
public abstract class PermanentFailureException extends RuntimeException {

  protected PermanentFailureException(String message) {
    super(message);
  }
}
