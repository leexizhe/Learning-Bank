package com.kafkabank.payment;

/**
 * The event references an account that doesn't exist. Unlike insufficient funds — a legitimate business outcome we
 * answer with a REJECTED result — this means the message itself is unprocessable, so it dead-letters rather than
 * becoming a customer-facing decision.
 *
 * <p>Permanent by construction: it extends {@link PermanentFailureException}, so the retry configuration classifies it
 * without anyone having to remember to add it to a list.
 */
public class UnknownAccountException extends PermanentFailureException {

    public UnknownAccountException(Long accountId) {
        super("Unknown account " + accountId);
    }
}
