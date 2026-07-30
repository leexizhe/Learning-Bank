package com.kafkabank.payment;

/**
 * "No such account" on the HTTP read path.
 *
 * <p>Deliberately <b>not</b> {@link UnknownAccountException}, even though the
 * condition is the same. That type's identity is load-bearing in
 * {@code PaymentConsumer}'s retry configuration — it means "dead-letter this
 * message". Reusing it here would tangle two unrelated policies together, so
 * that changing how the API reports a missing account could quietly change how
 * the consumer retries. Same condition, different decisions, different types.
 */
public class AccountNotFoundException extends RuntimeException {

    public AccountNotFoundException(Long accountId) {
        super("No account with id " + accountId);
    }
}
