package com.concurrencybank.phase2_deadlock;

/**
 * Thrown when an account lock can't be acquired within the timeout. In a real payment system this is the better failure
 * mode: return "service unavailable" for this one request after a bounded wait, rather than block a request thread
 * indefinitely because some other slow operation is holding the lock.
 */
public class TransferTimeoutException extends RuntimeException {

    public TransferTimeoutException(long accountId) {
        super("Timed out waiting for lock on account " + accountId);
    }
}
