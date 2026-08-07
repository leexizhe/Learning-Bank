package com.kafkabank.common;

public enum PaymentStatus {
    /** Funds were available and the debit committed. */
    ACCEPTED,
    /**
     * A valid request we deliberately declined (e.g. insufficient funds). NOT an error - no retry, no DLT.
     */
    REJECTED
}
