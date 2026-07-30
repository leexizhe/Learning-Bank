package com.kafkabank.common;

public enum ReconciliationState {
    /** Only one half of the story has arrived so far - still waiting for its pair. */
    PENDING,
    /** Both halves seen, and the payment succeeded. */
    CONFIRMED,
    /** Both halves seen, and the payment was rejected - anything downstream must be undone. */
    ROLLBACK
}
