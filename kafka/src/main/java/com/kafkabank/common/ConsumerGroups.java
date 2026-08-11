package com.kafkabank.common;

/**
 * Consumer group ids in one place, for the same reason {@link Topics} exists: the id is a bare string that nothing
 * validates, and getting it wrong fails silently — a renamed group just starts over from its own offsets, and any test
 * that looks a container up by group id quietly matches nothing.
 */
public final class ConsumerGroups {

    /** Moves the money. One group, so N instances share the partitions and no payment is debited twice. */
    public static final String PAYMENT = "payment-service";

    /**
     * Reads the same topic under its own group, so it gets a complete copy of the stream with independent offsets —
     * the fan-out half of the consumer-group model.
     */
    public static final String RECONCILIATION = "reconciliation-service";

    private ConsumerGroups() {}
}
