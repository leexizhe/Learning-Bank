package com.postgresbank.phase3_coordination;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The other half of the transactional-outbox pattern: a relay that polls unpublished rows and hands them to whatever
 * the real downstream is (a message broker, in production — see kafka-bank for that half).
 *
 * <p>This class holds only the schedule. The work, and the transaction, live in {@link OutboxRelayTransactionalOps} —
 * read its javadoc before changing this, because collapsing the two back into one class reintroduces a bug that the
 * test suite is structurally unable to catch.
 *
 * <p>{@link OutboxRelayTransactionalOps#relayOnce()} is called both by the scheduled trigger here and directly by
 * {@code OutboxIT}; tests don't wait out a real scheduling interval, they invoke the same method the scheduler would.
 */
@Component
public class OutboxRelay {

    private final OutboxRelayTransactionalOps ops;

    public OutboxRelay(OutboxRelayTransactionalOps ops) {
        this.ops = ops;
    }

    @Scheduled(fixedDelay = 2000)
    public void poll() {
        ops.relayOnce();
    }
}
