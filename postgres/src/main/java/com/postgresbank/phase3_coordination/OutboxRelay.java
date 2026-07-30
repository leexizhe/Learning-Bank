package com.postgresbank.phase3_coordination;

import com.postgresbank.common.Outbox;
import com.postgresbank.common.OutboxRepository;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The other half of the transactional-outbox pattern: a relay that polls
 * unpublished rows and hands them to whatever the real downstream is (a
 * message broker, in production - see kafka-bank for that half). Logging
 * here stands in for the publish call; the point of this project is the
 * write-side atomicity guarantee (phase2_ledger.TransferTransactionalOps),
 * not a broker integration.
 *
 * <p>{@link #relayOnce()} is called both by the scheduled trigger and
 * directly by OutboxIT - tests don't wait out a real scheduling interval,
 * they just invoke the same method the scheduler would have called.
 */
@Slf4j
@Component
public class OutboxRelay {

    private final OutboxRepository outbox;

    public OutboxRelay(OutboxRepository outbox) {
        this.outbox = outbox;
    }

    @Scheduled(fixedDelay = 2000)
    public void poll() {
        relayOnce();
    }

    @Transactional
    public int relayOnce() {
        List<Outbox> pending = outbox.findByPublishedFalse();
        for (Outbox event : pending) {
            log.info("publishing outbox event {}: {}", event.getEventId(), event.getPayload());
            event.setPublished(true);
        }
        return pending.size();
    }
}
