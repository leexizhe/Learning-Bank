package com.kafkabank.payment;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Moves committed outbox rows to Kafka, by two routes.
 *
 * <p><b>The poller is the guarantee; the listener is the optimization.</b> Deliberately the same shape as this module's
 * other reliability claim — the constraint is the guarantee, the check is the optimization — because it is the same
 * idea. The {@code @Scheduled} sweep is what makes the outbox correct: it will find any committed row eventually, no
 * matter what crashed, restarted or was never notified. The after-commit listener exists only so the happy path doesn't
 * sit in the table for up to a poll interval before anyone looks at it. Delete the listener and the system is slower
 * but still correct; delete the poller and it is fast right up until the first crash.
 *
 * <p><b>{@code AFTER_COMMIT}, not {@code @EventListener}.</b> A plain listener runs inside the publishing transaction,
 * so it would try to send a record for a row that might still roll back — reintroducing the phantom-event half of the
 * dual-write problem from the other direction. {@code AFTER_COMMIT} runs only once the debit and the outbox row are
 * durably committed.
 *
 * <p><b>Why the listener swallows everything.</b> Spring propagates exceptions thrown from an after-commit
 * synchronization back to the caller — which here is the Kafka listener thread, <em>after</em> its database work has
 * already committed. Letting a failure out would fail the consumer, trigger {@code @RetryableTopic}, redeliver an event
 * that was already applied, and eventually dead-letter a payment that in fact succeeded. All for a publish the poller
 * is about to retry anyway. So this catches everything and leaves the row for the sweep: the optimization must never be
 * able to damage the thing it optimizes.
 */
@Slf4j
@Component
public class PaymentOutboxRelay {

    private final PaymentOutboxRelayOps ops;

    public PaymentOutboxRelay(PaymentOutboxRelayOps ops) {
        this.ops = ops;
    }

    /**
     * The durability sweep. Note this calls {@code ops.relayOnce()} on another bean rather than a local {@code
     * @Transactional} method — see {@link PaymentOutboxRelayOps} for why that distinction is the difference between a
     * working relay and one that silently does nothing.
     */
    @Scheduled(fixedDelayString = "${kafkabank.outbox.poll-interval-ms:500}")
    public void poll() {
        try {
            ops.relayOnce();
        } catch (RuntimeException e) {
            // A broker outage shouldn't fill the log with stack traces every 500ms; the rows stay pending and the next
            // sweep tries again.
            log.warn("Outbox sweep failed, will retry on the next poll: {}", e.getMessage());
        }
    }

    /** The latency shortcut. Best-effort by design — see the class javadoc. */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onPaymentResultRecorded(PaymentResultRecorded event) {
        try {
            ops.relayOnce();
        } catch (RuntimeException e) {
            log.warn(
                    "Immediate relay of outbox row {} failed; leaving it for the poller: {}",
                    event.outboxId(),
                    e.getMessage());
        }
    }
}
