package com.kafkabank.payment;

import com.kafkabank.common.PaymentInitiated;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Fails the first {@value #FAILURES} deliveries of any event whose description
 * starts with {@value #MARKER}, and lets every delivery after that through.
 *
 * <p><b>Why two failures and not one.</b> {@code @RetryableTopic} is configured
 * for 3 attempts with a 300ms backoff and a 2.0 multiplier, so a single failure
 * would put the event on {@code payment-events-retry-300} and it would come back
 * about 300ms later. That is not enough of a margin: when the listener throws,
 * Spring publishes the record to the retry topic <em>on the consumer thread</em>,
 * which blocks the main partition for roughly 450ms — comparable to the backoff
 * itself. Measured on this machine, the retried event beat the following one by
 * 9ms, i.e. a coin flip. Failing twice pushes the event to the 600ms tier, so it
 * returns around 900ms in while the main consumer resumes at 450ms, and the
 * ordering the test asserts is reached with room to spare rather than by luck.
 *
 * <p>That head-of-line stall during the republish is itself worth knowing: even
 * "non-blocking" retries block the partition briefly while the record is handed
 * to the retry topic.
 *
 * <p><b>Yes, this is a test affordance living in {@code main}, and that is
 * deliberate.</b> The fault has to survive a round trip through a real broker:
 * the listener must throw, Spring must republish the record to
 * {@code payment-events-retry-0}, and the retry must then succeed. A mock cannot
 * reach across that — and this module has no Mockito anywhere by design, because
 * everything it teaches only exists when a real broker is involved. The
 * concurrency module ships {@code SimulatedExternalCheck} for the same reason,
 * so simulation classes are in-house style rather than a compromise.
 *
 * <p>Keyed on {@code eventId} rather than a counter, so concurrent tests can't
 * consume each other's single allowed failure.
 *
 * <p>Consulted at the very top of {@link PaymentProcessingService#process}, ahead
 * of the idempotency check, so the first attempt fails having written nothing at
 * all — which is what makes the retry a genuine reprocessing rather than a
 * duplicate-delivery no-op.
 */
@Slf4j
@Component
public class SimulatedTransientFailure {

    public static final String MARKER = "fail-transiently:";

    /** One short of {@code @RetryableTopic}'s 3 attempts, so the final attempt always succeeds. */
    private static final int FAILURES = 2;

    private final Map<String, AtomicInteger> deliveries = new ConcurrentHashMap<>();

    /**
     * @throws IllegalStateException on the first {@value #FAILURES} deliveries of a
     *     marked event. Deliberately <em>not</em> a {@link PermanentFailureException}:
     *     that base type is on {@code @RetryableTopic}'s exclude list and would
     *     dead-letter immediately instead of retrying, which is the opposite of what
     *     this is for.
     */
    void failFirstDeliveriesOf(PaymentInitiated event) {
        String description = event.description();
        if (description == null || !description.startsWith(MARKER)) {
            return;
        }
        int delivery = deliveries
                .computeIfAbsent(event.eventId(), id -> new AtomicInteger())
                .incrementAndGet();
        if (delivery <= FAILURES) {
            log.info("Simulating a transient failure for eventId={} (delivery {})", event.eventId(), delivery);
            throw new IllegalStateException("simulated transient failure for " + event.eventId());
        }
        log.info("Letting eventId={} through on delivery {}", event.eventId(), delivery);
    }
}
