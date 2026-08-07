package com.kafkabank.payment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkabank.common.PaymentResult;
import com.kafkabank.payment.entity.PaymentOutbox;
import com.kafkabank.payment.repository.PaymentOutboxRepository;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transactional half of the relay: claim unpublished rows, send them, mark them published — all inside one
 * transaction, so a row is only ever flipped after its send has actually been acknowledged.
 *
 * <p><b>Why this is a separate bean from {@link PaymentOutboxRelay}.</b> {@code @Transactional} is implemented by a
 * proxy, and a proxy can only intercept calls that arrive from <em>outside</em> the object. A scheduled method calling
 * an annotated method on {@code this} bypasses it entirely: no transaction is opened, the entities come back detached
 * from Spring Data's own short read-only transaction, and the flip to {@code published} is never flushed. The relay
 * would log "publishing…" forever and mark nothing — and the test suite would not notice, because a test calling the
 * method directly goes <em>through</em> the proxy and works fine. Splitting the scheduler from the transactional
 * operations is the same {@code *TransactionalOps} idiom the postgres module uses, applied where it is actually
 * load-bearing.
 *
 * <p><b>The send blocks, on purpose.</b> {@code KafkaTemplate.send} is asynchronous — it returns a future once the
 * record is in the producer's accumulator, not once the broker has it. Discarding that future and carrying on is the
 * exact bug this whole outbox exists to fix, so here the relay waits for the acknowledgement before flipping the row.
 * It runs on a background thread with nothing waiting on it, so blocking costs nothing.
 *
 * <p><b>This relay is at-least-once, and that is not a defect to hide.</b> If the process dies between a successful
 * send and the commit that flips {@code published}, the row is still pending and the record gets published twice. That
 * is fine here because the downstream is idempotent — {@code ReconciliationService} upserts on {@code paymentId} — but
 * the general point is worth saying out loud: <b>an outbox does not remove the need for idempotency, it moves it
 * downstream.</b> The alternative, flipping the row before sending, trades duplicates for lost messages, which for
 * money movement is the worse of the two.
 */
@Slf4j
@Component
public class PaymentOutboxRelayOps {

    /**
     * Bounded so a relay that has fallen behind catches up in batches rather than one doomed transaction.
     */
    private static final Limit BATCH = Limit.of(100);

    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final PaymentOutboxRepository outbox;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public PaymentOutboxRelayOps(
            PaymentOutboxRepository outbox, KafkaTemplate<String, Object> kafkaTemplate, ObjectMapper objectMapper) {
        this.outbox = outbox;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * {@code REQUIRES_NEW}, not the default {@code REQUIRED}, and that is not cosmetic. One caller is {@code
     * PaymentOutboxRelay}'s after-commit listener, which runs while the publishing transaction is still
     * <em>completing</em> — its resources are still bound to the thread, so {@code REQUIRED} joins a transaction that
     * can no longer be used and the claim query fails with "Query requires transaction be in progress, but no
     * transaction is known to be in progress". {@code REQUIRES_NEW} suspends whatever is finishing and opens a
     * genuinely new one.
     *
     * <p>Worth noticing how that bug hid: the poller retried 500ms later and succeeded, so the system stayed correct
     * and only a warning in the log said anything was wrong. Belt-and-braces designs mask the failure of one of their
     * braces.
     *
     * @return how many rows were published on this pass
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int relayOnce() {
        List<PaymentOutbox> pending = outbox.claimUnpublished(BATCH);
        for (PaymentOutbox row : pending) {
            publish(row);
            row.setPublished(true);
        }
        return pending.size();
    }

    /**
     * Deserializes back into a {@link PaymentResult} and sends it through the same {@link KafkaTemplate} the consumer
     * used to use, rather than shipping the stored JSON as a raw string. That keeps the wire format byte-identical —
     * {@code JsonSerializer} writes the same type headers it always did — so {@code ReconciliationConsumer} needs no
     * change at all. Sending the string would produce a subtly different record and surface as a mystifying
     * deserialization failure two services away.
     */
    private void publish(PaymentOutbox row) {
        try {
            PaymentResult result = objectMapper.readValue(row.getPayload(), PaymentResult.class);
            kafkaTemplate.send(row.getTopic(), row.getMessageKey(), result).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Relayed outbox row {} for eventId={}", row.getId(), row.getSourceEventId());
        } catch (JsonProcessingException e) {
            // Unparseable payload: retrying can't help, but neither can this thread. Rethrow so the transaction rolls
            // back and the row stays pending rather than being silently marked published.
            throw new IllegalStateException("Corrupt outbox payload in row " + row.getId(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted relaying outbox row " + row.getId(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Failed to relay outbox row " + row.getId(), e);
        }
    }
}
