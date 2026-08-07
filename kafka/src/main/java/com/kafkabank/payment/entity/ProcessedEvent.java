package com.kafkabank.payment.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import lombok.Getter;
import org.springframework.data.domain.Persistable;

/**
 * The idempotent-consumer ledger. One row per event we have already applied.
 *
 * <p>Kafka's default delivery guarantee is <b>at-least-once</b>: if a consumer commits its offset before finishing the
 * work it loses messages, and if it commits after (which is what we do) it can re-deliver a message the app had already
 * applied — crash in that window and the same debit is replayed.
 *
 * <p>You don't fix that by chasing exactly-once semantics; you fix it by making the consumer idempotent, so applying
 * the same event twice is harmless. The primary key on {@code eventId} is the actual enforcement — not the "have I seen
 * this?" read, which is only an optimization and is racy on its own.
 *
 * <p><b>Why it implements {@link Persistable}:</b> this entity has an <em>assigned</em> id rather than a generated one.
 * Spring Data decides between {@code persist()} and {@code merge()} by asking whether the id is null — so with an
 * assigned id it always concludes "this already exists" and calls {@code merge()}, which fires a SELECT to load the row
 * before inserting it. That's a wasted round trip on every single consumed message. Declaring {@code isNew()} as always
 * true says what we actually know — we only ever insert here, never update — and gets a plain INSERT.
 */
@Getter
@Entity
@Table(name = "processed_events")
public class ProcessedEvent implements Persistable<String> {

    @Id
    @Column(name = "event_id", nullable = false, length = 64)
    private String eventId;

    @Column(name = "payment_id", nullable = false, length = 64)
    private String paymentId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEvent() {}

    public ProcessedEvent(String eventId, String paymentId) {
        this.eventId = eventId;
        this.paymentId = paymentId;
        this.processedAt = Instant.now();
    }

    @Override
    public String getId() {
        return eventId;
    }

    /** Rows here are only ever inserted, never updated — see the class javadoc. */
    @Override
    @Transient
    public boolean isNew() {
        return true;
    }
}
