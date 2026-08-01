package com.kafkabank.payment.repository;

import com.kafkabank.payment.entity.PaymentOutbox;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

public interface PaymentOutboxRepository extends JpaRepository<PaymentOutbox, Long> {

    Optional<PaymentOutbox> findBySourceEventId(String sourceEventId);

    /**
     * The relay's claim query.
     *
     * <p>{@code ORDER BY id} rather than unordered: the id is monotonic, so this
     * publishes events in the order they were produced. An outbox relay that
     * reorders its own events would quietly undo the per-account ordering
     * guarantee the rest of this module is built on.
     *
     * <p>{@code PESSIMISTIC_WRITE} + {@code SKIP LOCKED} is the same job-queue
     * primitive the postgres module's {@code JobRunner} uses: two relay
     * instances (or the scheduled poller racing the after-commit listener) claim
     * disjoint rows instead of blocking on each other or double-publishing.
     * Without {@code SKIP LOCKED} the second claimant would simply wait for the
     * first, turning a parallel relay into a serial one.
     *
     * <p>{@link Limit} rather than an unbounded fetch: a relay that has fallen
     * behind should catch up in bounded batches, not try to load the entire
     * backlog into one transaction and time out forever at exactly the moment it
     * is most needed.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    // -2 is Hibernate's LockOptions.SKIP_LOCKED.
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("select o from PaymentOutbox o where o.published = false order by o.id")
    List<PaymentOutbox> claimUnpublished(Limit limit);

    long countByPublishedFalse();
}
