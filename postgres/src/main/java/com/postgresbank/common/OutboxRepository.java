package com.postgresbank.common;

import java.util.List;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxRepository extends JpaRepository<Outbox, Long> {

    List<Outbox> findByPublishedFalse();

    /**
     * The relay's claim query: bounded, and ordered by the monotonic id so events are published in the order they were
     * produced. Backed by the partial index on {@code (id) WHERE NOT published}, which stays proportional to the
     * backlog rather than to the table.
     */
    List<Outbox> findByPublishedFalseOrderById(Limit limit);

    long countByPayloadContaining(String fragment);
}
