package com.postgresbank.common;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxRepository extends JpaRepository<Outbox, Long> {

    List<Outbox> findByPublishedFalse();

    long countByPayloadContaining(String fragment);
}
