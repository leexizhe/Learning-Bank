package com.postgresbank.common;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;

/**
 * Written in the <em>same transaction</em> as the ledger postings it describes (see
 * phase2_ledger.TransferTransactionalOps) - that's the entire transactional-outbox pattern in one
 * sentence. A separate relay (phase3_coordination.OutboxRelay) polls {@code published = false} rows
 * and "publishes" them; if the surrounding transaction rolls back, this row rolls back with it, so
 * there is never a committed event with no corresponding ledger entry, or vice versa.
 */
@Getter
@Entity
@Table(name = "outbox")
public class Outbox {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "event_id", nullable = false, unique = true)
  private UUID eventId;

  @Column(nullable = false)
  private String payload;

  @Setter
  @Column(nullable = false)
  private boolean published;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  protected Outbox() {}

  public Outbox(String payload) {
    this.eventId = UUID.randomUUID();
    this.payload = payload;
    this.published = false;
    this.createdAt = Instant.now();
  }
}
