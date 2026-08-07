package com.postgresbank.phase6_operations;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;

/**
 * "As of posting N, this account's balance was X." A cache over the journal, never a replacement for it.
 *
 * <p>Lives in {@code phase6_operations} rather than {@code common} because nothing outside this phase uses it — the
 * same reasoning that puts {@code PaymentJob} in {@code phase3_coordination}.
 *
 * <p><b>Why this one does not implement {@code Persistable}, unlike {@code kafka-bank}'s {@code ProcessedEvent}.</b>
 * Both have assigned primary keys, so Spring Data's null-id heuristic concludes "already exists" for both and issues a
 * SELECT before writing. For {@code ProcessedEvent} that round trip is pure waste, because those rows are only ever
 * inserted. Here it is exactly right: a snapshot is <em>re-taken</em> for the same account over and over, so the
 * operation genuinely is an upsert and {@code merge()} is the correct semantics. Same framework, same shape of key,
 * opposite conclusion — driven by whether the row is ever written twice.
 */
@Getter
@Entity
@Table(name = "account_balance_snapshots")
public class BalanceSnapshot {

    @Id
    @Column(name = "account_id")
    private Long accountId;

    /** The newest posting id included in {@link #balanceMinor}. Everything above it is the delta. */
    @Column(name = "as_of_posting_id", nullable = false)
    private long asOfPostingId;

    @Column(name = "balance_minor", nullable = false)
    private long balanceMinor;

    @Column(name = "taken_at", nullable = false)
    private Instant takenAt;

    protected BalanceSnapshot() {}

    public BalanceSnapshot(Long accountId, long asOfPostingId, long balanceMinor) {
        this.accountId = accountId;
        this.asOfPostingId = asOfPostingId;
        this.balanceMinor = balanceMinor;
        this.takenAt = Instant.now();
    }
}
