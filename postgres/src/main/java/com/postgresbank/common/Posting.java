package com.postgresbank.common;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.Setter;

/**
 * One row per ledger movement. Never UPDATEd for balance purposes - a transfer inserts a debit and a credit row and
 * nothing is ever mutated afterward, which is what makes {@link LedgerService#balanceOf} safe to compute as a plain
 * {@code SUM}. {@code note} is deliberately not indexed: phase2_ledger's {@code Phase2LedgerIT.HotUpdateTests} updates
 * it repeatedly to demonstrate HOT (Heap-Only Tuple) updates, which only apply to non-indexed columns.
 */
@Getter
@Entity
@Table(name = "postings")
public class Posting {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "account_id", nullable = false)
    private Account account;

    @Column(name = "transfer_id")
    private Long transferId;

    @Column(name = "amount_minor", nullable = false)
    private long amountMinor;

    @Setter
    @Column(name = "note")
    private String note;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Posting() {}

    public Posting(Account account, Long transferId, long amountMinor, String note) {
        this.account = account;
        this.transferId = transferId;
        this.amountMinor = amountMinor;
        this.note = note;
        this.createdAt = Instant.now();
    }
}
