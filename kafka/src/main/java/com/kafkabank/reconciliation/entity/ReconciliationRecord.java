package com.kafkabank.reconciliation.entity;

import com.kafkabank.common.PaymentStatus;
import com.kafkabank.common.ReconciliationState;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;

/**
 * One row per payment, assembled from two different topics. Keyed by
 * {@code paymentId} — which is exactly why it can be assembled at all: the
 * initiated event and the result event carry the same paymentId, so they can be
 * matched even though they arrive separately, at different times, on different
 * topics, possibly in either order.
 */
@Getter
@Entity
@Table(name = "reconciliation_records")
public class ReconciliationRecord {

    @Id
    @Column(name = "payment_id", nullable = false, length = 64)
    private String paymentId;

    @Column(name = "account_id")
    private Long accountId;

    @Column(name = "amount_minor")
    private Long amountMinor;

    /** True once the payment-events side has been seen. */
    @Column(name = "initiated_seen", nullable = false)
    private boolean initiatedSeen;

    /** Null until the payment-results side has been seen. */
    @Enumerated(EnumType.STRING)
    @Column(name = "result_status", length = 32)
    private PaymentStatus resultStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false, length = 32)
    private ReconciliationState state;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected ReconciliationRecord() {}

    public ReconciliationRecord(String paymentId) {
        this.paymentId = paymentId;
        this.state = ReconciliationState.PENDING;
        this.updatedAt = Instant.now();
    }

    public void recordInitiated(Long accountId, long amountMinor) {
        this.accountId = accountId;
        this.amountMinor = amountMinor;
        this.initiatedSeen = true;
        recomputeState();
    }

    public void recordResult(PaymentStatus status) {
        this.resultStatus = status;
        recomputeState();
    }

    /**
     * Only reaches a terminal state once BOTH halves have arrived. Until then it
     * stays PENDING — which is the honest answer, and the thing a reconciliation
     * dashboard actually alerts on: a payment stuck PENDING past some threshold
     * means one of the two sides never showed up.
     */
    private void recomputeState() {
        if (initiatedSeen && resultStatus != null) {
            this.state = resultStatus == PaymentStatus.ACCEPTED
                    ? ReconciliationState.CONFIRMED
                    : ReconciliationState.ROLLBACK;
        } else {
            this.state = ReconciliationState.PENDING;
        }
        this.updatedAt = Instant.now();
    }
}
