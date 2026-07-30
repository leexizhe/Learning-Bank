package com.kafkabank.reconciliation;

import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.PaymentResult;
import com.kafkabank.reconciliation.entity.ReconciliationRecord;
import com.kafkabank.reconciliation.repository.ReconciliationRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The matching engine. Both halves of a payment's story arrive here from two
 * different topics, and this decides when they add up.
 *
 * <p>The interesting part is that <b>either half can arrive first</b>. The
 * initiated event is published before the result exists, so "initiated then
 * result" is the common case — but the two topics are consumed by independent
 * listener threads, so the result can absolutely win the race. Nothing here
 * assumes an order: each side fills in its own fields and then asks whether the
 * record has become complete.
 */
@Slf4j
@Service
public class ReconciliationService {

    private final ReconciliationRecordRepository records;

    public ReconciliationService(ReconciliationRecordRepository records) {
        this.records = records;
    }

    @Transactional
    public void onInitiated(PaymentInitiated event) {
        lockOrCreate(event.paymentId()).recordInitiated(event.accountId(), event.amountMinor());
        log.info("Reconciliation saw INITIATED for paymentId={}", event.paymentId());
    }

    @Transactional
    public void onResult(PaymentResult result) {
        lockOrCreate(result.paymentId()).recordResult(result.status());
        log.info("Reconciliation saw RESULT {} for paymentId={}", result.status(), result.paymentId());
    }

    /**
     * Returns the row for this payment, creating it first if nobody has yet, with a
     * write lock held for the rest of the transaction. Callers just mutate what they
     * get back — there's no explicit {@code save()} anywhere in this class, because
     * the returned entity is <em>managed</em>: Hibernate's dirty checking flushes any
     * change to it when the transaction commits.
     *
     * <p>Private and called only from the {@code @Transactional} methods above. The
     * transaction boundary has to be on the public entry points, because Spring's
     * proxy only intercepts calls arriving from outside the bean — annotating this
     * helper instead would silently do nothing at all.
     */
    private ReconciliationRecord lockOrCreate(String paymentId) {
        records.insertIfAbsent(paymentId);
        return records.findByIdForUpdate(paymentId)
                .orElseThrow(() -> new IllegalStateException("Row vanished after upsert: " + paymentId));
    }
}
