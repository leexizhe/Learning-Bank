package com.kafkabank.reconciliation;

import com.kafkabank.common.PaymentStatus;
import com.kafkabank.common.ReconciliationState;
import com.kafkabank.reconciliation.entity.ReconciliationRecord;
import com.kafkabank.reconciliation.repository.ReconciliationRecordRepository;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * The read side of a payment. Lives here rather than next to {@code POST
 * /api/payments} because the reconciliation role owns this state — the order
 * role published an event and forgot about it.
 */
@RestController
public class ReconciliationController {

    private final ReconciliationRecordRepository records;

    public ReconciliationController(ReconciliationRecordRepository records) {
        this.records = records;
    }

    public record ReconciliationView(
            String paymentId,
            Long accountId,
            Long amountMinor,
            boolean initiatedSeen,
            PaymentStatus resultStatus,
            ReconciliationState state) {}

    /**
     * A 404 here doesn't mean "no such payment" — it can equally mean "that payment
     * is real but neither of its events has been consumed yet". In an eventually
     * consistent read model, absence is a timing statement, not a truth claim.
     */
    @GetMapping("/api/payments/{paymentId}")
    public ReconciliationView getOne(@PathVariable String paymentId) {
        return records.findById(paymentId).map(ReconciliationController::toView).orElseThrow(
                () -> new ReconciliationNotFoundException(paymentId));
    }

    @GetMapping("/api/reconciliation")
    public List<ReconciliationView> listAll() {
        return records.findAll().stream().map(ReconciliationController::toView).toList();
    }

    private static ReconciliationView toView(ReconciliationRecord r) {
        return new ReconciliationView(
                r.getPaymentId(),
                r.getAccountId(),
                r.getAmountMinor(),
                r.isInitiatedSeen(),
                r.getResultStatus(),
                r.getState());
    }

    static class ReconciliationNotFoundException extends RuntimeException {
        ReconciliationNotFoundException(String paymentId) {
            super("No reconciliation record (yet) for payment " + paymentId);
        }
    }

    @ExceptionHandler(ReconciliationNotFoundException.class)
    public ProblemDetail handleNotFound(ReconciliationNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }
}
