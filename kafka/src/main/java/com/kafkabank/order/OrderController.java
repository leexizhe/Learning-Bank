package com.kafkabank.order;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Note this controller has no {@code GET /api/payments/{id}} — reading a
 * payment's outcome belongs to the reconciliation role, which owns that state.
 * Keeping the read on the other side of the fence is what stops this from
 * quietly becoming a monolith with a Kafka topic bolted on.
 */
@RestController
public class OrderController {

    private final PaymentInitiationService paymentInitiationService;

    public OrderController(PaymentInitiationService paymentInitiationService) {
        this.paymentInitiationService = paymentInitiationService;
    }

    /**
     * 202 Accepted, not 201 Created. Nothing has been debited when this returns —
     * the only thing that has happened is a durable append to Kafka. Returning 201
     * (or worse, a balance) would be lying about what the system has actually done,
     * and it's the single most common way people mis-model an async API.
     */
    @PostMapping("/api/payments")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public InitiatePaymentResponse initiate(@Valid @RequestBody InitiatePaymentRequest request) {
        return paymentInitiationService.initiate(request.accountId(), request.amountMinor(), request.description());
    }

    @ExceptionHandler(PaymentPublishException.class)
    public ProblemDetail handlePublishFailure(PaymentPublishException e) {
        // The broker wouldn't take the write, so we genuinely don't know that the
        // payment is safe. 503 tells the caller it's retryable, rather than pretending.
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }
}
