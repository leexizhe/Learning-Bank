package com.concurrencybank.phase3_virtualthreads;

import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GatewayController {

    private final PaymentGatewayService gatewayService;

    public GatewayController(PaymentGatewayService gatewayService) {
        this.gatewayService = gatewayService;
    }

    @PostMapping("/api/gateway/validate")
    public GatewayDecision validate(@RequestParam String transactionId) {
        return gatewayService.validate(transactionId);
    }

    /**
     * Same fan-out, done the pre-StructuredTaskScope way — see {@code
     * PaymentGatewayService.validateWithExecutorService}.
     */
    @PostMapping("/api/gateway/validate-legacy")
    public GatewayDecision validateLegacy(@RequestParam String transactionId) {
        return gatewayService.validateWithExecutorService(transactionId);
    }

    /**
     * Same fan-out again, via CompletableFuture — see {@code PaymentGatewayService.validateWithCompletableFuture}.
     */
    @PostMapping("/api/gateway/validate-completable-future")
    public GatewayDecision validateCompletableFuture(@RequestParam String transactionId) {
        return gatewayService.validateWithCompletableFuture(transactionId);
    }

    @ExceptionHandler(PaymentValidationException.class)
    public ProblemDetail handleValidationFailure(PaymentValidationException e) {
        return ProblemDetail.forStatusAndDetail(org.springframework.http.HttpStatus.BAD_GATEWAY, e.getMessage());
    }
}
