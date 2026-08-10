package com.postgresbank.phase3_coordination;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class CoordinationController {

    private final RefundService refunds;
    private final JobRunner jobs;
    private final PaymentJobRepository jobRepository;

    @PostMapping("/api/refunds/{orderId}")
    public RefundResponse refund(@PathVariable long orderId) {
        return new RefundResponse(orderId, refunds.tryRefund(orderId));
    }

    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping("/api/jobs")
    public PaymentJob seedJob(@RequestBody SeedJobRequest request) {
        return jobRepository.save(new PaymentJob(request.payload()));
    }

    @PostMapping("/api/jobs/claim")
    public ClaimResponse claim() {
        return new ClaimResponse(jobs.claimNext().orElse(null));
    }

    public record SeedJobRequest(String payload) {}

    public record RefundResponse(long orderId, boolean refunded) {}

    public record ClaimResponse(Long claimedJobId) {}
}
