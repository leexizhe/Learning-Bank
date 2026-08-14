package com.postgresbank.phase3_coordination;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * The hand-driving surface for phase 3's two coordination primitives. Nothing here is covered by a test —
 * {@code Phase3CoordinationIT} exercises {@link RefundService} and {@link JobRunner} directly, because both primitives
 * are about what happens when two callers collide, and that is far easier to arrange in a test than over HTTP. These
 * endpoints exist so you can watch the behaviour yourself with two terminals.
 *
 * <p><b>{@code POST /api/refunds/{orderId}} returns a boolean, not an error.</b> A second concurrent refund for the
 * same order is refused by {@code pg_try_advisory_xact_lock} <em>immediately</em> rather than queueing, so
 * {@code refunded=false} means "someone else is already doing this", not "something went wrong". Fire it twice at once
 * for the same id and exactly one comes back true.
 *
 * <p><b>{@code POST /api/jobs/claim} answers {@code claimedJobId: null} on an empty queue rather than 404.</b> A worker
 * polling an empty queue is the normal case, not an exceptional one — making it a 404 would mean every idle worker
 * logs an error once a second. Seed a few jobs with {@code POST /api/jobs}, then claim from several terminals at once:
 * with {@code SKIP LOCKED} each caller gets a different id and nobody blocks.
 */
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
