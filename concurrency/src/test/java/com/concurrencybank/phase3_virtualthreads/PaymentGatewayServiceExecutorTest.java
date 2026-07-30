package com.concurrencybank.phase3_virtualthreads;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Tests {@code PaymentGatewayService.validateWithExecutorService()} (the
 * {@code ExecutorService} fallback for when {@code StructuredTaskScope}'s
 * preview status is a problem). Same three scenarios, same method names, and
 * the same assertions as {@link PaymentGatewayServiceTest} — only the service
 * call differs — so the two files can be read side by side to see exactly
 * what structured concurrency buys you and what stays the same either way.
 */
class PaymentGatewayServiceExecutorTest {

    @Test
    void fansOutSoWallClockIsRoughlyTheSlowestCheckNotTheSum() {
        // 100 + 120 + 90 = 310ms run sequentially; concurrently it should take
        // roughly max(100,120,90) = 120ms. Same result as the StructuredTaskScope
        // version - fan-out was never the hard part.
        PaymentGatewayService service = new PaymentGatewayService(
                new FraudCheckClient(Duration.ofMillis(100)),
                new CreditCheckClient(Duration.ofMillis(120)),
                new SanctionsCheckClient(Duration.ofMillis(90)));

        Instant start = Instant.now();
        GatewayDecision decision = service.validateWithExecutorService("TX-1");
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(decision.approved()).isTrue();
        assertThat(decision.checks()).hasSize(3);
        assertThat(elapsed).isLessThan(Duration.ofMillis(250));
        service.shutdown();
    }

    @Test
    void aDeclinedCheckDoesNotCancelTheOthersAndIsReportedInTheResult() {
        PaymentGatewayService service = new PaymentGatewayService(
                new FraudCheckClient(Duration.ofMillis(20)),
                new CreditCheckClient(Duration.ofMillis(20)),
                new SanctionsCheckClient(Duration.ofMillis(20)));

        GatewayDecision decision = service.validateWithExecutorService("SANCTIONED-TX-2");

        assertThat(decision.approved()).isFalse();
        assertThat(decision.checks())
                .filteredOn(r -> r.checkName().equals("sanctions"))
                .first()
                .satisfies(r -> assertThat(r.approved()).isFalse());
        service.shutdown();
    }

    @Test
    void aFailingCheckCancelsTheSlowerSiblingsInsteadOfWaitingForThem() {
        // Fraud/credit are deliberately slow (2s); sanctions fails fast (30ms).
        // Here cancellation isn't automatic - it's the explicit
        // future.cancel(true) calls in validateWithExecutorService's catch
        // block, and completion-order (not submission-order) result reading
        // via ExecutorCompletionService, that make this finish in well under
        // 2s instead of waiting for the slow siblings to finish.
        PaymentGatewayService service = new PaymentGatewayService(
                new FraudCheckClient(Duration.ofSeconds(2)),
                new CreditCheckClient(Duration.ofSeconds(2)),
                new SanctionsCheckClient(Duration.ofMillis(30)));

        Instant start = Instant.now();
        assertThatThrownBy(() -> service.validateWithExecutorService("SANCTIONS-ERROR-TX-3"))
                .isInstanceOf(PaymentValidationException.class)
                .hasCauseInstanceOf(SanctionsCheckException.class);
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(elapsed).isLessThan(Duration.ofMillis(500));
        service.shutdown();
    }
}
