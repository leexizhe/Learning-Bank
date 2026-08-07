package com.concurrencybank.phase3_virtualthreads;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Tests {@code PaymentGatewayService.validate()} (the {@code StructuredTaskScope} path). {@link
 * PaymentGatewayServiceExecutorTest} runs the identical three scenarios against {@code validateWithExecutorService()}
 * (the {@code ExecutorService} fallback) — same method names, same assertions, only the service call differs, so the
 * two files can be read side by side.
 */
class PaymentGatewayServiceTest {

    @Test
    void fansOutSoWallClockIsRoughlyTheSlowestCheckNotTheSum() {
        // 100 + 120 + 90 = 310ms run sequentially; concurrently it should take roughly max(100,120,90) = 120ms.
        PaymentGatewayService service = new PaymentGatewayService(
                new FraudCheckClient(Duration.ofMillis(100)),
                new CreditCheckClient(Duration.ofMillis(120)),
                new SanctionsCheckClient(Duration.ofMillis(90)));

        Instant start = Instant.now();
        GatewayDecision decision = service.validate("TX-1");
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(decision.approved()).isTrue();
        assertThat(decision.checks()).hasSize(3);
        assertThat(elapsed).isLessThan(Duration.ofMillis(250));
    }

    @Test
    void aDeclinedCheckDoesNotCancelTheOthersAndIsReportedInTheResult() {
        PaymentGatewayService service = new PaymentGatewayService(
                new FraudCheckClient(Duration.ofMillis(20)),
                new CreditCheckClient(Duration.ofMillis(20)),
                new SanctionsCheckClient(Duration.ofMillis(20)));

        GatewayDecision decision = service.validate("SANCTIONED-TX-2");

        assertThat(decision.approved()).isFalse();
        assertThat(decision.checks())
                .filteredOn(r -> r.checkName().equals("sanctions"))
                .first()
                .satisfies(r -> assertThat(r.approved()).isFalse());
    }

    @Test
    void aFailingCheckCancelsTheSlowerSiblingsInsteadOfWaitingForThem() {
        // Fraud/credit are deliberately slow (2s); sanctions fails fast (30ms). If structured concurrency is cancelling
        // correctly, validate() returns in well under 2s instead of waiting for the slow siblings to finish.
        PaymentGatewayService service = new PaymentGatewayService(
                new FraudCheckClient(Duration.ofSeconds(2)),
                new CreditCheckClient(Duration.ofSeconds(2)),
                new SanctionsCheckClient(Duration.ofMillis(30)));

        Instant start = Instant.now();
        assertThatThrownBy(() -> service.validate("SANCTIONS-ERROR-TX-3"))
                .isInstanceOf(PaymentValidationException.class)
                .hasCauseInstanceOf(SanctionsCheckException.class);
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(elapsed).isLessThan(Duration.ofMillis(500));
    }
}
