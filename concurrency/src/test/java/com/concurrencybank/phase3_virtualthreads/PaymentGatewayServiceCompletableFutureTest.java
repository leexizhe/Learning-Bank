package com.concurrencybank.phase3_virtualthreads;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Tests {@code PaymentGatewayService.validateWithCompletableFuture()}. Same three method names, same setup, same
 * assertions as {@link PaymentGatewayServiceTest} and {@link PaymentGatewayServiceExecutorTest} — this version wires
 * its own {@code whenComplete}-based cancellation (see the method's javadoc), so all three variants are fail-fast the
 * same way, even though the cancellation mechanism, and what it actually stops, differs under the hood between this
 * method and {@code validateWithExecutorService}.
 */
class PaymentGatewayServiceCompletableFutureTest {

    @Test
    void fansOutSoWallClockIsRoughlyTheSlowestCheckNotTheSum() {
        PaymentGatewayService service = new PaymentGatewayService(
                new FraudCheckClient(Duration.ofMillis(100)),
                new CreditCheckClient(Duration.ofMillis(120)),
                new SanctionsCheckClient(Duration.ofMillis(90)));

        Instant start = Instant.now();
        GatewayDecision decision = service.validateWithCompletableFuture("TX-1");
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

        GatewayDecision decision = service.validateWithCompletableFuture("SANCTIONED-TX-2");

        assertThat(decision.approved()).isFalse();
        assertThat(decision.checks())
                .filteredOn(r -> r.checkName().equals("sanctions"))
                .first()
                .satisfies(r -> assertThat(r.approved()).isFalse());
        service.shutdown();
    }

    @Test
    void aFailingCheckCancelsTheSlowerSiblingsInsteadOfWaitingForThem() {
        // Same scenario, same assertion as the other two test files: this method returns quickly because the
        // whenComplete callback cancels fraud/credit's futures the moment sanctions fails. What's NOT visible from this
        // test - see the class/method javadoc instead -
        // is that "cancelled" here only means the futures report done early;
        // the two Thread.sleep(2s) calls underneath keep running for the full 2 seconds regardless, unlike
        // validateWithExecutorService's Future.cancel(true), which genuinely interrupts them.
        PaymentGatewayService service = new PaymentGatewayService(
                new FraudCheckClient(Duration.ofSeconds(2)),
                new CreditCheckClient(Duration.ofSeconds(2)),
                new SanctionsCheckClient(Duration.ofMillis(30)));

        Instant start = Instant.now();
        assertThatThrownBy(() -> service.validateWithCompletableFuture("SANCTIONS-ERROR-TX-3"))
                .isInstanceOf(PaymentValidationException.class)
                .hasCauseInstanceOf(SanctionsCheckException.class);
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(elapsed).isLessThan(Duration.ofMillis(500));
        service.shutdown();
    }
}
