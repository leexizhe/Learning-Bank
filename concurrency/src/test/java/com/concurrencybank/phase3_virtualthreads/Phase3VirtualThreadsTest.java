package com.concurrencybank.phase3_virtualthreads;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Three ways to fan out the same three checks, in one file so they can actually be compared. Each nested block runs the
 * identical three scenarios — same method names, same client timings, same assertions — against a different
 * {@code PaymentGatewayService} entry point: {@code validate()} (structured concurrency), {@code
 * validateWithExecutorService()} (the pre-preview fallback), and {@code validateWithCompletableFuture()}.
 *
 * <p>What differs between them is not the outcome but the mechanism, and the mechanism is invisible from the test —
 * read each block's javadoc for what the assertions can't show you.
 */
class Phase3VirtualThreadsTest {

    /** Timings shared by every fan-out scenario: 100 + 120 + 90 = 310ms sequentially, ~120ms concurrently. */
    private static PaymentGatewayService fanOutService() {
        return new PaymentGatewayService(
                new FraudCheckClient(Duration.ofMillis(100)),
                new CreditCheckClient(Duration.ofMillis(120)),
                new SanctionsCheckClient(Duration.ofMillis(90)));
    }

    /** All three checks equally fast, so the declining one is never in a race with the others. */
    private static PaymentGatewayService uniformlyFastService() {
        return new PaymentGatewayService(
                new FraudCheckClient(Duration.ofMillis(20)),
                new CreditCheckClient(Duration.ofMillis(20)),
                new SanctionsCheckClient(Duration.ofMillis(20)));
    }

    /** Fraud and credit are deliberately slow; sanctions fails fast, so cancellation is what keeps this under 2s. */
    private static PaymentGatewayService slowSiblingsService() {
        return new PaymentGatewayService(
                new FraudCheckClient(Duration.ofSeconds(2)),
                new CreditCheckClient(Duration.ofSeconds(2)),
                new SanctionsCheckClient(Duration.ofMillis(30)));
    }

    /**
     * {@code validate()} — the {@code StructuredTaskScope} path. Note this is the only one of the three with no
     * {@code shutdown()} call: the scope owns its threads and closes them on exit, which is the whole point.
     */
    @Nested
    class StructuredTaskScopeTests {

        @Test
        void fansOutSoWallClockIsRoughlyTheSlowestCheckNotTheSum() {
            PaymentGatewayService service = fanOutService();

            Instant start = Instant.now();
            GatewayDecision decision = service.validate("TX-1");
            Duration elapsed = Duration.between(start, Instant.now());

            assertThat(decision.approved()).isTrue();
            assertThat(decision.checks()).hasSize(3);
            assertThat(elapsed).isLessThan(Duration.ofMillis(250));
        }

        @Test
        void aDeclinedCheckDoesNotCancelTheOthersAndIsReportedInTheResult() {
            PaymentGatewayService service = uniformlyFastService();

            GatewayDecision decision = service.validate("SANCTIONED-TX-2");

            assertThat(decision.approved()).isFalse();
            assertThat(decision.checks())
                    .filteredOn(r -> r.checkName().equals("sanctions"))
                    .first()
                    .satisfies(r -> assertThat(r.approved()).isFalse());
        }

        @Test
        void aFailingCheckCancelsTheSlowerSiblingsInsteadOfWaitingForThem() {
            // If structured concurrency is cancelling correctly, validate() returns in well under 2s instead of
            // waiting for the slow siblings to finish.
            PaymentGatewayService service = slowSiblingsService();

            Instant start = Instant.now();
            assertThatThrownBy(() -> service.validate("SANCTIONS-ERROR-TX-3"))
                    .isInstanceOf(PaymentValidationException.class)
                    .hasCauseInstanceOf(SanctionsCheckException.class);
            Duration elapsed = Duration.between(start, Instant.now());

            assertThat(elapsed).isLessThan(Duration.ofMillis(500));
        }
    }

    /**
     * {@code validateWithExecutorService()} — the fallback for when {@code StructuredTaskScope}'s preview status is a
     * problem. Identical assertions to the block above; only the service call differs, which is exactly what shows you
     * what structured concurrency buys and what stays the same either way.
     */
    @Nested
    class ExecutorServiceTests {

        @Test
        void fansOutSoWallClockIsRoughlyTheSlowestCheckNotTheSum() {
            PaymentGatewayService service = fanOutService();

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
            PaymentGatewayService service = uniformlyFastService();

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
            // Here cancellation isn't automatic - it's the explicit future.cancel(true) calls in
            // validateWithExecutorService's catch block, and completion-order (not submission-order) result reading
            // via ExecutorCompletionService, that make this finish in well under 2s.
            PaymentGatewayService service = slowSiblingsService();

            Instant start = Instant.now();
            assertThatThrownBy(() -> service.validateWithExecutorService("SANCTIONS-ERROR-TX-3"))
                    .isInstanceOf(PaymentValidationException.class)
                    .hasCauseInstanceOf(SanctionsCheckException.class);
            Duration elapsed = Duration.between(start, Instant.now());

            assertThat(elapsed).isLessThan(Duration.ofMillis(500));
            service.shutdown();
        }
    }

    /**
     * {@code validateWithCompletableFuture()} — wires its own {@code whenComplete}-based cancellation, so all three
     * variants end up fail-fast in the same way even though what "cancelled" actually stops differs underneath. See
     * {@link CompletableFutureTests#aFailingCheckCancelsTheSlowerSiblingsInsteadOfWaitingForThem()} for the catch.
     */
    @Nested
    class CompletableFutureTests {

        @Test
        void fansOutSoWallClockIsRoughlyTheSlowestCheckNotTheSum() {
            PaymentGatewayService service = fanOutService();

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
            PaymentGatewayService service = uniformlyFastService();

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
            // Same assertion as the other two blocks, but for a weaker reason: the whenComplete callback cancels
            // fraud/credit's futures the moment sanctions fails, so this returns quickly - yet the two Thread.sleep(2s)
            // calls underneath keep running for the full 2 seconds regardless. "Cancelled" here only means the futures
            // report done early, unlike validateWithExecutorService's Future.cancel(true), which genuinely interrupts.
            PaymentGatewayService service = slowSiblingsService();

            Instant start = Instant.now();
            assertThatThrownBy(() -> service.validateWithCompletableFuture("SANCTIONS-ERROR-TX-3"))
                    .isInstanceOf(PaymentValidationException.class)
                    .hasCauseInstanceOf(SanctionsCheckException.class);
            Duration elapsed = Duration.between(start, Instant.now());

            assertThat(elapsed).isLessThan(Duration.ofMillis(500));
            service.shutdown();
        }
    }
}
