package com.kafkabank.phase4_ordering;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.kafkabank.BaseKafkaIT;
import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.PaymentStatus;
import com.kafkabank.common.ReconciliationState;
import com.kafkabank.common.Topics;
import com.kafkabank.payment.SimulatedTransientFailure;
import com.kafkabank.payment.entity.Account;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The per-account ordering guarantee, and the module's own retry mechanism breaking it. Read the two blocks in order:
 * the first establishes the guarantee, the second withdraws it in exactly the case where it mattered.
 *
 * <p>Both use the same trick, because it is the only honest way to assert on ordering: <b>pick amounts where the order
 * changes the answer.</b> Balances are additive, so "debit 10 then debit 20" ends at the same number either way and
 * proves nothing. Funding the account for exactly one of the two payments makes the terminal state — which payment was
 * ACCEPTED — something only one ordering can produce. Neither block waits on elapsed time or observes an intermediate
 * state.
 *
 * <p>Both also send through {@link #send} rather than the base class helper, so each publish can be awaited: blocking
 * on one before issuing the next makes "produced order" unambiguous, otherwise these would be asserting on the consumer
 * while the producer's own ordering was still open to question.
 */
class Phase4OrderingIT extends BaseKafkaIT {

    /**
     * Blocking on each send so "produced order" is unambiguous before the next one is issued. The record key is the
     * account id — that shared key is what puts these events on one partition, which is the only reason either block
     * below can say anything about ordering at all.
     */
    private void send(String paymentId, Account account, long amountMinor, String description)
            throws ExecutionException, InterruptedException {
        kafkaTemplate
                .send(
                        Topics.PAYMENT_EVENTS,
                        String.valueOf(account.getId()),
                        new PaymentInitiated(
                                UUID.randomUUID().toString(), paymentId, account.getId(), amountMinor, description))
                .get();
    }

    /**
     * Proves the per-account ordering guarantee with an outcome that is only possible if the two events were applied in
     * the order they were produced. The account holds exactly enough for the first payment and not the second, so:
     *
     * <ul>
     *   <li>in order → big one ACCEPTED (balance 0), small one REJECTED
     *   <li>out of order → small one ACCEPTED (balance 40,000), big one REJECTED
     * </ul>
     *
     * The final balance alone distinguishes the two. This works because both events share an {@code accountId}, so they
     * share a partition, so one consumer thread handles them in offset order — remove the shared key and this test
     * starts failing intermittently.
     */
    @Nested
    class InProducedOrderTests {

        @Test
        @Timeout(60)
        void eventsForOneAccountAreAppliedInProducedOrder() throws ExecutionException, InterruptedException {
            Account account = newAccount(100_000);

            String firstPaymentId = UUID.randomUUID().toString();
            String secondPaymentId = UUID.randomUUID().toString();

            send(firstPaymentId, account, 100_000, "drains it");
            send(secondPaymentId, account, 60_000, "too late");

            // 0, not 40,000 - so the 100,000 debit definitely went first.
            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertThat(balanceOf(account.getId())).isZero());

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var first = reconciliationFor(firstPaymentId);
                var second = reconciliationFor(secondPaymentId);
                assertThat(first.resultStatus()).isEqualTo(PaymentStatus.ACCEPTED);
                assertThat(first.state()).isEqualTo(ReconciliationState.CONFIRMED);
                assertThat(second.resultStatus()).isEqualTo(PaymentStatus.REJECTED);
                assertThat(second.state()).isEqualTo(ReconciliationState.ROLLBACK);
            });
        }
    }

    /**
     * <b>This block asserts a limitation of this module's own design, and that is the point of it.</b>
     *
     * <p>The README claims per-account ordering (events keyed by {@code accountId}, proved by the block above) and
     * non-blocking retries (via {@code @RetryableTopic}, the subject of
     * {@link com.kafkabank.phase3_retries_dlt.Phase3RetriesDltIT}). Those two claims are in direct tension, and the
     * tension only shows up in the failure case — which is exactly where ordering mattered. When event #2 for an
     * account fails, Spring republishes it to {@code payment-events-retry-300} and moves on; #3 keeps flowing on the
     * main topic and is applied <b>first</b>. Per-key ordering is gone precisely when it counted.
     *
     * <p><b>There is no free fix</b>, only a choice:
     *
     * <ul>
     *   <li>accept it, and make the downstream commutative or idempotent — fine for a debit keyed on an event id, not
     *       fine for a state machine;
     *   <li>block and retry in place, accepting head-of-line blocking, within a {@code max.poll.interval.ms} budget;
     *   <li>{@code Consumer.pause()} the partition and retry with backoff, keeping order at the cost of throughput
     *       <em>for that partition only</em>.
     * </ul>
     *
     * <p><b>The event fails twice rather than once, and that took a failing test to discover.</b> With a single failure
     * the retry lands on the 300ms tier — but when the listener throws, Spring publishes the record to the retry topic
     * <em>on the consumer thread</em>, stalling the main partition for roughly 450ms. The two are close enough that the
     * retried event beat the following one by 9ms on the first run: a coin flip dressed up as an assertion. Failing
     * twice pushes it to the 600ms tier, so the margin becomes about a second.
     *
     * <p>That stall is worth keeping in mind on its own: even "non-blocking" retries block the partition briefly while
     * handing the record off.
     */
    @Nested
    class UnderRetryTests {

        @Test
        @Timeout(90)
        void aRetriedEventIsAppliedAfterOneProducedLaterThanIt() throws ExecutionException, InterruptedException {
            // 10,000 for the first payment leaves 90,000 - enough for exactly one of the two 60,000 payments that
            // follow, which is what makes the outcome distinguish the two orderings.
            Account account = newAccount(100_000);

            String harmless = UUID.randomUUID().toString();
            String failsTwice = UUID.randomUUID().toString();
            String producedLast = UUID.randomUUID().toString();

            send(harmless, account, 10_000, "warm-up");
            send(failsTwice, account, 60_000, SimulatedTransientFailure.MARKER + " diverted to the retry topics");
            send(producedLast, account, 60_000, "produced third, applied second");

            await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                assertThat(reconciliationFor(harmless).resultStatus()).isEqualTo(PaymentStatus.ACCEPTED);
                assertThat(reconciliationFor(producedLast).resultStatus())
                        .as("produced LAST but applied while #2 sat on the retry topic, so it got the money")
                        .isEqualTo(PaymentStatus.ACCEPTED);
                assertThat(reconciliationFor(failsTwice).resultStatus())
                        .as("produced SECOND, retried after #3 had already spent the balance - ordering is gone")
                        .isEqualTo(PaymentStatus.REJECTED);
            });

            assertThat(balanceOf(account.getId()))
                    .as("100,000 - 10,000 - 60,000; the retried payment was declined, not lost")
                    .isEqualTo(30_000);
        }
    }
}
