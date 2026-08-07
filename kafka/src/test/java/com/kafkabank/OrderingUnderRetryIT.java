package com.kafkabank;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.PaymentStatus;
import com.kafkabank.common.Topics;
import com.kafkabank.payment.SimulatedTransientFailure;
import com.kafkabank.payment.entity.Account;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * <b>This test asserts a limitation of this module's own design, and that is the point of it.</b>
 *
 * <p>The README claims per-account ordering (events keyed by {@code accountId}, proved by {@code OrderingIT}) and
 * non-blocking retries (via {@code @RetryableTopic}). Those two claims are in direct tension, and the tension only
 * shows up in the failure case — which is exactly where ordering mattered. When event #2 for an account fails, Spring
 * republishes it to {@code payment-events-retry-300} and moves on; #3 keeps flowing on the main topic and is applied
 * <b>first</b>. Per-key ordering is gone precisely when it counted.
 *
 * <p><b>There is no free fix</b>, only a choice:
 *
 * <ul>
 *   <li>accept it, and make the downstream commutative or idempotent — fine for a debit keyed on an event id, not fine
 *       for a state machine;
 *   <li>block and retry in place, accepting head-of-line blocking, within a {@code max.poll.interval.ms} budget;
 *   <li>{@code Consumer.pause()} the partition and retry with backoff, keeping order at the cost of throughput <em>for
 *       that partition only</em>.
 * </ul>
 *
 * <p><b>Why the assertion is on reconciliation status, not on balance or timing.</b> Balances are additive, so the
 * final number is the same whichever order the two payments applied in — the same trap {@code OrderingIT}'s javadoc
 * already warns about. Instead the account is funded to afford <em>exactly one</em> of the two equal payments, so which
 * one ends up ACCEPTED is a terminal state that only one ordering can produce. Nothing here waits on elapsed time or
 * observes an intermediate state.
 *
 * <p><b>The event fails twice rather than once, and that took a failing test to discover.</b> With a single failure the
 * retry lands on the 300ms tier — but when the listener throws, Spring publishes the record to the retry topic <em>on
 * the consumer thread</em>, stalling the main partition for roughly 450ms. The two are close enough that the retried
 * event beat the following one by 9ms on the first run: a coin flip dressed up as an assertion. Failing twice pushes it
 * to the 600ms tier, so the margin becomes about a second.
 *
 * <p>That stall is worth keeping in mind on its own: even "non-blocking" retries block the partition briefly while
 * handing the record off.
 */
class OrderingUnderRetryIT extends BaseKafkaIT {

    @Test
    @Timeout(90)
    void aRetriedEventIsAppliedAfterOneProducedLaterThanIt() throws ExecutionException, InterruptedException {
        // 10,000 for the first payment leaves 90,000 - enough for exactly one of the two 60,000 payments that follow,
        // which is what makes the outcome distinguish the two orderings.
        Account account = newAccount(100_000);
        String key = String.valueOf(account.getId());

        String harmless = UUID.randomUUID().toString();
        String failsTwice = UUID.randomUUID().toString();
        String producedLast = UUID.randomUUID().toString();

        send(key, harmless, account, 10_000, "warm-up");
        send(key, failsTwice, account, 60_000, SimulatedTransientFailure.MARKER + " diverted to the retry topics");
        send(key, producedLast, account, 60_000, "produced third, applied second");

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

    /** Blocking on each send so "produced order" is unambiguous before the next one is issued. */
    private void send(String key, String paymentId, Account account, long amountMinor, String description)
            throws ExecutionException, InterruptedException {
        kafkaTemplate
                .send(
                        Topics.PAYMENT_EVENTS,
                        key,
                        new PaymentInitiated(
                                UUID.randomUUID().toString(), paymentId, account.getId(), amountMinor, description))
                .get();
    }
}
