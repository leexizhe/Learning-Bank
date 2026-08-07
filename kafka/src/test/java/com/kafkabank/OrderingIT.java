package com.kafkabank;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.PaymentStatus;
import com.kafkabank.common.ReconciliationState;
import com.kafkabank.common.Topics;
import com.kafkabank.payment.entity.Account;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Proves the per-account ordering guarantee with an outcome that is only possible if the two events were applied in the
 * order they were produced.
 *
 * <p>The trick is picking amounts where order changes the answer. Balances are additive, so "debit 10 then debit 20"
 * ends at the same number either way — that would prove nothing. Instead the account holds exactly enough for the first
 * payment and not the second, so:
 *
 * <ul>
 *   <li>in order → big one ACCEPTED (balance 0), small one REJECTED
 *   <li>out of order → small one ACCEPTED (balance 40,000), big one REJECTED
 * </ul>
 *
 * The final balance alone distinguishes the two. This works because both events share an {@code accountId}, so they
 * share a partition, so one consumer thread handles them in offset order — remove the shared key and this test starts
 * failing intermittently.
 */
class OrderingIT extends BaseKafkaIT {

    @Test
    @Timeout(60)
    void eventsForOneAccountAreAppliedInProducedOrder() throws ExecutionException, InterruptedException {
        Account account = newAccount(100_000);
        String key = String.valueOf(account.getId());

        String firstPaymentId = UUID.randomUUID().toString();
        String secondPaymentId = UUID.randomUUID().toString();

        // Sent via kafkaTemplate directly (rather than the base class helper) so each send can be awaited: blocking on
        // the first makes "produced order" unambiguous before the second is even issued, otherwise this would be
        // asserting on the consumer while the producer's own ordering was still open to question.
        kafkaTemplate
                .send(
                        Topics.PAYMENT_EVENTS,
                        key,
                        new PaymentInitiated(
                                UUID.randomUUID().toString(), firstPaymentId, account.getId(), 100_000, "drains it"))
                .get();
        kafkaTemplate
                .send(
                        Topics.PAYMENT_EVENTS,
                        key,
                        new PaymentInitiated(
                                UUID.randomUUID().toString(), secondPaymentId, account.getId(), 60_000, "too late"))
                .get();

        // 0, not 40,000 - so the 100,000 debit definitely went first.
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(balanceOf(account.getId())).isZero());

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(reconciliationFor(firstPaymentId).resultStatus()).isEqualTo(PaymentStatus.ACCEPTED);
            assertThat(reconciliationFor(firstPaymentId).state()).isEqualTo(ReconciliationState.CONFIRMED);
            assertThat(reconciliationFor(secondPaymentId).resultStatus()).isEqualTo(PaymentStatus.REJECTED);
            assertThat(reconciliationFor(secondPaymentId).state()).isEqualTo(ReconciliationState.ROLLBACK);
        });
    }
}
