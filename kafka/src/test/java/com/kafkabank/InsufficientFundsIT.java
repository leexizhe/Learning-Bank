package com.kafkabank;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.kafkabank.common.PaymentStatus;
import com.kafkabank.common.ReconciliationState;
import com.kafkabank.common.Topics;
import com.kafkabank.payment.entity.Account;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The distinction that matters most for how you design a consumer: a
 * <b>rejected</b> payment is not a <b>failed</b> one.
 *
 * <p>Insufficient funds is a correct, final business answer. It must not be
 * retried (retrying declines it again, forever) and it must not be
 * dead-lettered (nothing is broken and no human needs to look at it). It gets a
 * REJECTED result event, the offset is committed, and reconciliation records
 * ROLLBACK. Compare with {@link DeadLetterIT}, where the message genuinely can't
 * be processed.
 */
class InsufficientFundsIT extends BaseKafkaIT {

    @Test
    @Timeout(60)
    void anUnaffordablePaymentIsRejectedNotRetriedAndReconcilesAsRollback() {
        Account account = newAccount(1_000);

        String paymentId = postPayment(account.getId(), 999_999, "too expensive").paymentId();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var view = reconciliationFor(paymentId);
            assertThat(view.resultStatus()).isEqualTo(PaymentStatus.REJECTED);
            assertThat(view.state()).isEqualTo(ReconciliationState.ROLLBACK);
        });

        // The balance is untouched: rejected means nothing moved.
        assertThat(balanceOf(account.getId())).isEqualTo(1_000);

        // And critically, nothing was dead-lettered - this was an answer, not an error.
        // Drained once, after the outcome above is already settled, rather than inside
        // a retry loop: by this point the decision has demonstrably been made, so a
        // single read is enough and there is nothing to wait for.
        assertThat(drainTopic(Topics.PAYMENT_EVENTS_DLT, Duration.ofSeconds(5)))
                .noneMatch(record -> record.value() != null && record.value().contains(paymentId));
    }
}
