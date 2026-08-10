package com.kafkabank.phase3_retries_dlt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.kafkabank.BaseKafkaIT;
import com.kafkabank.common.PaymentStatus;
import com.kafkabank.common.ReconciliationState;
import com.kafkabank.common.Topics;
import com.kafkabank.payment.entity.Account;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * The two ways a payment can fail to go through, and why the consumer must treat them completely differently. Both
 * blocks below end up looking at the same dead-letter topic — one asserting the record is <b>absent</b>, the other that
 * it is <b>present</b> — which is the sharpest way to state the distinction.
 *
 * <p><b>Rejected is an answer; failed is a fault.</b> Insufficient funds is a correct, final business outcome, so
 * retrying it would just decline it again forever and dead-lettering it would page a human about a system that is
 * working. An unknown account cannot be processed at all, so it must never be silently dropped.
 *
 * <p>This phase comes before {@link com.kafkabank.phase4_ordering.Phase4OrderingIT} on purpose: the retry topics
 * introduced here are exactly what that phase's second block shows breaking the ordering guarantee.
 */
class Phase3RetriesDltIT extends BaseKafkaIT {

    /**
     * The distinction that matters most for how you design a consumer: a <b>rejected</b> payment is not a <b>failed</b>
     * one.
     *
     * <p>Insufficient funds is a correct, final business answer. It must not be retried (retrying declines it again,
     * forever) and it must not be dead-lettered (nothing is broken and no human needs to look at it). It gets a
     * REJECTED result event, the offset is committed, and reconciliation records ROLLBACK. Compare with
     * {@link DeadLetterTests}, where the message genuinely can't be processed.
     */
    @Nested
    class RejectedNotFailedTests {

        @Test
        @Timeout(60)
        void anUnaffordablePaymentIsRejectedNotRetriedAndReconcilesAsRollback() {
            Account account = newAccount(1_000);

            String paymentId =
                    postPayment(account.getId(), 999_999, "too expensive").paymentId();

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var view = reconciliationFor(paymentId);
                assertThat(view.resultStatus()).isEqualTo(PaymentStatus.REJECTED);
                assertThat(view.state()).isEqualTo(ReconciliationState.ROLLBACK);
            });

            // The balance is untouched: rejected means nothing moved.
            assertThat(balanceOf(account.getId())).isEqualTo(1_000);

            // And critically, nothing was dead-lettered - this was an answer, not an error. Drained once, after the
            // outcome above is already settled, rather than inside a retry loop: by this point the decision has
            // demonstrably been made, so a single read is enough and there is nothing to wait for.
            assertThat(drainTopic(Topics.PAYMENT_EVENTS_DLT, Duration.ofSeconds(5)))
                    .noneMatch(
                            record -> record.value() != null && record.value().contains(paymentId));
        }
    }

    /**
     * What happens to a message that genuinely cannot be processed.
     *
     * <p>The rule that matters in banking: <b>never silently drop it.</b> A dead-lettered record is a payment some
     * customer believes they made. It goes to a dedicated topic, keeping the original topic/offset and the failure
     * reason as headers, so a human can triage it and a replay job can re-drive it once the underlying bug is fixed.
     *
     * <p>An unknown account extends {@code PermanentFailureException}, which {@code @RetryableTopic} excludes, so it
     * skips the retry topics entirely and dead-letters immediately — retrying a permanently-unknown account would just
     * burn the attempts to reach the same conclusion.
     */
    @Nested
    class DeadLetterTests {

        @Test
        @Timeout(90)
        void anUnprocessableEventLandsInTheDeadLetterTopicWithDiagnosticHeaders() {
            String paymentId = UUID.randomUUID().toString();
            Long missingAccountId = 999_999_999L;

            ConsumerRecord<String, String> dead = awaitRecordMatching(
                    Topics.PAYMENT_EVENTS_DLT,
                    () -> sendInitiated(paymentId, missingAccountId, 5_000),
                    record -> record.value() != null && record.value().contains(paymentId),
                    Duration.ofSeconds(60));

            // The headers are the whole point - without them a DLT is just a graveyard you can't do anything with.
            assertThat(header(dead, KafkaHeaders.ORIGINAL_TOPIC)).isEqualTo(Topics.PAYMENT_EVENTS);
            assertThat(header(dead, KafkaHeaders.EXCEPTION_MESSAGE)).contains(String.valueOf(missingAccountId));
        }

        private String header(ConsumerRecord<String, String> record, String name) {
            var header = record.headers().lastHeader(name);
            assertThat(header).as("expected header %s on the DLT record", name).isNotNull();
            return new String(header.value(), StandardCharsets.UTF_8);
        }
    }
}
