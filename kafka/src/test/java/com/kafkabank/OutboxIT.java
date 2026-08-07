package com.kafkabank;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.PaymentStatus;
import com.kafkabank.payment.PaymentProcessingService;
import com.kafkabank.payment.UnknownAccountException;
import com.kafkabank.payment.entity.Account;
import com.kafkabank.payment.entity.PaymentOutbox;
import com.kafkabank.payment.repository.PaymentOutboxRepository;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The atomicity half of the outbox story: the result event is written in the same transaction as the debit, so the two
 * either both happen or neither does.
 *
 * <p>These call {@code PaymentProcessingService} directly rather than going through Kafka, because the property under
 * test is a <em>transaction boundary</em>, and a broker round trip would only add asynchrony between the test and the
 * thing it is asserting about. {@code OutboxRedeliveryIT} covers the end-to-end path.
 */
class OutboxIT extends BaseKafkaIT {

    @Autowired
    private PaymentProcessingService processing;

    @Autowired
    private PaymentOutboxRepository outbox;

    @Test
    @Timeout(60)
    void aProcessedPaymentLeavesExactlyOneOutboxRowKeyedByTheSourceEvent() {
        Account account = newAccount(100_000);
        String eventId = UUID.randomUUID().toString();

        processing.process(initiated(eventId, account, 40_000));

        PaymentOutbox row = outbox.findBySourceEventId(eventId).orElseThrow();
        assertThat(row.getMessageKey())
                .as("keyed by accountId, so routing through the outbox doesn't change the partition")
                .isEqualTo(String.valueOf(account.getId()));
        assertThat(row.getPayload())
                .as("the result is stored as JSON, ready to be sent verbatim")
                .contains(PaymentStatus.ACCEPTED.name());
        assertThat(balanceOf(account.getId())).isEqualTo(60_000);
    }

    /**
     * The atomicity proof. If the outbox write sat outside the transaction — a separate save, or worse a publish — this
     * row would survive a failure that rolled the debit back, and the system would announce a payment that never
     * happened. The phantom-event half of the dual-write problem.
     */
    @Test
    @Timeout(60)
    void aFailedPaymentLeavesNoOutboxRowBehind() {
        String eventId = UUID.randomUUID().toString();
        PaymentInitiated doomed =
                new PaymentInitiated(eventId, UUID.randomUUID().toString(), 999_999_999L, 1_000, "no such account");

        assertThatThrownBy(() -> processing.process(doomed)).isInstanceOf(UnknownAccountException.class);

        assertThat(outbox.findBySourceEventId(eventId))
                .as("the transaction rolled back, taking the outbox row with it")
                .isEmpty();
    }

    /**
     * The relay drains what the previous tests wrote. Asserted as "eventually nothing of ours is left pending" rather
     * than "the table is empty": the container is shared across test classes, so another class's row could be in flight
     * at any moment.
     */
    @Test
    @Timeout(60)
    void theRelayPublishesPendingRowsAndMarksThemDone() {
        Account account = newAccount(100_000);
        String eventId = UUID.randomUUID().toString();

        processing.process(initiated(eventId, account, 10_000));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Optional<PaymentOutbox> row = outbox.findBySourceEventId(eventId);
            assertThat(row).isPresent();
            assertThat(row.get().isPublished())
                    .as("the relay sent it and flipped the flag inside one transaction")
                    .isTrue();
        });
    }

    private static PaymentInitiated initiated(String eventId, Account account, long amountMinor) {
        return new PaymentInitiated(eventId, UUID.randomUUID().toString(), account.getId(), amountMinor, "outbox-it");
    }
}
