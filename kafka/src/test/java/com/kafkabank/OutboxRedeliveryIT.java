package com.kafkabank;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.PaymentStatus;
import com.kafkabank.common.Topics;
import com.kafkabank.payment.PaymentProcessingService;
import com.kafkabank.payment.entity.Account;
import com.kafkabank.payment.entity.PaymentOutbox;
import com.kafkabank.payment.repository.PaymentOutboxRepository;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The crash window, closed — the test this whole outbox exists for.
 *
 * <p><b>The scenario.</b> A payment is applied and its result committed to the outbox, then the
 * process dies before the send is confirmed. The offset was never acknowledged, so Kafka redelivers
 * the event. Under the old design that redelivery was actively harmful: the idempotency check would
 * correctly refuse to debit twice and, in doing so, guarantee the result could never be produced
 * again. Here it is a no-op and the result is recovered anyway, because recovering it was never the
 * consumer's job.
 *
 * <p><b>How the crash is simulated.</b> Not by killing the JVM, but by flipping {@code published}
 * back to false. That is exactly the state a crash between a successful send and the commit
 * recording it would leave behind, and exactly the state the relay exists to recover from.
 *
 * <p><b>Why this counts records rather than waiting for one.</b> The obvious shape — seek to the
 * end of the topic, then trigger the crash, then wait for a record — is a race against a relay that
 * publishes within about 400ms, and it loses intermittently. Counting matches from offset 0 is
 * monotonic instead: the result must appear <b>twice</b>, once from the original publish and once
 * from the recovery, and no amount of timing can make two records look like one. The payment id is
 * a fresh UUID, so records left on the topic by other test classes are invisible to the filter.
 *
 * <p><b>What was deliberately not done.</b> Standing up a second application context with the relay
 * disabled via {@code @SpringBootTest(properties = ...)} would be a trap here: the containers are
 * static and shared, so the second context's {@code @KafkaListener} joins the <em>same</em> {@code
 * payment-service} group against the same broker. Both contexts would hold partitions, and the
 * first — relay still running — could consume the test's event and publish it, quietly defeating
 * the assertion.
 *
 * <p><b>What this proves, precisely.</b> That the result survives the window and reaches the topic.
 * It does <em>not</em> distinguish which of the two relay routes recovered it — the after-commit
 * listener or the scheduled sweep — and pinning that down would need exactly the disabled-relay
 * context described above. Worth stating rather than implying.
 */
class OutboxRedeliveryIT extends BaseKafkaIT {

  @Autowired private PaymentProcessingService processing;

  @Autowired private PaymentOutboxRepository outbox;

  @Test
  @Timeout(120)
  void aResultLostBeforePublishIsRecoveredAndRedeliveryDoesNotDebitTwice() {
    Account account = newAccount(100_000);
    String eventId = UUID.randomUUID().toString();
    String paymentId = UUID.randomUUID().toString();

    // The payment is applied; the debit and the result commit together.
    processing.process(
        new PaymentInitiated(eventId, paymentId, account.getId(), 30_000, "crash-window"));
    assertThat(balanceOf(account.getId())).isEqualTo(70_000);

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(row(eventId).isPublished())
                    .as("the relay published it once, normally")
                    .isTrue());

    // THE CRASH: the row is pending again, as if the process had died between
    // a successful send and the commit that recorded it...
    markUnpublished(eventId);
    // ...and Kafka redelivers, because the offset was never acknowledged.
    sendInitiated(eventId, paymentId, account.getId(), 30_000);

    List<ConsumerRecord<String, String>> results =
        awaitRecordCount(
            Topics.PAYMENT_RESULTS, r -> r.value().contains(paymentId), 2, Duration.ofSeconds(60));

    assertThat(results)
        .as("published once normally, then recovered from the outbox after the crash")
        .hasSize(2);
    assertThat(results)
        .allSatisfy(
            record -> {
              assertThat(record.value())
                  .as("the recovered record is the stored result, not a freshly computed one")
                  .contains(PaymentStatus.ACCEPTED.name());
              assertThat(record.key())
                  .as("still keyed by account, so recovery cannot reorder anything")
                  .isEqualTo(String.valueOf(account.getId()));
            });
    assertThat(results.get(0).value())
        .as("byte-identical payloads - the relay replays, it does not regenerate")
        .isEqualTo(results.get(1).value());

    assertThat(balanceOf(account.getId()))
        .as("redelivery hit the idempotency check and did not debit a second time")
        .isEqualTo(70_000);
  }

  private PaymentOutbox row(String eventId) {
    return outbox.findBySourceEventId(eventId).orElseThrow();
  }

  private void markUnpublished(String eventId) {
    PaymentOutbox pending = row(eventId);
    pending.setPublished(false);
    outbox.saveAndFlush(pending);
  }
}
