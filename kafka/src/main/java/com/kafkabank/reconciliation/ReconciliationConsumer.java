package com.kafkabank.reconciliation;

import com.kafkabank.common.ConsumerGroups;
import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.PaymentResult;
import com.kafkabank.common.Topics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * <b>The consumer-group lesson, made concrete.</b> This listens to {@link Topics#PAYMENT_EVENTS} — the same topic
 * {@code PaymentConsumer} consumes — but under a different {@code groupId}.
 *
 * <p>That single difference is the whole model:
 *
 * <ul>
 *   <li>Consumers in the <b>same</b> group <em>split</em> the partitions between them. That's how you scale out: add
 *       instances of the payment service and each takes a share of the load, and Kafka guarantees no two of them ever
 *       process the same record — so a payment can't be debited twice by two instances.
 *   <li>Consumers in <b>different</b> groups each get their <em>own complete copy</em> of the stream, with independent
 *       offsets. That's how you fan out: payment, reconciliation, fraud, and notifications all read every event without
 *       knowing about each other, and adding a fifth consumer group later requires no change to the producer at all.
 * </ul>
 *
 * <p>That fan-out is the actual architectural payoff over a chain of synchronous HTTP calls, and it's worth being able
 * to say in one sentence.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReconciliationConsumer {

    private final ReconciliationService reconciliationService;

    /**
     * No {@code @RetryableTopic} here, deliberately. Reconciliation is derived, eventually-consistent bookkeeping — if
     * it lags or briefly fails, no money is wrong, so it doesn't need the payment path's retry-and-dead-letter
     * machinery. Choosing different reliability guarantees for different consumers of the same stream is the point, not
     * an oversight.
     */
    @KafkaListener(topics = Topics.PAYMENT_EVENTS, groupId = ConsumerGroups.RECONCILIATION)
    public void onPaymentInitiated(PaymentInitiated event, Acknowledgment ack) {
        reconciliationService.onInitiated(event);
        ack.acknowledge();
    }

    @KafkaListener(topics = Topics.PAYMENT_RESULTS, groupId = ConsumerGroups.RECONCILIATION)
    public void onPaymentResult(PaymentResult result, Acknowledgment ack) {
        reconciliationService.onResult(result);
        ack.acknowledge();
    }
}
