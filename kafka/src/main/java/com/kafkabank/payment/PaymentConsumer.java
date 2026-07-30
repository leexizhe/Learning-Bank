package com.kafkabank.payment;

import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.Topics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * The "Payment Service" of the three-service story: consumes
 * {@link Topics#PAYMENT_EVENTS}, moves money, publishes the outcome.
 */
@Slf4j
@Component
public class PaymentConsumer {

    private final PaymentProcessingService paymentProcessingService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentConsumer(PaymentProcessingService paymentProcessingService, KafkaTemplate<String, Object> kafkaTemplate) {
        this.paymentProcessingService = paymentProcessingService;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * <b>{@code @RetryableTopic} = non-blocking retries.</b> The naive way to retry is
     * to sleep and loop inside the listener — but the consumer holds its partition
     * while it does that, so one bad record stalls every well-behaved record queued
     * behind it (head-of-line blocking), and a long enough stall trips
     * {@code max.poll.interval.ms} and gets the consumer kicked out of the group.
     * Instead Spring republishes the failed record to a separate retry topic
     * ({@code payment-events-retry-0}, {@code -retry-1}) and moves on immediately.
     * The delay happens over there, on the retry topic's own consumer.
     *
     * <p><b>{@code exclude}</b> matters as much as the retry count. Retrying is only
     * sensible for <em>transient</em> failures (a database blip, a timeout). Retrying a
     * deserialization failure or a permanently-unknown account just burns the attempts
     * and delays the inevitable, so those go straight to the DLT. Note it excludes the
     * {@link PermanentFailureException} <em>base type</em> rather than enumerating
     * subclasses — the classification lives with each exception, not in a list here
     * that someone has to remember to update.
     *
     * <p><b>Manual ack.</b> {@code ack.acknowledge()} is the last statement, after the
     * database transaction has committed and the result has been published. Commit the
     * offset any earlier and a crash in between means Kafka believes this payment was
     * handled when it wasn't — the message is gone and the money never moved.
     */
    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 300, multiplier = 2.0),
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            exclude = {PermanentFailureException.class})
    @KafkaListener(topics = Topics.PAYMENT_EVENTS, groupId = "payment-service")
    public void consume(
            PaymentInitiated event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("Consuming eventId={} paymentId={} from partition={} offset={}",
                event.eventId(), event.paymentId(), partition, offset);

        // 1. Database work, in its own transaction. Empty means "already applied".
        paymentProcessingService
                .process(event)
                // 2. Publish the outcome only after that transaction has committed.
                //
                //    This is the classic DUAL-WRITE problem and worth naming in an
                //    interview: the DB commit and the Kafka publish are two separate
                //    systems with no shared transaction. Crash between them and the money
                //    has moved but no result event exists. The offset isn't committed
                //    either, so the event is redelivered - and the idempotency check
                //    correctly refuses to debit twice, but it also doesn't republish the
                //    missing result.
                //
                //    The production answer is the TRANSACTIONAL OUTBOX pattern: write the
                //    outgoing event into an outbox table inside the same DB transaction as
                //    the debit, and let a separate relay (or Debezium CDC) publish from
                //    that table. One atomic write, no window. Left out here on purpose -
                //    it's a whole subsystem, and the point of this file is the consumer
                //    reliability story.
                .ifPresent(result -> kafkaTemplate.send(Topics.PAYMENT_RESULTS, String.valueOf(result.accountId()), result));

        // 3. Only now is it safe to tell Kafka we're done with this offset.
        ack.acknowledge();
    }

    /**
     * Where records land once retries are exhausted. Never silently drop them: a
     * dead-lettered record is a payment that a customer believes they made, so it
     * needs a human (or a replay job) to look at it. Spring preserves the original
     * topic/partition/offset and the exception message as headers on the DLT record,
     * which is what makes triage and replay-after-fix possible.
     */
    @DltHandler
    public void handleDlt(
            PaymentInitiated event,
            @Header(KafkaHeaders.ORIGINAL_TOPIC) String originalTopic,
            @Header(KafkaHeaders.ORIGINAL_OFFSET) byte[] originalOffset,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {
        log.error(
                "DEAD LETTER: paymentId={} eventId={} originalTopic={} reason={}",
                event.paymentId(),
                event.eventId(),
                originalTopic,
                exceptionMessage);
    }
}
