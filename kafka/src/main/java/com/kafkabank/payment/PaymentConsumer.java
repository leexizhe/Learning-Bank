package com.kafkabank.payment;

import com.kafkabank.common.ConsumerGroups;
import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.Topics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * The "Payment Service" of the three-service story: consumes {@link Topics#PAYMENT_EVENTS}, moves money, publishes the
 * outcome.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentConsumer {

    private final PaymentProcessingService paymentProcessingService;

    /**
     * <b>{@code @RetryableTopic} = non-blocking retries.</b> The naive way to retry is to sleep and loop inside the
     * listener — but the consumer holds its partition while it does that, so one bad record stalls every well-behaved
     * record queued behind it (head-of-line blocking), and a long enough stall trips {@code max.poll.interval.ms} and
     * gets the consumer kicked out of the group. Instead Spring republishes the failed record to a separate retry topic
     * ({@code payment-events-retry-0}, {@code -retry-1}) and moves on immediately. The delay happens over there, on the
     * retry topic's own consumer.
     *
     * <p><b>{@code exclude}</b> matters as much as the retry count. Retrying is only sensible for <em>transient</em>
     * failures (a database blip, a timeout). Retrying a deserialization failure or a permanently-unknown account just
     * burns the attempts and delays the inevitable, so those go straight to the DLT. Note it excludes the {@link
     * PermanentFailureException} <em>base type</em> rather than enumerating subclasses — the classification lives with
     * each exception, not in a list here that someone has to remember to update.
     *
     * <p><b>Manual ack.</b> {@code ack.acknowledge()} is the last statement, after the database transaction has
     * committed. Commit the offset any earlier and a crash in between means Kafka believes this payment was handled
     * when it wasn't — the message is gone and the money never moved.
     *
     * <p><b>What changed, and why the ack didn't have to move.</b> This method used to take the result back from {@code
     * process()} and hand it to an asynchronous {@code kafkaTemplate.send}, then acknowledge on the next line — which
     * committed the offset while the record was still sitting in the producer's accumulator. A rejected send meant the
     * result was lost with no redelivery and no DLT.
     *
     * <p>The tempting repair is to block on that send before acknowledging. It doesn't work, and {@link
     * PaymentProcessingService} explains why in full: the retry it triggers hits the idempotency check, which correctly
     * refuses to debit twice and thereby guarantees the result can never be produced again. The actual fix is that the
     * result is now written to the outbox <em>inside</em> {@code process()}'s transaction, so there is no longer
     * anything asynchronous between the commit and the ack for a crash to fall into. The {@code KafkaTemplate}
     * dependency is gone from this class entirely — publishing is {@link PaymentOutboxRelay}'s job.
     */
    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 300, multiplier = 2.0),
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            exclude = {PermanentFailureException.class})
    @KafkaListener(topics = Topics.PAYMENT_EVENTS, groupId = ConsumerGroups.PAYMENT)
    public void consume(
            PaymentInitiated event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info(
                "Consuming eventId={} paymentId={} from partition={} offset={}",
                event.eventId(),
                event.paymentId(),
                partition,
                offset);

        // 1. Everything for this payment in one transaction: the idempotency marker, the debit, and the result event
        //    written to the outbox table. There is no second system to write to here, so there is no dual write to get
        //    caught out by - which is the whole reason the outbox exists.
        paymentProcessingService.process(event);

        // 2. Only now is it safe to tell Kafka we're done with this offset. Nothing asynchronous happened between the
        //    commit above and this line, so there is no window in which the offset advances past work that didn't land.
        ack.acknowledge();
    }

    /**
     * Where records land once retries are exhausted. Never silently drop them: a dead-lettered record is a payment that
     * a customer believes they made, so it needs a human (or a replay job) to look at it. Spring preserves the original
     * topic/partition/offset and the exception message as headers on the DLT record, which is what makes triage and
     * replay-after-fix possible.
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
