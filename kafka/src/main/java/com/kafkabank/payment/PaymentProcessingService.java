package com.kafkabank.payment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.PaymentResult;
import com.kafkabank.common.PaymentStatus;
import com.kafkabank.common.Topics;
import com.kafkabank.payment.entity.Account;
import com.kafkabank.payment.entity.PaymentOutbox;
import com.kafkabank.payment.entity.ProcessedEvent;
import com.kafkabank.payment.repository.AccountRepository;
import com.kafkabank.payment.repository.PaymentOutboxRepository;
import com.kafkabank.payment.repository.ProcessedEventRepository;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * All the work for one payment, in one transaction: the idempotency marker, the debit, and the outgoing result event.
 * Three writes, one commit, no window between them.
 *
 * <p><b>This used to return the result for the consumer to publish, and that was a bug.</b> The consumer took the
 * returned value, handed it to an asynchronous {@code KafkaTemplate.send}, discarded the future and committed the
 * offset on the next line. If the broker rejected the send, the offset was already committed and the result was gone
 * with no path to recovery.
 *
 * <p>The obvious fix — block on the send before acknowledging — narrows that window without closing it, and
 * understanding why is worth more than the fix itself. Blocking means a failed send throws, so {@code @RetryableTopic}
 * redelivers the event, so this method runs again, sees its own {@code processed_events} row and correctly refuses to
 * debit twice. Correct — and the result is now <em>permanently</em> unpublishable, because the only thing that could
 * have produced it was the run that already happened. The retry actively makes it worse.
 *
 * <p>Writing the result into the outbox resolves that, and note <em>how</em>: not by returning the previously computed
 * result on a duplicate, but by never needing to. The early return below is safe precisely because the first attempt's
 * outbox row is still there — published already, or waiting for the relay. Redelivery becomes genuinely a no-op instead
 * of a lossy one.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentProcessingService {

    private final AccountRepository accounts;
    private final ProcessedEventRepository processedEvents;
    private final PaymentOutboxRepository outbox;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher events;
    private final SimulatedTransientFailure simulatedFailure;

    /**
     * @throws UnknownAccountException when the message is unprocessable — triggers retry and eventually the DLT
     */
    @Transactional
    public void process(PaymentInitiated event) {
        // A no-op unless a test marked this event to fail transiently. Ahead of the idempotency check on purpose, so
        // the failed attempt writes nothing and the retry is a genuine reprocessing. See SimulatedTransientFailure for
        // why the fault has to live here rather than in a mock.
        simulatedFailure.failFirstDeliveriesOf(event);

        // The idempotency check. This read is only an optimization — it makes the common duplicate case cheap. The real
        // guarantee is the primary key on processed_events.event_id: two concurrent deliveries can both pass this read,
        // but only one can commit the insert; the loser's transaction fails and rolls back its debit with it.
        //
        // Returning early is lossless now: whatever this event produced the first time is sitting in payment_outbox,
        // and publishing it is the relay's job rather than this method's. See the class javadoc.
        if (processedEvents.existsById(event.eventId())) {
            log.info("Duplicate delivery of eventId={} - skipping, already applied", event.eventId());
            return;
        }

        Account account = accounts.findByIdForUpdate(event.accountId())
                .orElseThrow(() -> new UnknownAccountException(event.accountId()));

        // Marking the event processed happens in the SAME transaction as the debit below. If they were separate, a
        // crash between them would leave money moved with no record that it had been - and the redelivery would move it
        // again. It applies to the rejected path too: a decline is a final answer, so redelivering it must not re-run
        // the decision.
        processedEvents.save(new ProcessedEvent(event.eventId(), event.paymentId()));

        // Insufficient funds is a business ANSWER, not a failure. It gets a REJECTED result event, so it never retries
        // and never reaches the DLT - retrying it would just decline it again, forever.
        PaymentResult result;
        if (!account.canDebit(event.amountMinor())) {
            log.info("Rejecting payment {} - insufficient funds", event.paymentId());
            result = resultFor(event, PaymentStatus.REJECTED, "Insufficient funds", account);
        } else {
            account.debit(event.amountMinor());
            log.info(
                    "Accepted payment {} - debited {} from account {}",
                    event.paymentId(),
                    event.amountMinor(),
                    event.accountId());
            result = resultFor(event, PaymentStatus.ACCEPTED, null, account);
        }

        // The third write in this transaction, committing atomically with the debit and the processed_events row. That
        // atomicity is the entire point.
        PaymentOutbox row = outbox.save(new PaymentOutbox(
                event.eventId(), Topics.PAYMENT_RESULTS, String.valueOf(event.accountId()), toJson(result)));

        // Fired now, delivered only after this transaction commits. Purely a latency shortcut - the scheduled sweep is
        // what makes it correct.
        events.publishEvent(new PaymentResultRecorded(row.getId()));
    }

    /**
     * The accepted and rejected results differ only in status and reason; everything else is copied through.
     */
    private static PaymentResult resultFor(
            PaymentInitiated event, PaymentStatus status, String reason, Account account) {
        return new PaymentResult(
                UUID.randomUUID().toString(),
                event.paymentId(),
                event.accountId(),
                event.amountMinor(),
                status,
                reason,
                account.getBalanceMinor());
    }

    private String toJson(PaymentResult result) {
        try {
            return objectMapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            // If the result can't be serialized the outbox row can't be written, and this transaction must not commit -
            // the alternative is a debit with no corresponding event, which is the very thing being prevented.
            throw new IllegalStateException("Could not serialize result for payment " + result.paymentId(), e);
        }
    }
}
