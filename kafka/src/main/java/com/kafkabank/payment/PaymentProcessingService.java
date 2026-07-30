package com.kafkabank.payment;

import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.PaymentResult;
import com.kafkabank.common.PaymentStatus;
import com.kafkabank.payment.entity.Account;
import com.kafkabank.payment.entity.ProcessedEvent;
import com.kafkabank.payment.repository.AccountRepository;
import com.kafkabank.payment.repository.ProcessedEventRepository;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * All the database work for one payment, in one transaction. Deliberately does
 * <b>not</b> publish anything to Kafka — see {@link PaymentConsumer} for why
 * publishing sits outside this boundary.
 */
@Slf4j
@Service
public class PaymentProcessingService {

    private final AccountRepository accounts;
    private final ProcessedEventRepository processedEvents;

    public PaymentProcessingService(AccountRepository accounts, ProcessedEventRepository processedEvents) {
        this.accounts = accounts;
        this.processedEvents = processedEvents;
    }

    /**
     * @return the result to publish, or empty when this event was already applied
     *         (a duplicate delivery) and must not be applied a second time.
     * @throws UnknownAccountException when the message is unprocessable — triggers
     *         retry and eventually the DLT
     */
    @Transactional
    public Optional<PaymentResult> process(PaymentInitiated event) {
        // The idempotency check. This read is only an optimization — it makes the
        // common duplicate case cheap. The real guarantee is the primary key on
        // processed_events.event_id: two concurrent deliveries can both pass this
        // read, but only one can commit the insert; the loser's transaction fails
        // and rolls back its debit with it.
        if (processedEvents.existsById(event.eventId())) {
            log.info("Duplicate delivery of eventId={} - skipping, already applied", event.eventId());
            return Optional.empty();
        }

        Account account = accounts.findByIdForUpdate(event.accountId())
                .orElseThrow(() -> new UnknownAccountException(event.accountId()));

        // Marking the event processed happens in the SAME transaction as the debit
        // below. If they were separate, a crash between them would leave money moved
        // with no record that it had been - and the redelivery would move it again.
        // It applies to the rejected path too: a decline is a final answer, so
        // redelivering it must not re-run the decision.
        processedEvents.save(new ProcessedEvent(event.eventId(), event.paymentId()));

        // Insufficient funds is a business ANSWER, not a failure. It gets a REJECTED
        // result event, so it never retries and never reaches the DLT - retrying it
        // would just decline it again, forever.
        if (!account.canDebit(event.amountMinor())) {
            log.info("Rejecting payment {} - insufficient funds", event.paymentId());
            return Optional.of(resultFor(event, PaymentStatus.REJECTED, "Insufficient funds", account));
        }

        account.debit(event.amountMinor());
        log.info(
                "Accepted payment {} - debited {} from account {}",
                event.paymentId(),
                event.amountMinor(),
                event.accountId());
        return Optional.of(resultFor(event, PaymentStatus.ACCEPTED, null, account));
    }

    /** The accepted and rejected results differ only in status and reason; everything else is copied through. */
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
}
