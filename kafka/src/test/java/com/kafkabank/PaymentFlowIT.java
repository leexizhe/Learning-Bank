package com.kafkabank;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.kafkabank.common.ReconciliationState;
import com.kafkabank.order.InitiatePaymentRequest;
import com.kafkabank.order.InitiatePaymentResponse;
import com.kafkabank.payment.entity.Account;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * The end-to-end happy path across all three roles: HTTP → {@code payment-events} → debit → {@code payment-results} →
 * reconciliation CONFIRMED. Nothing here calls a service method directly; every step is driven by a real record moving
 * through a real broker.
 */
class PaymentFlowIT extends BaseKafkaIT {

    @Test
    @Timeout(60)
    void paymentFlowsFromHttpThroughKafkaToDebitAndConfirmedReconciliation() {
        Account account = newAccount(100_000);

        ResponseEntity<InitiatePaymentResponse> response = rest.postForEntity(
                "/api/payments",
                new InitiatePaymentRequest(account.getId(), 25_000, "rent"),
                InitiatePaymentResponse.class);

        // 202, not 201: at this instant nothing has been debited. All that has happened is a durable append to the
        // topic.
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().partition()).isNotNegative();
        String paymentId = response.getBody().paymentId();

        // The debit happens asynchronously, on the consumer's thread.
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(balanceOf(account.getId())).isEqualTo(75_000));

        // ...and the reconciliation role, in its own consumer group, independently assembles both halves and reaches a
        // terminal state.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var view = reconciliationFor(paymentId);
            assertThat(view.state()).isEqualTo(ReconciliationState.CONFIRMED);
            assertThat(view.initiatedSeen()).isTrue();
        });
    }

    @Test
    @Timeout(60)
    void everyEventForOneAccountGetsTheSamePartition() {
        Account account = newAccount(1_000_000);

        int first = postPayment(account.getId(), 100, "a").partition();
        int second = postPayment(account.getId(), 100, "b").partition();
        int third = postPayment(account.getId(), 100, "c").partition();

        // hash(key) % partitionCount is deterministic, so the same accountId always resolves to the same partition.
        // That is the entire mechanism behind the per-account ordering guarantee - there is nothing else enforcing it.
        assertThat(first).isEqualTo(second).isEqualTo(third);
    }
}
