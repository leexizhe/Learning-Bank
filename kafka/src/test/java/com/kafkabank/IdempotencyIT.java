package com.kafkabank;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.kafkabank.payment.entity.Account;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The single most important reliability property in this project.
 *
 * <p>Kafka gives <b>at-least-once</b> delivery. A consumer that commits its offset
 * only after finishing the work (which is what {@code MANUAL_IMMEDIATE} buys us)
 * can still crash in the window between "database committed" and "offset
 * committed" — and then the broker redelivers a message the application has
 * already applied. For a payment, applying it twice means debiting a customer
 * twice.
 *
 * <p>These tests publish directly rather than through the REST API, because the
 * API generates a fresh {@code eventId} per call — and controlling the eventId is
 * the whole point: identical ids are a redelivery, distinct ids are two payments.
 */
class IdempotencyIT extends BaseKafkaIT {

    @Test
    @Timeout(60)
    void theSameEventDeliveredTwiceDebitsOnlyOnce() {
        Account account = newAccount(50_000);
        String eventId = UUID.randomUUID().toString();
        String paymentId = UUID.randomUUID().toString();

        // Same eventId, published twice - a redelivery, not a second payment.
        sendInitiated(eventId, paymentId, account.getId(), 20_000);
        sendInitiated(eventId, paymentId, account.getId(), 20_000);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(balanceOf(account.getId())).isEqualTo(30_000));

        // Hold still for a moment and confirm it STAYS at one debit. Without this the
        // test could pass simply by checking before the second copy was consumed.
        await().during(Duration.ofSeconds(3))
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(balanceOf(account.getId())).isEqualTo(30_000));
    }

    @Test
    @Timeout(60)
    void differentEventIdsForTheSameAccountBothApply() {
        Account account = newAccount(50_000);

        // Distinct eventIds are genuinely distinct payments, not duplicates - the
        // idempotency guard must not swallow these.
        sendInitiated(UUID.randomUUID().toString(), account.getId(), 10_000);
        sendInitiated(UUID.randomUUID().toString(), account.getId(), 15_000);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(balanceOf(account.getId())).isEqualTo(25_000));
    }
}
