package com.kafkabank;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.Topics;
import com.kafkabank.order.InitiatePaymentRequest;
import com.kafkabank.order.InitiatePaymentResponse;
import com.kafkabank.payment.entity.Account;
import com.kafkabank.payment.repository.AccountRepository;
import com.kafkabank.reconciliation.ReconciliationController.ReconciliationView;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * Shared plumbing for the integration tests. Every assertion in this suite is on an <b>observable outcome</b> — an HTTP
 * response, a row in Postgres, or a record actually consumed off a topic — never on a mock or an internal call count.
 * Kafka is asynchronous, so "did it work?" is always "did it become true within a time budget?", which is what
 * Awaitility is for in the subclasses.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
abstract class BaseKafkaIT extends TestContainerConfig {

    /**
     * Under {@code RANDOM_PORT} Boot auto-configures this bean already pointed at the running server, so tests use
     * relative paths and nothing has to capture {@code @LocalServerPort} or build a base URL by hand.
     */
    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    protected KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    protected AccountRepository accounts;

    /**
     * A fresh account per test. The containers are reused across test classes, so sharing the seeded alice/bob rows
     * would let one test's debits change another test's starting balance depending on execution order.
     */
    protected Account newAccount(long balanceMinor) {
        return accounts.save(new Account("owner-" + UUID.randomUUID(), balanceMinor));
    }

    protected long balanceOf(Long accountId) {
        return accounts.findById(accountId).orElseThrow().getBalanceMinor();
    }

    /** Publishes an event directly, bypassing the REST API so a test can control the eventId. */
    protected void sendInitiated(String eventId, String paymentId, Long accountId, long amountMinor) {
        kafkaTemplate.send(
                Topics.PAYMENT_EVENTS,
                String.valueOf(accountId),
                new PaymentInitiated(eventId, paymentId, accountId, amountMinor, "it"));
    }

    /** Same, with a generated eventId, for tests that don't care about the id itself. */
    protected String sendInitiated(String paymentId, Long accountId, long amountMinor) {
        String eventId = UUID.randomUUID().toString();
        sendInitiated(eventId, paymentId, accountId, amountMinor);
        return eventId;
    }

    protected InitiatePaymentResponse postPayment(Long accountId, long amountMinor, String description) {
        return rest.postForEntity(
                        "/api/payments",
                        new InitiatePaymentRequest(accountId, amountMinor, description),
                        InitiatePaymentResponse.class)
                .getBody();
    }

    protected ReconciliationView reconciliationFor(String paymentId) {
        ReconciliationView view = rest.getForEntity("/api/payments/" + paymentId, ReconciliationView.class)
                .getBody();
        assertThat(view).as("no reconciliation record yet for %s", paymentId).isNotNull();
        return view;
    }

    /**
     * Reads a topic from the beginning with a throwaway consumer group, so a test sees every record regardless of what
     * the application's own consumers have already committed — separate group, separate offsets, which is the same
     * consumer-group property the reconciliation role relies on.
     *
     * <p>{@code KafkaTestUtils.getRecords} is spring-kafka-test's own "poll until you have at least N records or the
     * timeout expires" loop — worth using rather than hand-rolling, because a single {@code poll()} on a fresh consumer
     * usually returns nothing while it is still joining the group, which makes naive versions of this flaky.
     *
     * <p>Call this <b>once</b>, after the thing you're asserting about has settled. It is the wrong tool for waiting:
     * each call builds a client, joins a group and re-reads the topic from offset 0, so putting it inside a retry loop
     * re-reads everything every attempt and leaks a consumer group each time. Use {@link #awaitRecordMatching} when you
     * need to wait for a record to appear.
     */
    protected List<ConsumerRecord<String, String>> drainTopic(String topic, Duration timeout) {
        try (KafkaConsumer<String, String> consumer = newStringConsumer()) {
            consumer.subscribe(List.of(topic));
            // records(topic) is an Iterable, not a Collection, so it can't go straight into List.copyOf.
            List<ConsumerRecord<String, String>> collected = new ArrayList<>();
            KafkaTestUtils.getRecords(consumer, timeout, 1).records(topic).forEach(collected::add);
            return collected;
        }
    }

    /**
     * Waits for a record matching {@code match} to appear on {@code topic} as a result of running {@code trigger}.
     *
     * <p>Subscribes and seeks to the end <em>before</em> triggering, so it only ever sees records the trigger caused —
     * no filtering out of history left behind by earlier tests, and no re-reading the whole topic. One consumer and one
     * group for the entire wait, rather than one per polling attempt.
     */
    protected ConsumerRecord<String, String> awaitRecordMatching(
            String topic, Runnable trigger, Predicate<ConsumerRecord<String, String>> match, Duration timeout) {

        try (KafkaConsumer<String, String> consumer = newStringConsumer()) {
            consumer.subscribe(List.of(topic));
            // poll() is what actually drives the group join, so the assignment isn't known until at least one poll has
            // happened - seeking before that would silently do nothing.
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(200));
            }
            consumer.seekToEnd(consumer.assignment());
            consumer.poll(Duration.ZERO);

            trigger.run();

            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record :
                        consumer.poll(Duration.ofMillis(500)).records(topic)) {
                    if (match.test(record)) {
                        return record;
                    }
                }
            }
            throw new AssertionError("No matching record appeared on " + topic + " within " + timeout);
        }
    }

    /**
     * Waits until {@code count} records matching {@code match} have appeared on {@code topic}, reading from the
     * beginning with one throwaway group.
     *
     * <p>Use this instead of {@link #awaitRecordMatching} when the thing being asserted is <b>how many times</b>
     * something was published rather than whether it was. Counting from offset 0 is monotonic, so it cannot be raced:
     * there is no seek-to-end that has to happen before the trigger, and no window in which a fast publisher can slip a
     * record past the consumer while it is still joining the group. Filter on something unique to the test (a fresh
     * paymentId) and history from other classes is irrelevant.
     */
    protected List<ConsumerRecord<String, String>> awaitRecordCount(
            String topic, Predicate<ConsumerRecord<String, String>> match, int count, Duration timeout) {

        try (KafkaConsumer<String, String> consumer = newStringConsumer()) {
            consumer.subscribe(List.of(topic));
            List<ConsumerRecord<String, String>> matched = new ArrayList<>();
            long deadline = System.nanoTime() + timeout.toNanos();

            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record :
                        consumer.poll(Duration.ofMillis(500)).records(topic)) {
                    if (match.test(record)) {
                        matched.add(record);
                    }
                }
                if (matched.size() >= count) {
                    return matched;
                }
            }
            throw new AssertionError("Expected "
                    + count
                    + " matching records on "
                    + topic
                    + " within "
                    + timeout
                    + ", but saw "
                    + matched.size());
        }
    }

    private KafkaConsumer<String, String> newStringConsumer() {
        Map<String, Object> props =
                KafkaTestUtils.consumerProps(KAFKA.getBootstrapServers(), "test-" + UUID.randomUUID(), "false");
        // consumerProps defaults the KEY deserializer to Integer; our keys are accountId strings.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
