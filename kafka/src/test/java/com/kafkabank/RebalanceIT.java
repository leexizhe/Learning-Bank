package com.kafkabank;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.kafkabank.payment.entity.Account;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.annotation.DirtiesContext;

/**
 * A real consumer-group rebalance, mid-flow, with the idempotency table as the safety net.
 *
 * <p>Rebalancing is the most common operational question about Kafka and the one most people answer from a 2019 blog
 * post, so it is worth having the mechanics cold. <b>Three timeouts, three different failures:</b>
 *
 * <ul>
 *   <li>{@code heartbeat.interval.ms} — liveness, sent from a background thread. Miss these and the coordinator thinks
 *       the process died.
 *   <li>{@code session.timeout.ms} — how long the coordinator waits for those heartbeats before declaring the member
 *       dead.
 *   <li>{@code max.poll.interval.ms} — how long your <em>processing</em> may take between polls. This is the one that
 *       evicts a perfectly healthy consumer that is simply slow, and it is why {@code @RetryableTopic} exists rather
 *       than sleeping inside the listener.
 * </ul>
 *
 * <p><b>Eager vs cooperative.</b> The classic protocol is stop-the-world: every member revokes every partition, then
 * the whole group reassigns. Cooperative sticky revokes only the partitions that actually move, so consumers keep
 * working on the rest — which is why it is configured in {@code application.yml}. The upgrade path is the real
 * interview answer: you cannot flip a live group in one step, you deploy {@code [CooperativeSticky, Range]} first and
 * drop {@code Range} in a second rollout.
 *
 * <p><b>What this test forces.</b> Stopping and restarting the listener container makes it leave and rejoin the group —
 * a genuine rebalance, not a simulation — and raising the concurrency to one above the partition count reassigns
 * everything. Events are in flight across the whole thing, and every account must end up debited exactly once: <b>no
 * loss, no double-apply.</b> What saves you is the {@code processed_events} primary key, because a rebalance can
 * absolutely redeliver a record whose offset had not yet been committed. Rebalancing is not an exactly-once mechanism;
 * idempotency is what makes it survivable.
 *
 * <p><b>Also note the extra consumer does nothing.</b> Concurrency goes to 4 against 3 partitions, and the fourth
 * thread sits idle forever — a partition is assigned to exactly one consumer in a group, so consumer count above
 * partition count buys nothing. That is the ceiling {@code KafkaTopicConfig} sizes for, and the reason "just add
 * consumers" stops helping.
 *
 * <p>{@code @DirtiesContext} because this mutates the shared application context's listener container. The {@code
 * finally} restores it, but a rebuilt context after the class is cheap insurance against leaving the rest of the suite
 * running against a container this test reconfigured.
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RebalanceIT extends BaseKafkaIT {

    private static final int PAYMENTS = 12;
    private static final long STARTING_BALANCE = 100_000;
    private static final long AMOUNT = 1_000;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Test
    @Timeout(120)
    void noRecordIsLostOrDoubleAppliedAcrossARebalance() {
        List<Account> accounts = new ArrayList<>();
        for (int i = 0; i < PAYMENTS; i++) {
            accounts.add(newAccount(STARTING_BALANCE));
        }

        ConcurrentMessageListenerContainer<?, ?> container = paymentServiceContainer();
        int originalConcurrency = container.getConcurrency();

        try {
            // First half, before anything moves.
            for (int i = 0; i < PAYMENTS / 2; i++) {
                sendInitiated(UUID.randomUUID().toString(), accounts.get(i).getId(), AMOUNT);
            }

            System.out.println("RebalanceIT: assignment before = " + assignment(container));

            // Leave the group and rejoin with a different member count. This is a real rebalance: partitions are
            // revoked and reassigned.
            container.stop();
            container.setConcurrency(originalConcurrency + 1);
            container.start();

            // Second half, sent while the group is settling.
            for (int i = PAYMENTS / 2; i < PAYMENTS; i++) {
                sendInitiated(UUID.randomUUID().toString(), accounts.get(i).getId(), AMOUNT);
            }

            await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                for (Account account : accounts) {
                    assertThat(balanceOf(account.getId()))
                            .as("account %d debited exactly once across the rebalance", account.getId())
                            .isEqualTo(STARTING_BALANCE - AMOUNT);
                }
            });

            System.out.println("RebalanceIT: assignment after  = " + assignment(container));

            assertThat(assignment(container))
                    .as("a 4th consumer on a 3-partition topic gets nothing - partition count is the ceiling")
                    .hasSizeLessThanOrEqualTo(3);
        } finally {
            container.stop();
            container.setConcurrency(originalConcurrency);
            container.start();
        }
    }

    /** The container backing {@code @KafkaListener(groupId = "payment-service")}. */
    private ConcurrentMessageListenerContainer<?, ?> paymentServiceContainer() {
        for (MessageListenerContainer candidate : registry.getListenerContainers()) {
            if ("payment-service".equals(candidate.getGroupId())
                    && candidate instanceof ConcurrentMessageListenerContainer<?, ?> concurrent) {
                return concurrent;
            }
        }
        throw new AssertionError("no listener container found for group payment-service");
    }

    private static List<String> assignment(MessageListenerContainer container) {
        var assigned = container.getAssignedPartitions();
        return assigned == null
                ? List.of()
                : assigned.stream().map(Object::toString).sorted().toList();
    }
}
