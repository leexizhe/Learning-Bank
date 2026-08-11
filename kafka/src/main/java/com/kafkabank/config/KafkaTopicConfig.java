package com.kafkabank.config;

import com.kafkabank.common.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics are declared here rather than left to Kafka's auto-creation, because auto-created topics silently get the
 * broker's defaults — usually 1 partition and 1 replica, which is exactly what you don't want in production and won't
 * notice until you try to scale consumers.
 */
@Configuration
public class KafkaTopicConfig {

    /**
     * Partition count is the ceiling on consumer parallelism: within one consumer group, a partition is assigned to
     * exactly one consumer, so 3 partitions means at most 3 consumers doing useful work — a 4th sits idle.
     *
     * <p>It's also close to a one-way door. You can <em>add</em> partitions later, but you can't remove them, and
     * adding them re-maps which key goes to which partition (the assignment is {@code hash(key) % partitionCount}),
     * which breaks the per-account ordering guarantee for keys that move. Hence the usual advice: size for your maximum
     * expected parallelism plus 20-30% headroom and leave it alone. 3 here is a demo number; a real payments topic
     * would be more like 24 or 48.
     */
    public static final int PARTITIONS = 3;

    /**
     * 1 replica because this project runs against a single-broker dev cluster — asking for 3 replicas on a 1-broker
     * cluster fails outright at topic creation.
     *
     * <p>In production this would be 3, paired with {@code min.insync.replicas=2}: the data survives losing a broker,
     * and combined with the producer's {@code acks=all} a write is only acknowledged once at least 2 replicas have it.
     * Those two settings only mean something together — {@code acks=all} with {@code min.insync.replicas=1} still
     * acknowledges when only the leader has the data, which is the durability gap people miss.
     */
    private static final short REPLICAS = 1;

    @Bean
    NewTopic paymentEventsTopic() {
        return TopicBuilder.name(Topics.PAYMENT_EVENTS)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    NewTopic paymentResultsTopic() {
        return TopicBuilder.name(Topics.PAYMENT_RESULTS)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    // No bean for payment-events-dlt (or the -retry-N topics) on purpose:
    // @RetryableTopic on PaymentConsumer registers those itself, derived from the
    // main topic's name. Declaring them here as well would mean two NewTopic beans racing to create the same topic. See
    // Topics.PAYMENT_EVENTS_DLT for the name that convention produces.
}
