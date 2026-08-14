package com.kafkabank;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Singleton containers for the whole test JVM: started once in a static block and shared by every IT, rather than one
 * broker per test class. Kafka takes a few seconds to come up, so starting it per class would dominate the run time.
 *
 * <p>{@code org.testcontainers.kafka.KafkaContainer} (not the older {@code
 * org.testcontainers.containers.KafkaContainer}) is the one that drives the official {@code apache/kafka} image in
 * <b>KRaft</b> mode — no ZooKeeper container to run alongside it. Worth knowing: KRaft replaced ZooKeeper for cluster
 * metadata in modern Kafka, so a "how does Kafka do leader election?" question has a different answer now than it did a
 * few years ago.
 */
public abstract class TestContainerConfig {

    // The distinct database name is load-bearing, not cosmetic: reuse is keyed on a hash of the container
    // configuration, so two modules asking for an identical postgres:18-alpine would share one container and collide
    // on each other's schema. Full story in postgres/src/test/java/com/postgresbank/TestContainerConfig.java.
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18-alpine")
            .withDatabaseName("kafkabank")
            .withReuse(true);

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1").withReuse(true);

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }
}
