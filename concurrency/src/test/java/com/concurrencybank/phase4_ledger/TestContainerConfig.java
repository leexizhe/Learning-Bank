package com.concurrencybank.phase4_ledger;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Singleton container for the whole test JVM: started once in a static block,
 * shared by every IT. {@code schema.sql} runs on every context start (see
 * {@code spring.sql.init.mode=always}), so it uses {@code CREATE TABLE IF NOT
 * EXISTS} to stay idempotent against a reused container.
 */
public abstract class TestContainerConfig {

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine").withReuse(true);

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
}
