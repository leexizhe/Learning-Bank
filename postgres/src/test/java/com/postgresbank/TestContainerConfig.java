package com.postgresbank;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * One Postgres container for the whole test JVM, started once in a static
 * block and shared across every IT class - starting a fresh container per
 * class would dominate the run time for no benefit here.
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
