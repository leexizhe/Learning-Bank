package com.postgresbank;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * One Postgres container for the whole test JVM, started once in a static block and shared across every IT class -
 * starting a fresh container per class would dominate the run time for no benefit here.
 */
public abstract class TestContainerConfig {

    // The database name is what keeps this container distinct from the other modules', and this is the canonical
    // explanation of why — the kafka and concurrency suites each repeat the rule in one line and point here.
    //
    // Reuse is keyed on a hash of the container configuration, so two modules asking for an identically-configured
    // postgres:18-alpine get handed the *same* physical container. Whichever runs second then finds the other's
    // `accounts` table already there, silently skips its own CREATE TABLE IF NOT EXISTS, and fails on the missing
    // columns. Note how badly that fails: not "container in use", but a schema error in a module that never touched
    // the other's schema — and only when the two run in the same session, so it cannot reproduce on CI, where
    // TESTCONTAINERS_REUSE_ENABLE is unset and reuse degrades to a no-op.
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18-alpine")
            .withDatabaseName("postgresbank")
            .withReuse(true);

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
