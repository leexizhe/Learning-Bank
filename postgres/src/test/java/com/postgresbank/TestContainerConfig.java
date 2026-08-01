package com.postgresbank;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * One Postgres container for the whole test JVM, started once in a static block and shared across
 * every IT class - starting a fresh container per class would dominate the run time for no benefit
 * here.
 */
public abstract class TestContainerConfig {

  // The database name is what keeps this container distinct from the other
  // modules'. Reuse is keyed on a hash of the container configuration, so two
  // modules asking for an identically-configured postgres:18-alpine get the
  // *same* physical container - and then whichever runs second finds the
  // other's `accounts` table already there, silently skips its own
  // CREATE TABLE IF NOT EXISTS, and fails on the missing columns.
  static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:18-alpine")
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
