package com.acrabank;

import com.acrabank.client.AcraTokenProvider;
import com.acrabank.repository.BusinessProfileRepository;
import com.acrabank.testsupport.FakeAcraServer;
import com.acrabank.testsupport.MutableClock;
import java.time.Clock;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * One Postgres container and one fake ACRA for the whole test JVM, both started before any Spring context exists - the
 * {@code RestClient} bean bakes in its base URL at construction, so the server has to be listening and its port known
 * by the time properties are bound.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(AcraTestBase.FrozenClockConfig.class)
public abstract class AcraTestBase {

    /** An arbitrary but fixed starting point, so every assertion about time is exact. */
    protected static final Instant T0 = Instant.parse("2026-08-06T09:00:00Z");

    // The database name is what keeps this container distinct from the other modules'. Testcontainers keys reuse on a
    // hash of the configuration, so two modules asking for an identically-configured postgres:18-alpine would get the
    // *same* physical container - and the second one to run would find the first one's tables already there. See the
    // same note in the postgres module's TestContainerConfig.
    protected static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18-alpine")
            .withDatabaseName("acrabank")
            .withReuse(true);

    protected static final FakeAcraServer ACRA = new FakeAcraServer();

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);

        registry.add("acra.base-url", ACRA::baseUrl);
        // Supplied here rather than resolved from the environment, so the whole suite runs on a machine that has never
        // seen an ACRA credential.
        registry.add("acra.client-id", () -> "test-client");
        registry.add("acra.client-secret", () -> "test-secret");
    }

    @Autowired
    protected MutableClock clock;

    @Autowired
    protected BusinessProfileRepository profiles;

    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    private AcraTokenProvider tokens;

    /**
     * The Spring context is cached across test classes, so the token provider is the same singleton all the way
     * through. Without the discard below, a token minted in one test would still be valid in the next - the clock
     * having been wound back to T0 - and every assertion counting token requests would be measuring leftover state.
     */
    @BeforeEach
    void resetTheWorld() {
        ACRA.reset();
        clock.reset(T0);
        tokens.discardCachedToken();
        profiles.deleteAll();
    }

    @TestConfiguration
    static class FrozenClockConfig {

        /**
         * Replaces the application's {@code Clock.systemUTC()}. {@code @Primary} rather than a bean override, so the
         * production bean definition is left exactly as it is.
         */
        @Bean
        @Primary
        MutableClock testClock() {
            return new MutableClock(T0);
        }

        /** Kept so anything injecting the interface resolves to the same instance. */
        @Bean
        Clock clockAlias(MutableClock testClock) {
            return testClock;
        }
    }
}
