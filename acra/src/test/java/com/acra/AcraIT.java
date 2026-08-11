package com.acra;

import static org.assertj.core.api.Assertions.assertThat;

import com.acra.repository.AcraProfileRepository;
import com.acra.testsupport.FakeAcraServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * The whole flow against a real Postgres and a real (fake) ACRA. Both start before the Spring context, because the
 * RestClient bakes in its base URL when it is built.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AcraIT {

    private static final String UEN = "16888888A";

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18-alpine")
            .withDatabaseName("acrabank")
            .withReuse(true);

    static final FakeAcraServer ACRA = new FakeAcraServer();

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);

        registry.add("acra.base-url", ACRA::baseUrl);
        // Supplied here, so the suite runs on a machine that has never seen an ACRA credential.
        registry.add("acra.client-id", () -> "test-client");
        registry.add("acra.client-secret", () -> "test-secret");
    }

    @Autowired
    AcraProfileRepository profiles;

    @Autowired
    TestRestTemplate rest;

    @BeforeEach
    void reset() {
        ACRA.reset();
        profiles.deleteAll();
    }

    @Test
    void aLookupMintsATokenAndStoresTheProfile() {
        ResponseEntity<String> response = rest.getForEntity("/api/profiles/" + UEN, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(ACRA.tokenRequests()).isEqualTo(1);
        assertThat(ACRA.profileRequests()).isEqualTo(1);
        assertThat(ACRA.presentedTokens()).containsExactly("test-token-1");

        assertThat(profiles.findById(UEN)).get().satisfies(stored -> {
            assertThat(stored.getEntityName()).isEqualTo("ABC ENTERPRISE");
            assertThat(stored.getPayload()).contains("SOLE-PROPRIETOR");
            assertThat(stored.getFetchedAt()).isNotNull();
        });
    }

    @Test
    void everyLookupMintsItsOwnToken() {
        rest.getForEntity("/api/profiles/" + UEN, String.class);
        rest.getForEntity("/api/profiles/" + UEN, String.class);

        // Nothing is cached, so the second lookup authenticates again and the row is simply rewritten.
        assertThat(ACRA.tokenRequests()).isEqualTo(2);
        assertThat(ACRA.presentedTokens()).containsExactly("test-token-1", "test-token-2");
        assertThat(profiles.count()).isEqualTo(1);
    }

    @Test
    void anUnknownUenIs404EvenThoughAcraAnswered200() {
        ResponseEntity<String> response = rest.getForEntity("/api/profiles/99999999X", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(profiles.count()).isZero();
    }
}
