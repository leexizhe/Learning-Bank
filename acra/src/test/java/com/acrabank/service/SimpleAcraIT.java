package com.acrabank.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.acrabank.AcraTestBase;
import com.acrabank.entity.BusinessProfile;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;

/**
 * {@link SimpleAcraService} against a real Redis and the fake ACRA, asserting on traffic the server actually received
 * rather than on what the service says it did.
 *
 * <p>One thing these tests cannot do, and it is the cost of moving expiry into Redis: the module's {@code MutableClock}
 * has no effect here, because Redis expires keys by its own wall clock. So the third test proves the TTL is *set*, not
 * that expiry works. {@code TokenExpiryIT} can assert the in-memory boundary to the second precisely because that
 * arithmetic runs against the injected clock - that precision is what was traded away for the simpler code.
 */
class SimpleAcraIT extends AcraTestBase {

    private static final String UEN = "16888888A";

    private static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    static {
        REDIS.start();
    }

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        // Spring runs every @DynamicPropertySource in the hierarchy, so the base class's Postgres and ACRA properties
        // still apply - this only adds Redis on top.
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    SimpleAcraService service;

    @Autowired
    StringRedisTemplate redis;

    /**
     * The Redis container outlives the test class, so without this a token cached by one test would still be there for
     * the next and every token-request count would be measuring leftover state. Same concern as the
     * {@code discardCachedToken} call in {@link AcraTestBase}.
     */
    @BeforeEach
    void clearTheCachedToken() {
        redis.delete("acra:token");
    }

    @Test
    void aFirstLookupMintsATokenAndStoresTheProfile() {
        BusinessProfile stored = service.fetchAndStore(UEN);

        assertThat(ACRA.tokenRequests()).isEqualTo(1);
        assertThat(ACRA.profileRequests()).isEqualTo(1);
        assertThat(ACRA.requestedUens()).containsExactly(UEN);
        assertThat(ACRA.presentedTokens()).containsExactly("test-token-1");

        assertThat(stored.getUen()).isEqualTo(UEN);
        assertThat(stored.getEntityName()).isEqualTo("ABC ENTERPRISE");
        // Read back from Postgres rather than trusting the returned instance, which could be a JPA cache hit.
        assertThat(profiles.findById(UEN))
                .get()
                .extracting(BusinessProfile::getPayload)
                .isNotNull();
    }

    @Test
    void aSecondLookupReusesTheTokenFromRedis() {
        service.fetchAndStore(UEN);
        service.fetchAndStore(UEN);

        assertThat(ACRA.tokenRequests())
                .as("the second lookup read the token out of Redis")
                .isEqualTo(1);
        assertThat(ACRA.profileRequests())
                .as("but still fetched the profile again")
                .isEqualTo(2);
        assertThat(ACRA.presentedTokens()).containsExactly("test-token-1", "test-token-1");
    }

    @Test
    void theTokenIsStoredUnderATtl() {
        service.fetchAndStore(UEN);

        Long ttl = redis.getExpire("acra:token");
        // -1 is Redis for "key exists, no expiry" and -2 for "no such key"; either would mean the token outlives its
        // welcome, which is the one thing the configured TTL exists to prevent.
        assertThat(ttl)
                .isNotNull()
                .isPositive()
                .isLessThanOrEqualTo(Duration.ofMinutes(25).toSeconds());
    }
}
