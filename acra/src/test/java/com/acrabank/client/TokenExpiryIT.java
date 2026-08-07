package com.acrabank.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.acrabank.AcraTestBase;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The expiry arithmetic, asserted exactly rather than approximately.
 *
 * <p>Every case here moves an injected clock instead of sleeping. That is not only faster - a sleeping version of
 * {@link #refreshesOneSecondBeforeTheSkewedExpiry()} would take half an hour - it is the only version that proves
 * anything. A timing assertion that passes tells you the machine was not busy, not that the boundary is where you think
 * it is.
 */
class TokenExpiryIT extends AcraTestBase {

    @Autowired
    AcraProfileClient client;

    @Test
    void refreshesOneSecondBeforeTheSkewedExpiry() {
        // The real sandbox value. 1799 stated, minus the 60s skew, leaves 1739s of use.
        ACRA.issueTokensValidFor(1799L);
        client.fetch("16888888A");
        assertThat(ACRA.tokenRequests()).isEqualTo(1);

        clock.advance(Duration.ofSeconds(1738));
        client.fetch("16888888A");
        assertThat(ACRA.tokenRequests())
                .as("still inside the usable window at 1738s")
                .isEqualTo(1);

        clock.advance(Duration.ofSeconds(1));
        client.fetch("16888888A");
        assertThat(ACRA.tokenRequests())
                .as("skew reached at 1739s, 60s before ACRA drops it")
                .isEqualTo(2);
        assertThat(ACRA.presentedTokens()).endsWith("test-token-2");
    }

    @Test
    void honoursAShorterExpiresInWithoutACodeChange() {
        // If ACRA tightens the lifetime in production, nothing here needs editing: the number is read from the
        // response, never assumed.
        ACRA.issueTokensValidFor(120L);
        client.fetch("16888888A");

        clock.advance(Duration.ofSeconds(59));
        client.fetch("16888888A");
        assertThat(ACRA.tokenRequests()).isEqualTo(1);

        clock.advance(Duration.ofSeconds(1));
        client.fetch("16888888A");
        assertThat(ACRA.tokenRequests()).as("120s - 60s skew = 60s of use").isEqualTo(2);
    }

    @Test
    void fallsBackToTheConfiguredLifetimeWhenExpiresInIsMissing() {
        ACRA.issueTokensValidFor(null);
        client.fetch("16888888A");

        // 30m fallback - 60s skew = 29m.
        clock.advance(Duration.ofMinutes(29).minusSeconds(1));
        client.fetch("16888888A");
        assertThat(ACRA.tokenRequests()).isEqualTo(1);

        clock.advance(Duration.ofSeconds(1));
        client.fetch("16888888A");
        assertThat(ACRA.tokenRequests()).isEqualTo(2);
    }

    @Test
    void aLifetimeShorterThanTheSkewStillGetsUsedRatherThanRefreshedEveryCall() {
        // The degenerate case: 1s stated against a 60s skew. Treating that as already expired would mean a token call
        // per request, which is worse than useless - so the provider falls back to half the stated lifetime.
        ACRA.issueTokensValidFor(1L);
        client.fetch("16888888A");
        assertThat(ACRA.tokenRequests()).isEqualTo(1);

        clock.advance(Duration.ofMillis(400));
        client.fetch("16888888A");
        assertThat(ACRA.tokenRequests())
                .as("500ms of usable life, 400ms elapsed")
                .isEqualTo(1);

        clock.advance(Duration.ofMillis(100));
        client.fetch("16888888A");
        assertThat(ACRA.tokenRequests()).isEqualTo(2);
    }
}
