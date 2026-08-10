package com.acrabank.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.acrabank.AcraTestBase;
import com.acrabank.exception.AcraApiException;
import com.acrabank.testsupport.FakeAcraServer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The layer that makes the expiry arithmetic safe: if ACRA rejects a cached token, ACRA is right and the arithmetic is
 * wrong. This is what lets the rest of the design survive a lifetime that differs from 1799 in production, or a token
 * revoked early.
 */
class TokenReauthIT extends AcraTestBase {

    @Autowired
    AcraProfileClient client;

    @Test
    void aRejectedTokenIsReplacedAndTheCallRetriedOnce() {
        ACRA.nextProfileResponse(401, "{\"error\":\"invalid_token\"}");

        String payload = client.fetch("16888888A");

        assertThat(payload).contains("ABC ENTERPRISE");
        assertThat(ACRA.tokenRequests()).as("re-authenticated exactly once").isEqualTo(2);
        assertThat(ACRA.profileRequests()).isEqualTo(2);
        // The strong assertion: the retry did not replay the token that was just rejected. Counting calls alone would
        // pass even if it had.
        assertThat(ACRA.presentedTokens()).containsExactly("test-token-1", "test-token-2");
    }

    @Test
    void aPersistentRejectionFailsAfterExactlyOneRetry() {
        ACRA.defaultProfileResponse(401, "{\"error\":\"invalid_token\"}");

        assertThatThrownBy(() -> client.fetch("16888888A"))
                .isInstanceOf(AcraApiException.class)
                .hasMessageContaining("credentials rejected after re-authenticating");

        // Bad credentials are a configuration error. Retrying them in a loop would turn that into sustained traffic
        // against someone else's auth server.
        assertThat(ACRA.profileRequests()).isEqualTo(2);
        assertThat(ACRA.tokenRequests()).isEqualTo(2);
    }

    @Test
    void a403IsTreatedTheSameAsA401() {
        ACRA.nextProfileResponse(403, "{\"error\":\"forbidden\"}");

        assertThat(client.fetch("16888888A")).contains("ABC ENTERPRISE");
        assertThat(ACRA.tokenRequests()).isEqualTo(2);
    }

    @Test
    void aSuccessfulCallNeverReauthenticates() {
        ACRA.defaultProfileResponse(200, FakeAcraServer.SAMPLE_PROFILE);

        client.fetch("16888888A");

        assertThat(ACRA.tokenRequests()).isEqualTo(1);
        assertThat(ACRA.profileRequests()).isEqualTo(1);
    }
}
