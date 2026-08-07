package com.acrabank.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.acrabank.AcraTestBase;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** The whole reason the token is held in memory: many lookups, one authentication. */
class TokenCachingIT extends AcraTestBase {

    @Autowired
    AcraProfileClient client;

    @Test
    void manyProfileLookupsShareOneToken() {
        for (int i = 0; i < 10; i++) {
            client.fetch("1688888" + i + "A");
        }

        assertThat(ACRA.profileRequests()).isEqualTo(10);
        assertThat(ACRA.tokenRequests())
                .as("ten lookups must not cost ten authentications")
                .isEqualTo(1);
        // Not just "one token call happened" - every lookup carried the *same* token, which is the actual claim.
        assertThat(ACRA.presentedTokens()).containsOnly("test-token-1");
    }

    @Test
    void authenticatesWithHttpBasic() {
        client.fetch("16888888A");

        String expected = "Basic "
                + Base64.getEncoder().encodeToString("test-client:test-secret".getBytes(StandardCharsets.UTF_8));
        assertThat(ACRA.tokenAuthHeaders()).containsExactly(expected);
    }

    @Test
    void passesTheUenThroughAndTheTokenInItsOwnHeader() {
        client.fetch("16888888A");

        assertThat(ACRA.requestedUens()).containsExactly("16888888A");
        // Not Authorization: Bearer, despite token_type saying Bearer.
        assertThat(ACRA.presentedTokens()).containsExactly("test-token-1");
    }
}
