package com.acrabank.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.acrabank.AcraTestBase;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * End to end over HTTP: the read-through behaviour that decides whether a lookup costs an ACRA call at all. Every
 * assertion is on requests the fake ACRA actually received, so "served from the database" means the network was
 * genuinely not touched.
 */
class ProfileCacheIT extends AcraTestBase {

    @Test
    void theFirstLookupCallsAcraAndTheSecondDoesNot() {
        ResponseEntity<String> first = rest.getForEntity("/api/profiles/16888888A", String.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody()).contains("ABC ENTERPRISE");
        assertThat(ACRA.profileRequests()).isEqualTo(1);

        ResponseEntity<String> second = rest.getForEntity("/api/profiles/16888888A", String.class);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody()).contains("ABC ENTERPRISE");
        assertThat(ACRA.profileRequests()).as("still inside the 7d TTL").isEqualTo(1);
    }

    @Test
    void refreshTrueBypassesTheTtl() {
        rest.getForEntity("/api/profiles/16888888A", String.class);
        rest.getForEntity("/api/profiles/16888888A?refresh=true", String.class);

        assertThat(ACRA.profileRequests()).isEqualTo(2);
        assertThat(profiles.count()).isEqualTo(1);
    }

    @Test
    void anExpiredTtlRefetches() {
        rest.getForEntity("/api/profiles/16888888A", String.class);

        clock.advance(Duration.ofDays(7).minusSeconds(1));
        rest.getForEntity("/api/profiles/16888888A", String.class);
        assertThat(ACRA.profileRequests()).as("one second short of the TTL").isEqualTo(1);

        clock.advance(Duration.ofSeconds(1));
        rest.getForEntity("/api/profiles/16888888A", String.class);
        assertThat(ACRA.profileRequests()).isEqualTo(2);
    }

    @Test
    void theRawEndpointReturnsTheStoredPayloadAsJson() {
        rest.getForEntity("/api/profiles/16888888A", String.class);

        ResponseEntity<String> raw = rest.getForEntity("/api/profiles/16888888A/raw", String.class);

        assertThat(raw.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(raw.getHeaders().getContentType()).isNotNull().satisfies(type -> assertThat(
                        type.includes(org.springframework.http.MediaType.APPLICATION_JSON))
                .isTrue());
        // Not a quoted string - the payload nests as real JSON.
        assertThat(raw.getBody()).startsWith("{").contains("\"primaryActivity\"");
        assertThat(ACRA.profileRequests()).as("served from the database").isEqualTo(1);
    }
}
