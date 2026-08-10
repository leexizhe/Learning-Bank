package com.acrabank.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.acrabank.AcraTestBase;
import com.acrabank.entity.BusinessProfile;
import com.acrabank.testsupport.FakeAcraServer;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/** How failures are classified, and what they are not allowed to do to stored data. */
class ProfileErrorIT extends AcraTestBase {

    /**
     * The failure mode this API actually has. ACRA does not 404 an unknown UEN - it answers {@code 200 OK} with {@code
     * {"entities":[]}}, which every naive integration stores as a real row. A mistyped UEN would then sit in the
     * database looking like a successful lookup, and be served from cache for the next week without another ACRA call
     * to correct it.
     */
    @Test
    void anUnknownUenIs404EvenThoughAcraAnswered200() {
        ACRA.defaultProfileResponse(200, FakeAcraServer.NO_SUCH_ENTITY);

        ResponseEntity<String> response = rest.getForEntity("/api/profiles/99999999X", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(profiles.count()).as("an empty result must not become a row").isZero();
    }

    @Test
    void anHttpNotFoundIsAlsoTreatedAsNotFound() {
        // Not the observed behaviour, but cheap to honour if ACRA ever changes its mind.
        ACRA.defaultProfileResponse(404, "{\"error\":\"UEN not found\"}");

        ResponseEntity<String> response = rest.getForEntity("/api/profiles/99999999X", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(profiles.count()).isZero();
    }

    @Test
    void anUpstreamFailureIs502NotFive00() {
        ACRA.defaultProfileResponse(500, "{\"error\":\"internal\"}");

        ResponseEntity<String> response = rest.getForEntity("/api/profiles/16888888A", String.class);

        // 502 says the upstream is broken, which is true and tells the caller retrying is reasonable. A 500 would send
        // someone to read our logs for someone else's outage.
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_GATEWAY);
        assertThat(profiles.count()).isZero();
    }

    @Test
    void aMalformedBodyIs502RatherThanACorruptRow() {
        ACRA.defaultProfileResponse(200, "not json at all");

        ResponseEntity<String> response = rest.getForEntity("/api/profiles/16888888A", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_GATEWAY);
        assertThat(profiles.count()).isZero();
    }

    @Test
    void aFailedRefreshLeavesThePreviouslyStoredRowIntact() {
        rest.getForEntity("/api/profiles/16888888A", String.class);
        assertThat(profiles.count()).isEqualTo(1);

        clock.advance(Duration.ofDays(30));
        ACRA.defaultProfileResponse(500, "{\"error\":\"internal\"}");

        ResponseEntity<String> response = rest.getForEntity("/api/profiles/16888888A", String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_GATEWAY);

        // The row is neither deleted nor half-written: same payload, same timestamp.
        BusinessProfile stored = profiles.findById("16888888A").orElseThrow();
        assertThat(stored.getEntityName()).isEqualTo("ABC ENTERPRISE");
        assertThat(stored.getFetchedAt()).isEqualTo(T0);
    }
}
