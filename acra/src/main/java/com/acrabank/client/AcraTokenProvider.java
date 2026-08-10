package com.acrabank.client;

import com.acrabank.config.AcraProperties;
import com.acrabank.exception.AcraApiException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * One access token per process, held in memory and reused until it is close to expiring.
 *
 * <p>ACRA's sandbox answers the client-credentials grant with {@code expires_in: 1799} - half an hour, less a second.
 * That number rules out both of the obvious approaches: minting a token per request wastes a round trip on every
 * lookup, and storing one as a long-lived key would leave the service returning 401s within the hour. So it is cached
 * here, and three things decide when to throw it away, in descending order of authority:
 *
 * <ol>
 *   <li>{@code expires_in} from the response, minus {@link AcraProperties#expirySkew()}. The number is read, never
 *       hardcoded, so a shorter TTL in production needs no code change.
 *   <li>{@link AcraProperties#expiryFallback()} if the field is absent or unusable. Deliberately short: guessing wrong
 *       costs one extra token call, not an outage.
 *   <li>A 401 from the profile endpoint, which calls {@link #invalidate(String)}. This is the layer that makes the
 *       other two safe - it covers early revocation and any sandbox/production difference in lifetime, so nothing here
 *       depends on 1799 being correct forever.
 * </ol>
 *
 * <p>The token is never persisted and never logged.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AcraTokenProvider {

    private final RestClient http;
    private final AcraProperties properties;
    private final Clock clock;

    private final AtomicReference<CachedToken> cached = new AtomicReference<>();

    // Only the refresh is serialised, never the cache hit: the common path reads the AtomicReference and returns
    // without touching this lock at all. Its job is to stop a burst of requests arriving on an empty cache from firing
    // N simultaneous token calls - a stampede that ACRA would see as a small denial of service.
    private final ReentrantLock refreshLock = new ReentrantLock();

    public String token() {
        CachedToken current = cached.get();
        if (current != null && current.usableAt(clock.instant())) {
            return current.value();
        }

        refreshLock.lock();
        try {
            // Re-check under the lock. Whoever held it before us has almost certainly just fetched a token, and the
            // whole point of the lock is that we use theirs.
            CachedToken afterWaiting = cached.get();
            if (afterWaiting != null && afterWaiting.usableAt(clock.instant())) {
                return afterWaiting.value();
            }
            CachedToken fresh = requestToken();
            cached.set(fresh);
            return fresh.value();
        } finally {
            refreshLock.unlock();
        }
    }

    /**
     * Drop the cached token after the API rejected it - but only if it is still the one the caller used. An
     * unconditional {@code set(null)} would race: two threads that both got a 401 on the old token would have the
     * second one throwing away the replacement the first just fetched, and the pair would ping-pong re-authenticating.
     */
    public void invalidate(String rejectedToken) {
        cached.updateAndGet(current -> current != null && current.value().equals(rejectedToken) ? null : current);
    }

    /**
     * Drop whatever is cached, unconditionally.
     *
     * <p>Deliberately separate from {@link #invalidate(String)}. That one is the 401 path and has to be conditional;
     * this one is "start cold", which is what a test needs between cases and what a force-reauthenticate admin action
     * would want.
     */
    public void discardCachedToken() {
        cached.set(null);
    }

    private CachedToken requestToken() {
        // Deliberately not RFC 6749. The spec puts grant_type in a form-encoded body; this sandbox wants it in the
        // query string with an empty JSON body, which is what the working Postman call does, so that is what is
        // reproduced here.
        String credentials = Base64.getEncoder()
                .encodeToString(
                        (properties.clientId() + ":" + properties.clientSecret()).getBytes(StandardCharsets.UTF_8));

        ResponseEntity<TokenResponse> response = http.post()
                .uri(uri -> uri.path("/authorizeServer/oauth/token")
                        .queryParam("grant_type", "client_credentials")
                        .build())
                .header(HttpHeaders.AUTHORIZATION, "Basic " + credentials)
                .contentType(MediaType.APPLICATION_JSON)
                .body("")
                .retrieve()
                .onStatus(status -> true, (request, ignored) -> {})
                .toEntity(TokenResponse.class);

        TokenResponse body = response.getBody();
        if (!response.getStatusCode().is2xxSuccessful() || body == null || body.accessToken() == null) {
            // The status is safe to log; the body is not, since a token response contains a token.
            throw new AcraApiException(
                    response.getStatusCode().value(), "token endpoint returned no usable access_token");
        }

        Duration lifetime = lifetimeOf(body);
        Instant expiresAt = clock.instant().plus(lifetime);
        log.debug("acquired ACRA token, treating it as usable for {}s", lifetime.toSeconds());
        return new CachedToken(body.accessToken(), expiresAt);
    }

    private Duration lifetimeOf(TokenResponse body) {
        Duration stated = body.expiresIn() != null && body.expiresIn() > 0
                ? Duration.ofSeconds(body.expiresIn())
                : properties.expiryFallback();

        Duration usable = stated.minus(properties.expirySkew());
        if (usable.isNegative() || usable.isZero()) {
            // A token shorter than the skew itself. Rather than treat it as already dead - which would refresh on every
            // single call - use half of what we were given.
            log.warn(
                    "ACRA token lifetime {}s is shorter than the {}s safety skew; using half of it instead",
                    stated.toSeconds(),
                    properties.expirySkew().toSeconds());
            return stated.dividedBy(2);
        }
        if (body.expiresIn() == null) {
            log.warn(
                    "token response had no expires_in; falling back to {}s",
                    properties.expiryFallback().toSeconds());
        }
        return usable;
    }

    private record CachedToken(String value, Instant expiresAt) {
        boolean usableAt(Instant now) {
            return now.isBefore(expiresAt);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record TokenResponse(
            @JsonProperty("access_token") String accessToken,
            @JsonProperty("expires_in") Long expiresIn) {}
}
