package com.acrabank.service;

import com.acrabank.config.AcraProperties;
import com.acrabank.entity.BusinessProfile;
import com.acrabank.mapper.ProfileMapper;
import com.acrabank.repository.BusinessProfileRepository;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

/**
 * The whole ACRA flow in a straight line: read the token from Redis, mint one if it is not there, call the profile
 * endpoint, save the row.
 *
 * <p>This is the deliberately plain alternative to {@code AcraTokenProvider} + {@code AcraProfileClient} +
 * {@code BusinessProfileService}. The token lives in Redis under one key with a fixed TTL from
 * {@link AcraProperties#tokenCacheTtl()}, and that TTL is the entire expiry policy - nothing here reads
 * {@code expires_in}, and there is no safety skew.
 *
 * <p>Three things the older stack does are missing on purpose, and each is a real tradeoff rather than an oversight:
 *
 * <ul>
 *   <li>No stampede lock. A burst arriving on a cold cache mints several tokens instead of one. ACRA issues them
 *       happily and the last write wins, so the cost is a few wasted round trips, not a correctness problem.
 *   <li>No 401 invalidation. If ACRA revokes a token early, calls fail until the TTL lapses. Keeping the TTL well under
 *       the advertised lifetime is what makes that unlikely rather than a retry path that makes it recoverable.
 *   <li>No freshness check on the stored profile. Every call goes to ACRA and overwrites the row, where
 *       {@code BusinessProfileService} would have served the database copy for a week.
 * </ul>
 *
 * <p>Shared with the older stack: the {@code business_profile} table, and {@link ProfileMapper} for parsing and column
 * promotion - that logic is about ACRA's response shape, which simplifying the caching does not change.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SimpleAcraService {

    private static final String TOKEN_KEY = "acra:token";
    private static final String TOKEN_PATH = "/authorizeServer/oauth/token";
    private static final String PROFILE_PATH = "/api/acra/entityQuery/businessProfile";

    private final StringRedisTemplate redis;
    private final RestClient http;
    private final AcraProperties properties;
    private final BusinessProfileRepository profiles;
    private final ProfileMapper mapper;
    private final Clock clock;

    /**
     * Fetch the profile for {@code uen} from ACRA and store it, returning the saved row.
     *
     * <p>Not {@code @Transactional}, which matters more than it looks: a transaction here would hold a pooled
     * connection open across the HTTP call, so a slow government API would become pool exhaustion for endpoints that
     * never touch ACRA. {@code save} opens its own short transaction after the network work is done.
     */
    public BusinessProfile fetchAndStore(String uen) {
        String payload = http.get()
                .uri(uri -> uri.path(PROFILE_PATH).queryParam("uen", uen).build())
                // Not "Authorization: Bearer", despite the grant returning token_type: Bearer. This endpoint wants the
                // raw token in its own header.
                .header("token", token())
                .retrieve()
                .body(String.class);

        // ACRA signals "no such UEN" with an empty entities array under a 200, so the status alone cannot tell a hit
        // from a miss. Checking before the write is what stops a mistyped UEN becoming a valid-looking empty row.
        JsonNode root = mapper.parse(payload);
        mapper.requireEntity(root, uen);

        // The id is assigned rather than generated, so JPA cannot tell a new entity from a detached one - load first,
        // then mutate, and a re-fetch updates the existing row instead of inserting a second one.
        BusinessProfile profile = profiles.findById(uen).orElseGet(() -> new BusinessProfile(uen));
        mapper.apply(profile, payload, root, clock.instant());
        return profiles.save(profile);
    }

    private String token() {
        String cached = redis.opsForValue().get(TOKEN_KEY);
        if (cached != null) {
            return cached;
        }

        String fresh = requestToken();
        // The three-argument set is the entire expiry mechanism: Redis drops the key on its own once the TTL lapses,
        // and the next call falls through to a mint. Nothing in this class ever has to ask whether a token is old.
        redis.opsForValue().set(TOKEN_KEY, fresh, properties.tokenCacheTtl());
        log.debug("cached a new ACRA token for {}s", properties.tokenCacheTtl().toSeconds());
        return fresh;
    }

    private String requestToken() {
        String credentials = Base64.getEncoder()
                .encodeToString(
                        (properties.clientId() + ":" + properties.clientSecret()).getBytes(StandardCharsets.UTF_8));

        // Deliberately not RFC 6749. The spec puts grant_type in a form-encoded body; this sandbox wants it in the
        // query string with an empty JSON body, which is what the working Postman call does.
        TokenResponse body = http.post()
                .uri(uri -> uri.path(TOKEN_PATH)
                        .queryParam("grant_type", "client_credentials")
                        .build())
                .header(HttpHeaders.AUTHORIZATION, "Basic " + credentials)
                .contentType(MediaType.APPLICATION_JSON)
                .body("")
                .retrieve()
                .body(TokenResponse.class);

        if (body == null || body.accessToken() == null) {
            // No body in the message: a token response contains a token.
            throw new IllegalStateException("ACRA's token endpoint returned no access_token");
        }
        return body.accessToken();
    }

    /**
     * Only the field that is used. {@code expires_in} is read by {@code AcraTokenProvider} but deliberately ignored
     * here - the configured TTL decides everything.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record TokenResponse(
            @JsonProperty("access_token") String accessToken) {}
}
