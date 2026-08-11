package com.acra.service;

import com.acra.config.AcraProperties;
import com.acra.entity.AcraProfile;
import com.acra.repository.AcraProfileRepository;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.server.ResponseStatusException;

/** Mint a token, call ACRA's business profile endpoint, store what came back. */
@Service
@RequiredArgsConstructor
public class AcraService {

    private static final String TOKEN_PATH = "/authorizeServer/oauth/token";
    private static final String PROFILE_PATH = "/api/acra/entityQuery/businessProfile";

    private final RestClient http;
    private final AcraProperties properties;
    private final AcraProfileRepository profiles;
    private final ObjectMapper json;

    public String fetch(String uen) {
        String payload = http.get()
                .uri(uri -> uri.path(PROFILE_PATH).queryParam("uen", uen).build())
                // Not "Authorization: Bearer", despite the grant returning token_type: Bearer - this endpoint wants the
                // raw token in a header named "token".
                .header("token", token())
                .retrieve()
                .body(String.class);

        // ACRA answers an unknown UEN with 200 and an empty entities array, so the status alone can't tell a hit from a
        // miss.
        JsonNode entities = parse(payload).at("/entities");
        if (!entities.isArray() || entities.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "no ACRA profile for UEN " + uen);
        }

        // --- persistence: comment out these two lines to run without a database ---
        String entityName = entities.at("/0/entityName").asText(null);
        profiles.save(new AcraProfile(uen, entityName, payload, Instant.now()));
        // --------------------------------------------------------------------------

        return payload;
    }

    private String token() {
        String credentials = Base64.getEncoder()
                .encodeToString(
                        (properties.clientId() + ":" + properties.clientSecret()).getBytes(StandardCharsets.UTF_8));

        // Deliberately not RFC 6749: this API wants grant_type in the query string with an empty JSON body.
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
            throw new IllegalStateException("ACRA's token endpoint returned no access_token");
        }
        return body.accessToken();
    }

    @SneakyThrows
    private JsonNode parse(String payload) {
        return json.readTree(payload);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record TokenResponse(
            @JsonProperty("access_token") String accessToken) {}
}
