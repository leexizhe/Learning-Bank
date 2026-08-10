package com.acrabank.client;

import com.acrabank.exception.AcraApiException;
import com.acrabank.exception.ProfileNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * The Business Profile (Company) call. Returns the response body verbatim - this class deliberately knows nothing about
 * the shape of an ACRA profile, so a field ACRA adds tomorrow is stored today.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AcraProfileClient {

    private static final String PROFILE_PATH = "/api/acra/entityQuery/businessProfile";

    private final RestClient http;
    private final AcraTokenProvider tokens;

    public String fetch(String uen) {
        String token = tokens.token();
        ResponseEntity<String> response = request(uen, token);

        // The cached token was accepted when we minted it and is not accepted now. Rather than trusting our own expiry
        // arithmetic, let ACRA be the authority: throw the token away and try once more with a fresh one.
        if (rejectedCredentials(response.getStatusCode())) {
            log.debug("ACRA rejected the cached token ({}), re-authenticating", response.getStatusCode());
            tokens.invalidate(token);
            response = request(uen, tokens.token());

            // Exactly one retry. A second rejection means the credentials are wrong or the account has lost access, and
            // retrying that in a loop turns a config error into a hammering of someone else's auth server.
            if (rejectedCredentials(response.getStatusCode())) {
                throw new AcraApiException(
                        response.getStatusCode().value(), "credentials rejected after re-authenticating");
            }
        }

        if (response.getStatusCode().value() == HttpStatus.NOT_FOUND.value()) {
            throw new ProfileNotFoundException(uen);
        }
        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new AcraApiException(response.getStatusCode().value(), truncate(response.getBody()));
        }

        String body = response.getBody();
        if (body == null || body.isBlank()) {
            throw new AcraApiException(response.getStatusCode().value(), "empty response body");
        }
        return body;
    }

    private ResponseEntity<String> request(String uen, String token) {
        return http.get()
                .uri(uri -> uri.path(PROFILE_PATH).queryParam("uen", uen).build())
                // Not "Authorization: Bearer", despite the grant returning token_type: Bearer. This endpoint wants the
                // raw token in its own header.
                .header("token", token)
                .retrieve()
                .onStatus(status -> true, (request, ignored) -> {})
                .toEntity(String.class);
    }

    private boolean rejectedCredentials(org.springframework.http.HttpStatusCode status) {
        return status.value() == HttpStatus.UNAUTHORIZED.value() || status.value() == HttpStatus.FORBIDDEN.value();
    }

    private String truncate(String body) {
        if (body == null) {
            return "<no body>";
        }
        return body.length() <= 500 ? body : body.substring(0, 500) + "...";
    }
}
