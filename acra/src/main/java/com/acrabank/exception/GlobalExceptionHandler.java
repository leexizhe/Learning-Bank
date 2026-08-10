package com.acrabank.exception;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ProfileNotFoundException.class)
    public ResponseEntity<Map<String, String>> notFound(ProfileNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
    }

    /**
     * 502, not 500. The distinction matters operationally: a 500 says this service is broken and sends someone to read
     * our logs, while a 502 says the thing upstream of us is - which is the truth, and is also what tells a caller that
     * retrying later is reasonable.
     */
    @ExceptionHandler(AcraApiException.class)
    public ResponseEntity<Map<String, String>> upstreamFailed(AcraApiException e) {
        log.warn("ACRA call failed: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(Map.of("error", e.getMessage()));
    }

    /**
     * The same 502, for the other shape of upstream failure. {@code SimpleAcraService} leaves RestClient's default
     * status handling in place, so a 4xx or 5xx from ACRA arrives as this rather than as an {@link AcraApiException}.
     * Without this handler it would fall through to a 500 and blame the wrong service.
     */
    @ExceptionHandler(RestClientResponseException.class)
    public ResponseEntity<Map<String, String>> upstreamRejected(RestClientResponseException e) {
        log.warn("ACRA call failed: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(Map.of("error", "ACRA returned " + e.getStatusCode().value()));
    }

    /** Connection refused, DNS failure, or a timeout - ACRA never answered at all. */
    @ExceptionHandler(ResourceAccessException.class)
    public ResponseEntity<Map<String, String>> unreachable(ResourceAccessException e) {
        log.warn("ACRA unreachable: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(Map.of("error", "ACRA unreachable: " + e.getMessage()));
    }
}
