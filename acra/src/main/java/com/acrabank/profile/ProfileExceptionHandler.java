package com.acrabank.profile;

import com.acrabank.client.AcraApiException;
import com.acrabank.client.ProfileNotFoundException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.ResourceAccessException;

@Slf4j
@RestControllerAdvice
public class ProfileExceptionHandler {

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

    /** Connection refused, DNS failure, or a timeout - ACRA never answered at all. */
    @ExceptionHandler(ResourceAccessException.class)
    public ResponseEntity<Map<String, String>> unreachable(ResourceAccessException e) {
        log.warn("ACRA unreachable: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(Map.of("error", "ACRA unreachable: " + e.getMessage()));
    }
}
