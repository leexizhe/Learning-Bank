package com.acra.controller;

import com.acra.service.AcraService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientException;

@Slf4j
@RestController
@RequiredArgsConstructor
public class AcraController {

    private final AcraService acra;

    @GetMapping("/api/profiles/{uen}")
    public ResponseEntity<String> profile(@PathVariable String uen) {
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(acra.fetch(uen));
    }

    /** 502 rather than 500: ACRA is broken or unreachable, not this service. */
    @ExceptionHandler(RestClientException.class)
    public ResponseEntity<String> upstreamFailed(RestClientException e) {
        log.warn("ACRA call failed: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .contentType(MediaType.APPLICATION_JSON)
                .body("{\"error\":\"ACRA call failed\"}");
    }
}
