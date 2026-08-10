package com.acrabank.controller;

import com.acrabank.service.SimpleAcraService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** The one endpoint {@link SimpleAcraService} is reachable through. */
@RestController
@RequiredArgsConstructor
public class SimpleAcraController {

    private final SimpleAcraService profiles;

    /**
     * Always calls ACRA and always rewrites the row - there is no {@code ?refresh} flag here because there is nothing
     * to bypass. Compare {@code /api/profiles/{uen}}, which answers from Postgres while the stored copy is fresh.
     */
    @GetMapping("/api/simple/profiles/{uen}")
    public ResponseEntity<String> profile(@PathVariable String uen) {
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(profiles.fetchAndStore(uen).getPayload());
    }
}
