package com.acrabank.controller;

import com.acrabank.dto.ProfileResponse;
import com.acrabank.entity.BusinessProfile;
import com.acrabank.mapper.ProfileMapper;
import com.acrabank.service.BusinessProfileService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class BusinessProfileController {

    private final BusinessProfileService profiles;
    private final ProfileMapper mapper;

    /**
     * Read-through: answers from Postgres while the stored copy is inside the TTL, calls ACRA otherwise. {@code
     * ?refresh=true} skips the freshness check and always calls ACRA.
     */
    @GetMapping("/api/profiles/{uen}")
    public ProfileResponse profile(@PathVariable String uen, @RequestParam(defaultValue = "false") boolean refresh) {
        BusinessProfile profile = profiles.get(uen, refresh);
        return new ProfileResponse(
                profile.getUen(),
                profile.getEntityName(),
                profile.getEntityStatus(),
                profile.getEntityType(),
                profile.getRegistrationDate(),
                profile.getFetchedAt(),
                // Re-parsed rather than passed through as a String, so the payload nests as real JSON in the response
                // instead of arriving as one escaped blob.
                mapper.parse(profile.getPayload()));
    }

    /** The stored ACRA response on its own, for callers that want exactly what ACRA said. */
    @GetMapping("/api/profiles/{uen}/raw")
    public ResponseEntity<String> raw(@PathVariable String uen, @RequestParam(defaultValue = "false") boolean refresh) {
        BusinessProfile profile = profiles.get(uen, refresh);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(profile.getPayload());
    }
}
