package com.acrabank.profile;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Optional;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The database half of a read-through fetch, kept separate from {@link BusinessProfileService} for one reason: the HTTP
 * call to ACRA must not happen inside a transaction.
 *
 * <p>A {@code @Transactional} method holds a pooled connection for its whole duration. Wrapping the network call would
 * mean a connection sitting idle-in-transaction for however long a government API takes to answer - so a slow upstream
 * stops being a latency problem and becomes a pool exhaustion problem, taking down endpoints that never touch ACRA at
 * all. Same reasoning as {@code OutboxRelayTransactionalOps} in the postgres module.
 */
@Component
public class BusinessProfileTransactionalOps {

    private final BusinessProfileRepository profiles;
    private final ProfileMapper mapper;

    public BusinessProfileTransactionalOps(BusinessProfileRepository profiles, ProfileMapper mapper) {
        this.profiles = profiles;
        this.mapper = mapper;
    }

    @Transactional(readOnly = true)
    public Optional<BusinessProfile> find(String uen) {
        return profiles.findById(uen);
    }

    /**
     * Insert or update, keyed on the UEN. Because the id is assigned rather than generated, JPA cannot tell a new
     * entity from a detached one, so the row is loaded first and mutated if it exists - which also means a re-fetch
     * advances {@code fetched_at} on the existing row instead of inserting a second one.
     */
    @Transactional
    public BusinessProfile upsert(String uen, String payload, JsonNode root, Instant fetchedAt) {
        BusinessProfile profile = profiles.findById(uen).orElseGet(() -> new BusinessProfile(uen));
        mapper.apply(profile, payload, root, fetchedAt);
        return profiles.save(profile);
    }
}
