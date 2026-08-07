package com.acrabank.profile;

import com.acrabank.client.AcraProfileClient;
import com.acrabank.config.AcraProperties;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Read-through cache over ACRA: serve the stored row while it is fresh, otherwise fetch, store, and serve that. Note
 * there is no {@code @Transactional} here - see {@link BusinessProfileTransactionalOps} for why the network call is
 * kept outside the transaction.
 */
@Slf4j
@Service
public class BusinessProfileService {

    private final AcraProfileClient acra;
    private final BusinessProfileTransactionalOps db;
    private final ProfileMapper mapper;
    private final AcraProperties properties;
    private final Clock clock;

    public BusinessProfileService(
            AcraProfileClient acra,
            BusinessProfileTransactionalOps db,
            ProfileMapper mapper,
            AcraProperties properties,
            Clock clock) {
        this.acra = acra;
        this.db = db;
        this.mapper = mapper;
        this.properties = properties;
        this.clock = clock;
    }

    public BusinessProfile get(String uen, boolean forceRefresh) {
        Optional<BusinessProfile> stored = db.find(uen);

        if (!forceRefresh && stored.isPresent() && isFresh(stored.get())) {
            log.debug("serving {} from the database, no ACRA call", uen);
            return stored.get();
        }

        // If this throws, the error surfaces and nothing is written - any previously stored row keeps its old payload
        // and its old fetched_at rather than being half-overwritten. Note that it is not *served* either: a caller
        // asking for a profile we could not refresh gets a 502, not silently stale data. Falling back to the stale row
        // would be a defensible alternative, but it should be an explicit choice with its own flag, not an accident of
        // error handling.
        String payload = acra.fetch(uen);

        // Parsed and validated before a transaction is opened, not inside one. ACRA signals "no such UEN" with an empty
        // entities array under a 200, so this is the check that stops a mistyped UEN becoming an empty row that then
        // gets served from cache for a week.
        JsonNode root = mapper.parse(payload);
        mapper.requireEntity(root, uen);

        return db.upsert(uen, payload, root, clock.instant());
    }

    private boolean isFresh(BusinessProfile profile) {
        Instant staleBefore = clock.instant().minus(properties.profileTtl());
        return profile.getFetchedAt().isAfter(staleBefore);
    }
}
