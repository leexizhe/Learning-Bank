-- The UEN is the primary key: ACRA already guarantees it is unique per entity, so a surrogate id would add nothing
-- except the possibility of two rows for one company.
--
-- payload holds the entire ACRA response. Everything else in this table is derived from it and exists only so the
-- common queries don't have to go through JSON operators - which is why all five are nullable. A field ACRA renames
-- costs a null column and a warning in the log; it never costs the data, because the response is stored whole either
-- way.
--
-- jsonb rather than json: it is parsed once on write instead of on every read, and it is the only one of the two that
-- can be indexed. The tradeoff is that it does not preserve key order or insignificant whitespace, so what you read
-- back is semantically equal to the response, not textually identical to it.
CREATE TABLE IF NOT EXISTS business_profile (
    uen                VARCHAR(20) PRIMARY KEY,
    entity_name        VARCHAR(500),
    entity_status      VARCHAR(100),
    entity_type        VARCHAR(200),
    registration_date  DATE,
    payload            JSONB NOT NULL,
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_business_profile_entity_name ON business_profile(entity_name);

-- Supports "which profiles are stale enough to re-fetch" without a sequential scan once this table is big enough for
-- that to matter.
CREATE INDEX IF NOT EXISTS idx_business_profile_fetched_at ON business_profile(fetched_at);
