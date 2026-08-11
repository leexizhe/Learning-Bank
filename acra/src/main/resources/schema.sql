-- The UEN is the primary key: ACRA already guarantees it is unique per entity. payload holds the whole response, so a
-- field ACRA renames costs a null column and never the data.
CREATE TABLE IF NOT EXISTS acra_profile (
    uen          VARCHAR(20) PRIMARY KEY,
    entity_name  VARCHAR(500),
    payload      JSONB NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
