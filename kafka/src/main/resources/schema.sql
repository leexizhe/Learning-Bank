-- CREATE TABLE IF NOT EXISTS everywhere: spring.sql.init runs on every startup, and
-- Testcontainers may reuse a container across test classes.

CREATE TABLE IF NOT EXISTS accounts (
    id            BIGSERIAL PRIMARY KEY,
    owner         VARCHAR(255) NOT NULL UNIQUE,
    balance_minor BIGINT       NOT NULL,
    version       BIGINT       NOT NULL DEFAULT 0
);

-- The idempotent-consumer table. Kafka gives at-least-once delivery, so the same
-- event CAN arrive twice (e.g. the consumer crashed after the DB commit but before
-- the offset commit). The PRIMARY KEY on event_id is the actual guarantee: the
-- second attempt to insert the same event id fails, and we skip re-applying it.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     VARCHAR(64) PRIMARY KEY,
    payment_id   VARCHAR(64) NOT NULL,
    processed_at TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- The reconciliation view: one row per payment, filled in from BOTH topics as the
-- two halves of the story arrive (initiated from payment-events, outcome from
-- payment-results). They can arrive in either order, which is the whole problem
-- reconciliation exists to solve.
CREATE TABLE IF NOT EXISTS reconciliation_records (
    payment_id    VARCHAR(64) PRIMARY KEY,
    account_id    BIGINT,
    amount_minor  BIGINT,
    initiated_seen BOOLEAN    NOT NULL DEFAULT FALSE,
    result_status VARCHAR(32),
    state         VARCHAR(32) NOT NULL,
    updated_at    TIMESTAMP   NOT NULL DEFAULT NOW()
);

INSERT INTO accounts (owner, balance_minor)
VALUES ('alice', 1000000), ('bob', 500000), ('treasury', 100000000)
ON CONFLICT (owner) DO NOTHING;
