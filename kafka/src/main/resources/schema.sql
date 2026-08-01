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

-- The transactional outbox. The result of a payment is written HERE, in the same
-- transaction as the debit and the processed_events row, rather than published to
-- Kafka directly from the consumer. One atomic write instead of a database commit
-- and a broker publish that can't be made atomic with each other.
--
-- The unique key is source_event_id - the id of the PaymentInitiated event that
-- produced this result, not the result's own id. That's what makes redelivery
-- harmless: at most one outbox row can ever exist per consumed event, so a
-- duplicate delivery finds the row already there and re-publishing is the relay's
-- problem rather than the consumer's. Same shape as processed_events: the
-- constraint is the guarantee, the check is the optimization.
CREATE TABLE IF NOT EXISTS payment_outbox (
    id              BIGSERIAL PRIMARY KEY,
    source_event_id VARCHAR(64)  NOT NULL UNIQUE,
    topic           VARCHAR(100) NOT NULL,
    message_key     VARCHAR(64)  NOT NULL,
    payload         TEXT         NOT NULL,
    published       BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Partial index: the relay only ever asks for unpublished rows, and that set stays
-- tiny while the table grows forever. Indexing WHERE NOT published keeps the index
-- proportional to the backlog rather than to history.
CREATE INDEX IF NOT EXISTS idx_payment_outbox_unpublished
    ON payment_outbox (id) WHERE NOT published;

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
