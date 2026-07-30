-- Balance is never a column. It's always SUM(postings.amount_minor) for an
-- account - a derived projection over an append-only journal, never mutated.
CREATE TABLE IF NOT EXISTS accounts (
    id         BIGSERIAL PRIMARY KEY,
    owner      VARCHAR(100) NOT NULL,
    opened_at  TIMESTAMP NOT NULL DEFAULT now()
);

-- One row per business request. The UNIQUE constraint on idempotency_key is
-- the actual idempotency guarantee - a retried request hits a unique
-- violation instead of double-posting; the "have we seen this key" read
-- beforehand is only an optimization to skip the round trip.
CREATE TABLE IF NOT EXISTS transfers (
    id               BIGSERIAL PRIMARY KEY,
    idempotency_key  VARCHAR(100) NOT NULL UNIQUE,
    from_account_id  BIGINT NOT NULL REFERENCES accounts(id),
    to_account_id    BIGINT NOT NULL REFERENCES accounts(id),
    amount_minor     BIGINT NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT now()
);

-- The ledger itself: append-only, never UPDATEd for balance purposes. A
-- transfer produces exactly two rows (a debit and a credit) in one
-- transaction - double-entry, debits = credits enforced in TransferService.
-- transfer_id is nullable because phase1's write-skew demo posts single,
-- standalone debits directly, outside the transfer/idempotency flow.
CREATE TABLE IF NOT EXISTS postings (
    id            BIGSERIAL PRIMARY KEY,
    account_id    BIGINT NOT NULL REFERENCES accounts(id),
    transfer_id   BIGINT REFERENCES transfers(id),
    amount_minor  BIGINT NOT NULL,
    note          VARCHAR(200),
    created_at    TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_postings_account_id ON postings(account_id);

-- Written in the SAME transaction as the postings above (see
-- TransferService). A separate OutboxRelay polls unpublished rows and
-- "publishes" them (logged here rather than to a real broker - the point of
-- this project is the atomicity guarantee, not a message broker).
CREATE TABLE IF NOT EXISTS outbox (
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID NOT NULL UNIQUE,
    payload     TEXT NOT NULL,
    published   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

-- A job queue. Multiple JobRunner instances claim rows with
-- SELECT ... FOR UPDATE SKIP LOCKED so they never block on - or double-claim -
-- a row another instance already grabbed.
CREATE TABLE IF NOT EXISTS payment_jobs (
    id          BIGSERIAL PRIMARY KEY,
    payload     VARCHAR(200) NOT NULL,
    status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);
