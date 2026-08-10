-- Balance is never a column. It's always SUM(postings.amount_minor) for an account - a derived projection over an
-- append-only journal, never mutated.
CREATE TABLE IF NOT EXISTS accounts (
    id         BIGSERIAL PRIMARY KEY,
    owner      VARCHAR(100) NOT NULL,
    opened_at  TIMESTAMP NOT NULL DEFAULT now()
);

-- One row per business request. The UNIQUE constraint on idempotency_key is the actual idempotency guarantee - a
-- retried request hits a unique violation instead of double-posting; the "have we seen this key" read beforehand is
-- only an optimization to skip the round trip.
CREATE TABLE IF NOT EXISTS transfers (
    id               BIGSERIAL PRIMARY KEY,
    idempotency_key  VARCHAR(100) NOT NULL UNIQUE,
    from_account_id  BIGINT NOT NULL REFERENCES accounts(id),
    to_account_id    BIGINT NOT NULL REFERENCES accounts(id),
    amount_minor     BIGINT NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT now()
);

-- The ledger itself: append-only, never UPDATEd for balance purposes. A transfer produces exactly two rows (a debit and
-- a credit) in one transaction - double-entry, debits = credits enforced in TransferService. transfer_id is nullable
-- because phase1's write-skew demo posts single, standalone debits directly, outside the transfer/idempotency flow.
CREATE TABLE IF NOT EXISTS postings (
    id            BIGSERIAL PRIMARY KEY,
    account_id    BIGINT NOT NULL REFERENCES accounts(id),
    transfer_id   BIGINT REFERENCES transfers(id),
    amount_minor  BIGINT NOT NULL,
    note          VARCHAR(200),
    created_at    TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_postings_account_id ON postings(account_id);

-- Composite, and the column order is the whole point: equality columns first, then the range/sort column. This serves
-- "WHERE account_id = ? ORDER BY created_at DESC, id DESC" as a single index range scan with the rows already in the
-- right order, so the planner needs no Sort node on top. Reverse the columns and it cannot -
-- phase5_indexing.Phase5IndexingIT.CompositeOrderTests proves that against real query plans rather than asserting it.
--
-- DESC matches the query's ORDER BY. B-trees can be walked backwards, so this is not strictly required, but stating it
-- keeps the index and the query obviously aligned. The trailing id breaks ties on identical created_at, which is what
-- makes keyset pagination total rather than merely probable.
CREATE INDEX IF NOT EXISTS idx_postings_account_created_id
    ON postings (account_id, created_at DESC, id DESC);

-- Foreign keys are NOT indexed automatically in Postgres, unlike primary keys. Two consequences people get bitten by:
-- joins from the child side seq-scan, and - worse - deleting or updating a parent row has to scan the whole child table
-- to check the constraint, while holding a lock.
CREATE INDEX IF NOT EXISTS idx_postings_transfer_id ON postings (transfer_id);
CREATE INDEX IF NOT EXISTS idx_transfers_from_account_id ON transfers (from_account_id);
CREATE INDEX IF NOT EXISTS idx_transfers_to_account_id ON transfers (to_account_id);

-- Zero-amount postings are meaningless in a double-entry ledger - they are always a bug upstream, and a constraint says
-- so at the point of insert rather than leaving them to be puzzled over in a statement six months later.
--
-- DROP IF EXISTS + ADD in ONE statement, because this file is replayed on every context start
-- (spring.sql.init.mode=always) and ADD CONSTRAINT has no IF NOT EXISTS in any Postgres version. The obvious
-- alternative - wrapping it in a DO $$ ... EXCEPTION WHEN duplicate_object $$ block - does not work here: Spring's
-- ScriptUtils splits this file on semicolons and does not understand dollar quoting, so it chops the block at the first
-- internal `;` and Postgres reports an unterminated dollar quote. One statement sidesteps that entirely, and has the
-- better property anyway: editing the predicate below actually takes effect, whereas the DO-block form would silently
-- keep the old definition forever.
ALTER TABLE postings
    DROP CONSTRAINT IF EXISTS postings_amount_nonzero,
    ADD  CONSTRAINT postings_amount_nonzero CHECK (amount_minor <> 0);

-- Written in the SAME transaction as the postings above (see TransferService). A separate OutboxRelay polls unpublished
-- rows and "publishes" them (logged here rather than to a real broker - the point of this project is the atomicity
-- guarantee, not a message broker).
CREATE TABLE IF NOT EXISTS outbox (
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID NOT NULL UNIQUE,
    payload     TEXT NOT NULL,
    published   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

-- A PARTIAL index: only the rows matching the WHERE clause are indexed at all. The relay only ever asks for unpublished
-- rows, and that set stays tiny while the table grows forever - so this index stays proportional to the backlog rather
-- than to history, and the planner can use it precisely because the query's predicate matches the index's. Same trick
-- below for the job queue.
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (id) WHERE NOT published;

-- A job queue. Multiple JobRunner instances claim rows with SELECT ... FOR UPDATE SKIP LOCKED so they never block on -
-- or double-claim - a row another instance already grabbed.
CREATE TABLE IF NOT EXISTS payment_jobs (
    id          BIGSERIAL PRIMARY KEY,
    payload     VARCHAR(200) NOT NULL,
    status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_payment_jobs_pending ON payment_jobs (id) WHERE status = 'PENDING';

-- Balance snapshots. The ledger stays the source of truth; this is a cache that can always be recomputed and audited
-- against it. Reading a balance as SUM(postings) is O(rows for that account), which is fine at 100 rows and unusable at
-- 10 million - so a periodic job records "as of posting N the balance was X" and the read becomes snapshot +
-- SUM(postings newer than N).
CREATE TABLE IF NOT EXISTS account_balance_snapshots (
    account_id       BIGINT PRIMARY KEY REFERENCES accounts(id),
    as_of_posting_id BIGINT NOT NULL,
    balance_minor    BIGINT NOT NULL,
    taken_at         TIMESTAMP NOT NULL DEFAULT now()
);

-- Double-entry enforced by the database rather than by construction: every posting belonging to a transfer must,
-- together with its siblings, sum to zero.
--
-- The body is a SINGLE-QUOTED string, not the usual $$ ... $$ dollar quoting. Spring's ScriptUtils splits this file on
-- semicolons and does not understand dollar quotes, so a $$-quoted body gets chopped at the first internal `;` and
-- Postgres reports an unterminated dollar quote. It does track single quotes, so the old-style literal survives intact
-- - at the cost of doubling every quote inside.
--
-- The NULL check on the first line is load-bearing: phase1_isolation posts standalone debits with transfer_id NULL (the
-- write-skew demo), and transfer_id is nullable by explicit design. Without it, Phase1IsolationIT and
-- JointOverdraftService break immediately.
CREATE OR REPLACE FUNCTION assert_transfer_balanced() RETURNS trigger LANGUAGE plpgsql AS
'DECLARE
    total BIGINT;
BEGIN
    IF NEW.transfer_id IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT COALESCE(sum(amount_minor), 0) INTO total FROM postings WHERE transfer_id = NEW.transfer_id;
    IF total <> 0 THEN
        RAISE EXCEPTION ''transfer % does not balance: its postings sum to %'', NEW.transfer_id, total
            USING ERRCODE = ''check_violation'';
    END IF;
    RETURN NULL;
END';

-- DEFERRABLE INITIALLY DEFERRED is the entire point. This fires at COMMIT, not at INSERT: at statement time a transfer
-- is half-written and legitimately unbalanced, so a non-deferred check would reject every transfer ever made. A plain
-- CHECK constraint cannot express this at all - CHECK sees one row.
--
-- DROP + CREATE rather than a duplicate_object guard, because this file replays on every startup and the guard would
-- silently keep a stale definition after an edit. It takes a brief ACCESS EXCLUSIVE lock at startup, which is itself a
-- live example of the zero-downtime-migration material.
DROP TRIGGER IF EXISTS postings_transfer_balanced ON postings;
CREATE CONSTRAINT TRIGGER postings_transfer_balanced
    AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_transfer_balanced();
