# Design: moving money between accounts

*"Design a system that moves money between accounts, at 10k transfers/sec, that never loses or duplicates a payment."*

The three modules in this repo are vertical slices of this system. This document is the horizontal story that ties them
together — and every claim below points at code that exists and a test that proves it, because a design you have
actually built is a different conversation from one you have only drawn.

Read it as a 45-minute whiteboard: clarify, sketch, then spend most of the time on what happens when it breaks.

---

## 1. Clarify first

Interviewers grade the questions as much as the answer. The ones that change the design:

- **Internal ledger transfers, or external rails?** Internal means one database and a real transaction. External means
  you cannot roll back the outside world, and everything below about reversals and reconciliation becomes load-bearing.
- **Single currency?** Multi-currency means FX rates, which means a `NUMERIC` somewhere and a decision about who bears
  rounding.
- **Synchronous confirmation, or async?** "Your transfer is complete" on the HTTP response is a very different system
  from "we've accepted it".
- **What consistency does a balance read need?** Read-your-writes for the payer is usually non-negotiable; everyone else
  can tolerate lag.
- **What's the actual failure budget?** "Never loses a payment" is a durability requirement. "Never duplicates" is an
  idempotency requirement. They have different solutions and people conflate them.

Assume for the rest of this: internal ledger, single currency, async confirmation, read-your-writes for the payer.

---

## 2. Idempotency at the edge

`POST /transfers` with a caller-supplied `Idempotency-Key` header. Store `(caller, key) → request_hash, status,
response`.

- Same key, same body → **replay the stored response**. Not "do it again".
- Same key, **different** body → **reject**. The client has a bug, and silently serving either result is worse than an
  error. Stripe returns **400** with an `idempotency_error` — *"keys for idempotent requests can only be used with the
  same parameters they were first used with"*. That case is the detail that separates people who have implemented this
  from people who have read about it.
- Same key, first request **still in flight** → you need a `PENDING` state and a retry-after, not just success/failure.
  The naive two-state design deadlocks the client into either double-submitting or giving up.

**The guarantee is the unique constraint, not the check.** Two concurrent requests can both pass a "have I seen this
key?" read; only one can commit the insert. → `postgres/phase2_ledger`, proved by `IdempotencyIT`.

---

## 3. The ledger

**Append-only double-entry.** A transfer writes two rows summing to zero: a debit and a credit. Nothing is ever updated,
so there is no lost-update problem and the audit trail is the data structure rather than a thing bolted onto it.

**Balance is a projection**, `SUM(postings)`, never a column. → `postgres/phase2_ledger`.

**Money is an integer count of minor units** (`BIGINT`). Never floating point — `0.1 + 0.2` is not `0.3` and a bank
cannot ship that. `NUMERIC` when you need fractional minor units or FX rates.

**Enforced in the database, not just by convention.** A deferred constraint trigger asserts every transfer's postings
sum to zero, checked at COMMIT rather than at INSERT — because halfway through writing a transfer the ledger is
legitimately unbalanced. → `postgres/phase6_operations`, proved by `DeferredBalanceConstraintIT`.

**Reads stay fast via snapshots.** `SUM(postings)` is O(rows for that account), which is fine at a hundred and unusable
at ten million. A periodic job records "as of posting N the balance was X" and reads become snapshot + delta. The
journal stays the source of truth, so the snapshot can always be recomputed and audited — **a bad snapshot is a
performance bug, not a correctness one.** → `postgres/phase6_operations`, proved by `SnapshotIT`.

---

## 4. Concurrency control

**Two transfers touching the same two accounts in opposite directions** is the classic deadlock. Fix by always locking
in a deterministic order — ascending account id — which makes circular wait structurally impossible. →
`concurrency/phase2_deadlock`, and `NaiveTransferServiceDeadlockTest` proves the unordered version genuinely deadlocks
using the JVM's own detector.

**In a real system the lock is the database's**, not the JVM's: `SELECT … FOR UPDATE` in ascending id order, because a
JVM lock does not survive two instances of the service. → `postgres/phase1_isolation`.

**Some invariants are not expressible as a row lock.** A shared overdraft limit across two accounts is **write skew**:
each transaction reads a predicate the other is about to invalidate, and neither writes a row the other read — so a row
lock would not have helped even if you had taken one. Fix with SERIALIZABLE (SSI detects the dependency and aborts one
with `40001`) plus a retry, or materialise the conflict onto a single lockable row. → proved by `WriteSkewIT`.

**And the database breaks its own deadlocks**, unlike the JVM: `deadlock_timeout`, a wait-graph check, and one victim
killed with `40P01`. So `40P01` and `40001` belong in the same retry loop. → proved by `PgDeadlockIT`.

**The real bottleneck is a hot account**, not the lock strategy. Ordering does not fix contention on one row. Options:
optimistic concurrency with retry; per-account queueing so one account's traffic serialises without blocking anyone
else; or sharding by account.

---

## 5. Async fan-out

The debit is a database commit; telling anyone about it is a network call. There is no transaction spanning both — **the
dual-write problem** — and it has no solution at the point of the write, only a displacement.

**Transactional outbox.** Write the outgoing event into the *same database*, in the *same transaction* as the debit. A
relay publishes from there. One atomic write, no window. → `kafka/payment`, proved by `OutboxIT` and
`OutboxRedeliveryIT`.

Downstream consumers each get their own guarantees from the same stream: notifications can be lossy, fraud must be
timely, reporting can lag, reconciliation must be complete. **Picking different guarantees per consumer is the point,
not an inconsistency.**

**The relay is at-least-once**, so the downstream must be idempotent too. Idempotency does not disappear when you add an
outbox; it moves. Say that out loud — it is the staff-level version of the answer.

**Ordering has a limit worth naming.** Keying by `accountId` gives per-account ordering — until a retry topic breaks it,
because a failed event goes to the retry topic while the next one keeps flowing. → proved by `OrderingUnderRetryIT`,
with the three ways out in `kafka/README.md`.

---

## 6. Reconciliation

Matching your ledger against the external rail's statement, continuously.

**Reconciliation is a product requirement, not a background job.** Someone has to see a break, and there has to be a
state for "we think this happened and they don't". Model breaks explicitly — `PENDING`, `CONFIRMED`, `ROLLBACK` — rather
than treating a mismatch as an exception.

The consumer reads both topics and matches the two halves of each payment by `paymentId`, in either arrival order. →
`kafka/reconciliation`.

---

## 7. What happens when it breaks

**This is the part that is actually graded.** Each of these has an answer in the repo rather than a hand-wave.

| Failure | What happens | Where |
|---|---|---|
| Process dies after the DB commit, before the publish | Offset uncommitted, event redelivered, idempotency check declines to re-debit — and the outbox row is still pending, so the relay publishes it. Nothing is lost. | `OutboxRedeliveryIT` |
| Broker unavailable for 10 minutes | Outbox rows accumulate; the relay retries every 500ms and drains when it returns. Alert on **oldest unpublished row**, not queue depth. | `PaymentOutboxRelay` |
| Consumer processes the same event twice | `processed_events` primary key. The constraint is the guarantee; the check is the optimization. | `IdempotencyIT` |
| A consumer is slow and gets evicted mid-batch | Rebalance, partitions reassigned, uncommitted records redelivered to whoever picks them up — survivable *only* because the consumer is idempotent. | `RebalanceIT` |
| Downstream rail times out with **no response** | **The worst case**: you do not know whether it happened. You cannot retry blindly and you cannot assume failure. You need a *query* API on the rail, or a reversal — and a `PENDING` state to sit in meanwhile. | design-only |
| A replica lags and the payer sees a stale balance | Route read-your-writes to the primary, or pin the session to the primary for a window after a write. | design-only |
| A bad deploy poisons the DLT | DLT depth should alert at **zero** — every record is a customer's money. Records carry original topic/offset/exception for replay after the fix. | `DeadLetterIT` |
| A long transaction is left open | Vacuum cannot reclaim anything newer than the oldest snapshot, so the table bloats even though rows are being deleted. `idle_in_transaction_session_timeout`. | `LongTxnBloatIT` |

---

## 8. Scale

**Shard by `account_id`.** Most transfers are within a shard; the ledger for one account stays in one place, which keeps
the common path a single transaction.

**Cross-shard transfers need a saga**, not 2PC: reserve → commit → compensate. Two-phase commit across shards is a
liveness hazard — a coordinator failure holds locks on every participant until someone intervenes, which is exactly the
availability you were sharding to get.

**A compensating "refund" is a new ledger entry, never an erasure.** The journal is append-only; correcting an error
adds a row that says so. This matters legally as much as technically.

**Partition the ledger by time** once it is large: range partitioning on `postings(created_at)` by month, so old
partitions can be dropped rather than `DELETE`d — a delete of millions of rows just creates bloat.

**Where the throughput actually goes.** 10k transfers/sec is 20k posting inserts plus an outbox row. That is comfortable
for one Postgres on decent hardware *provided* the hot-account problem is handled and the connection pool is small — a
pool of 10 usually beats a pool of 100, because each connection is a Postgres *process*. Virtual threads make it easier
to hit that limit, not harder.

---

## What this repo does not implement

Naming the boundary is part of the answer:

- **No external rails.** Everything here is an internal ledger, so settlement, network timeouts and reversals are
  discussed but not built.
- **No sharding.** One database, one node.
- **No schema registry.** JSON with trusted packages, fine for a demo; production is Avro/Protobuf plus compatibility
  modes.
- **No exactly-once Kafka transactions.** Deliberate — EOS covers Kafka→Kafka only, and the instant you touch Postgres
  you are outside it. The outbox is the honest answer for this shape of system.

---

## Where to look

| Section | Module |
|---|---|
| Idempotency at the edge | [`postgres/phase2_ledger`](postgres/README.md) |
| The ledger, snapshots, deferred constraints | [`postgres/phase2_ledger`, `phase6_operations`](postgres/README.md) |
| Concurrency control | [`concurrency/phase2_deadlock`](concurrency/README.md), [`postgres/phase1_isolation`](postgres/README.md) |
| Indexes and query plans | [`postgres/phase5_indexing`](postgres/README.md) |
| Async fan-out, outbox, ordering | [`kafka/payment`](kafka/README.md) |
| Reconciliation | [`kafka/reconciliation`](kafka/README.md) |
| Live-coding primitives, the JMM | [`concurrency/phase7_primitives`, `phase8_memorymodel`](concurrency/README.md) |
