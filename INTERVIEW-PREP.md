# Interview Prep — gap analysis and depth guide

Companion to the three module READMEs. Those explain **what the code does**.
This one is about **what is still missing**, ranked, with the concrete thing to
build or the concrete sentence to be able to say.

Written against the repo as of `225376a` — Spring Boot 3.5.6, JDK 25,
`postgres:16-alpine` and `apache/kafka:3.9.1` in Testcontainers.

**On evidence.** Every "the repo doesn't have X" claim below was checked by
grepping the source, not by reading the module READMEs — those are persuasive
enough to fool you into thinking something is implemented when it's only
described. Three claims in the first draft of this file failed exactly that way
(`spring.threads.virtual.enabled` is set; `KafkaTopicConfig`'s javadoc already
covers partition-count sizing; `LockOrderedTransferServiceTest` doesn't do what
I said it did). Anything without a file:line is my judgement, not a verified
fact, and should be treated that way.

**No link list, deliberately.** External claims here — version numbers, JEP and
KIP numbers, release dates — were checked against primary docs when this was
written, and a saved list of URLs would only rot into the next wrong-version
claim. Look anything external up fresh at the source (openjdk.org/jeps,
kafka.apache.org, postgresql.org/docs) rather than trusting this file's
recollection of it.

---

## Status — what has since been closed

This document drove a round of work on the repo. Closed since it was written,
each with tests:

| § | Item | Now |
|---|---|---|
| 1.1 | Consumer acks before the publish is confirmed | Fixed — transactional outbox in the kafka module (`OutboxIT`, `OutboxRedeliveryIT`) |
| 1.2 | `retries: 3` weakens the idempotent producer | Fixed — `delivery.timeout.ms` |
| 1.3 | Wrong JEP number | Fixed — 499 → 505 in all four sites |
| 1.4 | Postgres version drift | Bumped 16 → 18 across all three modules |
| 2.2 | [P0] Live-coding classics missing | `phase7_primitives` — rate limiters, LRU, `Condition` buffer, pool |
| 2.2 | [P0] JMM used but never stated | `phase8_memorymodel` |
| 2.2 | [P1] ABA, [P1] `LongAdder` | `AbaProblemDemo`, `CounterContention` |
| 2.3 | Deterministic deadlock test | `NaiveTransferServiceDeadlockTest` via `ThreadMXBean` |
| 3.2 | [P0] Rebalancing not covered | `RebalanceIT` + a README section |
| 3.2 | [P0] Retry topic breaks ordering | `OrderingUnderRetryIT` — proved, not just named |
| 4.2 | [P0] Indexes and query plans — "the single largest hole" | `phase5_indexing`, plans asserted via `EXPLAIN … FORMAT JSON` |
| 4.2 | [P0] Balance is `SUM(postings)` forever | `phase6_operations` — snapshots |
| 4.2 | [P1] Enforcing debits = credits in the DB | Deferred constraint trigger |
| 4.2 | [P1] Keyset pagination | Query + endpoint + `KeysetPaginationIT` |
| 4.2 | [P1] Connection pooling | First HikariCP config, with the sizing rationale |
| 4.2 | [P1] Postgres-level deadlocks | `PgDeadlockIT` (`40P01`) |
| 5 | CI | `.github/workflows/build.yml` + badge |
| 5 | Testing philosophy written down | Root README |
| 6 | System design narrative | `DESIGN.md` |

**Three places this document was wrong**, found by building it:

1. §2.3's `UnsafePublication` "run it a million times and it fires" — it doesn't,
   on x86. 200k reads, zero sightings. The phase now asserts the guarantee and
   only reports the anomaly, which is a better lesson than the test would have been.
2. §2.3's mirror `assertThat(deadlocked).isNull()` — `findDeadlockedThreads()` is
   JVM-global and Surefire shares one fork, so it fails or passes on class
   ordering. Both assertions are now scoped to their own thread ids.
3. §4.3's raw DDL — `schema.sql` replays on every startup, and Spring's
   `ScriptUtils` splits on semicolons without understanding dollar quoting, so
   `DO $$ … $$` blocks and `$$`-quoted function bodies are chopped in half.

Still open: §3.2's EOS/log-compaction/schema-evolution material, §4.2's
partitioning and replication, §5's observability section, and the Kafka 4.x bump
that would make KIP-848 and share groups demonstrable rather than talking points.

---

**How to use it:** each topic section has the same four parts —

1. **Already defensible** — what you can claim today without hedging.
2. **Gaps, ranked** — what an interviewer probes that the repo has no answer for.
   `[P0]` = would visibly hurt you, `[P1]` = separates mid from senior,
   `[P2]` = nice-to-have depth.
3. **Build this** — small, concrete additions that turn a gap into a demo.
4. **Depth ladder** — the same question answered at mid / senior / staff level.

Don't try to close everything. The P0s in §1 and §2, plus §6 (system design),
is the highest-value path.

---

## 0. The rounds, and which module feeds which

Wise/Revolut-shaped loops run roughly: recruiter screen → online assessment →
**live coding (45–60 min)** → **system design (60 min)** → take-home /
architecture review → behavioural + bar raiser. Revolut's live-coding round in
particular expects *production-quality* code — thread-safe, SOLID, **with unit
tests** — inside the hour, and its design round is graded on operational
workflow (what happens when it breaks) more than on box-drawing.

| Round | What it actually tests | Module that feeds it | Repo's coverage |
|---|---|---|---|
| Online assessment | DSA under time pressure | none | **not covered** — that's the sibling `DSA/` repo's job |
| Live coding | write a correct thread-safe class from scratch, with tests, while narrating | `concurrency` phases 1–2, 5–6 | strong on *concepts*, thin on the **classic 45-min tasks** (§2) |
| System design | money movement, failure modes, idempotency, reconciliation | all three | the pieces exist, **the design narrative doesn't** (§6) |
| Take-home / architecture review | judgment, trade-offs written down | all three | this repo *is* a take-home-quality artifact — see §7 |
| Deep-dive / bar raiser | "why is that line there", incident thinking | all three READMEs | strongest area; the READMEs are already written in interview voice |

The single biggest structural gap: **three deep vertical slices, no horizontal
design story tying them together.** §6 fixes that.

---

## 1. Fix these first — accuracy defects [P0]

These are things that are currently *wrong or misleading in the repo*.
Defending a reliability story that has a hole in it costs more than not covering
the topic at all.

### 1.1 The Kafka consumer commits its offset before the publish is confirmed

`PaymentConsumer.consume` (`kafka/.../PaymentConsumer.java:90-93`):

```java
.ifPresent(result -> kafkaTemplate.send(Topics.PAYMENT_RESULTS, ..., result));
ack.acknowledge();
```

`KafkaTemplate.send` is **asynchronous** — it returns a
`CompletableFuture<SendResult<..>>` and returns immediately, having only
enqueued the record in the producer's accumulator. The code discards that future
and acks the offset on the next line. If the broker rejects the send (or the
process dies before the accumulator is flushed), the offset is already committed
and **the result event is lost with no path to recovery** — no redelivery, no
DLT. (You do get an ERROR log: `KafkaTemplate` registers a
`LoggingProducerListener` by default, so the failure is *visible*. Nothing
*retries* it, which is the part that matters.)

That directly undercuts the README's headline claim ("commit the offset last,
never first"), and it's exactly the kind of thing a deep-dive round finds.

**The obvious fix doesn't actually work, and that's the interesting part:**

```java
// Option A — block on the send before acking. (Exception handling elided.)
kafkaTemplate.send(Topics.PAYMENT_RESULTS, key, result).get(5, SECONDS);
ack.acknowledge();
```

This narrows the window but does **not** recover the event. If the send times
out, the listener throws → `@RetryableTopic` redelivers → and
`PaymentProcessingService.process` hits its own idempotency check
(`PaymentProcessingService.java:46-49`), sees `processed_events` already
contains the id, and returns `Optional.empty()`. The debit is correctly not
re-applied — and the result is **never published**, because there's nothing left
to publish it from. The kafka README already describes this exact trap one
paragraph away ("the idempotency check correctly refuses to debit twice but also
doesn't republish the missing result") without connecting it to the `send` that
causes it.

So Option A only works if it's paired with making `process()` return the
*previously computed* result on a duplicate instead of `Optional.empty()` —
which means persisting the result alongside the `ProcessedEvent` row. At which
point you have built two-thirds of an outbox, so build the whole thing:

```java
// Option B — the one you'd actually defend. Write the result into an outbox
// table inside process()'s transaction; a relay publishes from there. One
// atomic write, and redelivery is harmless because the row already exists.
```

Option B is also better *for this repo specifically*: the postgres module
already has `OutboxRelay` and the kafka README already names the dual-write gap.
Wiring the two together (§3.3) closes the loop and gives you a much better story
than either module has alone.

**Say this one out loud in an interview even if you never fix it.** "Here's a
bug in my own code, here's why the obvious fix doesn't work, here's the one that
does" is a stronger three minutes than any clean design you can present.

### 1.2 `retries: 3` quietly weakens the idempotent producer

`kafka/src/main/resources/application.yml:28`. With `enable.idempotence: true`,
Kafka's own default is `retries = Integer.MAX_VALUE`, bounded in *wall-clock*
time by `delivery.timeout.ms` (120s default) rather than by an attempt count.
Pinning it to 3 means a partition leader election — a completely routine event —
can burn through the retries and surface as a send failure that would otherwise
have healed itself. (How fast depends on how each attempt fails: a
`LEADER_NOT_AVAILABLE` comes back immediately and spends all three attempts in
~300ms at the default `retry.backoff.ms`; an attempt that hangs until
`request.timeout.ms` takes 30 seconds a go. Either way the *attempt count* is
the wrong dial.)

The modern guidance is: **leave `retries` alone, tune `delivery.timeout.ms`.**
Say that out loud and you're immediately in the top decile of Kafka answers.

### 1.3 Wrong JEP number for structured concurrency (typo-level, fix and forget)

`README.md:65`, `concurrency/README.md:142`, `concurrency/pom.xml:22` and
`PaymentGatewayService.java:78` say **"JDK 25 (JEP 499)"**. JDK 25 is
**JEP 505**, the fifth preview; JEP 499 was the fourth, in JDK 24. Fix the four
sites — `concurrency/pom.xml:22` also says "fourth preview" and needs "fifth".
Nothing to study here.

### 1.4 Version drift worth being able to speak to

Not defects — but "why are you on X when Y shipped?" is a fair question, and
"I hadn't noticed" is the wrong answer.

| Repo pins | Current (Jul 2026) | The sentence to have ready |
|---|---|---|
| Spring Boot 3.5.6 | **4.0** (Nov 2025) | Boot 4 = Jackson 3, JSpecify null-safety annotations, modularised autoconfigure, first-class API versioning, built-in retry/concurrency-limiting. JDK 17 baseline retained. |
| `postgres:16-alpine` (all **three** `TestContainerConfig`s — concurrency has one too) | **18** (Sep 2025) | 17 = much cheaper vacuum (new dead-tuple store), incremental backup. 18 = async I/O (`io_method`, io_uring on Linux), `uuidv7()`, B-tree **skip scan**, virtual generated columns, OAuth. |
| JDK 25 | 25 is the LTS; 26 shipped Mar 2026 | Staying on the LTS is the right call — say it as a decision, not a default. |
| **`apache/kafka:3.9.1`** (`kafka/.../TestContainerConfig.java:33`) | **4.2** | This one constrains you rather than just dating you. Kafka 4.0 dropped ZooKeeper entirely and made **KIP-848** generally available (broker-side by default; clients opt in with `group.protocol=consumer` — classic is still the client default); 4.2 shipped **KIP-932** share groups. On 3.9 you can demo neither, so the §3.2 material stays talking points unless you bump the image. |

---

## 2. Concurrency

### 2.1 Already defensible

Genuinely strong and unusually complete for a prep repo: lost updates and the
read-modify-write hazard; `synchronized` vs CAS retry loops; Coffman conditions
and global lock ordering; `tryLock` with a bounded timeout; virtual threads
including the "unlimited threads ≠ unlimited concurrency" distinction (rare —
most candidates stop at "they're cheap"); three fan-out styles compared on
cancellation semantics, with the `CompletableFuture.cancel(true)` doesn't-
interrupt trap called out correctly; string-literal interning as an accidental
shared lock, and lock striping as the fix; static vs instance monitors;
double-checked locking and why `volatile` is load-bearing, plus the holder
idiom; carrier-thread pinning; `ScopedValue` vs `ThreadLocal`; `wait` in a
`while` loop and `notifyAll` vs `notify`.

The `ExecutorCompletionService` point — that `get()`-ing futures in submission
order is *not* fail-fast — is the kind of detail most senior candidates get
wrong. Lead with it.

### 2.2 Gaps, ranked

**[P0] The classic live-coding tasks aren't here.** The round is "implement X,
thread-safe, with tests, in 45 minutes". The repo does build two things from
scratch in that shape — a round-robin load balancer and `TellerQueue`, a bounded
buffer on `wait`/`notify` — but not the tasks that actually get asked at
fintechs:

- **Rate limiter** — token bucket and sliding window. Asked constantly.
- **Thread-safe LRU cache** — `LinkedHashMap` + lock, then "now make it
  concurrent" → segmented / `ConcurrentHashMap` + `ConcurrentLinkedDeque`, then
  "now add TTL".
- **Bounded blocking queue** — you have the `wait/notify` version
  (`TellerQueue`); you don't have the `ReentrantLock` + two `Condition`s version,
  which is the expected follow-up (§2.3).
- **Connection/object pool** with borrow-timeout and return-on-close.
- **Concurrent task scheduler / delay queue**.
- **Read-write cache with `StampedLock`** optimistic reads.

**[P0] The Java Memory Model is used but never stated.** `volatile` appears in
`ExchangeRateService` with a correct explanation, but there's no articulation of
**happens-before** itself: program order, monitor release→acquire, volatile
write→read, `final`-field freeze at constructor exit, `Thread.start()` and
`join()`, and what **safe publication** means. "Why is a `final` field visible
without synchronization but a non-final one isn't?" is a standard senior probe
and the repo has nothing on it.

**[P1] Thread-pool sizing and `ThreadPoolExecutor` internals.** Nothing on
`corePoolSize` / `maximumPoolSize` / queue interaction — including the classic
trap that **an unbounded `LinkedBlockingQueue` makes `maximumPoolSize` dead
code**, because the pool only grows past core size when the queue rejects. Also
missing: rejection policies (`CallerRunsPolicy` as crude backpressure), the
sizing heuristic `N = N_cpu × U × (1 + W/C)`, and Little's law.

**[P1] `LongAdder` vs `AtomicLong`.** The concurrency README says "for a single
counter, `AtomicLong` usually beats a lock". True, and incomplete — under heavy
write contention `AtomicLong`'s CAS loop degrades badly and `LongAdder` (striped
cells, summed on read) wins by an order of magnitude. The trade-off: `sum()` is
not atomic w.r.t. concurrent updates, so it's for metrics, not for balances.
That distinction — *why you'd use `LongAdder` for a request counter and never
for an account balance* — is a great answer in a bank context.

**[P1] The ABA problem.** `AtomicAccount`'s CAS retry loop is the perfect setup,
and the follow-up is always "when does CAS give you the wrong answer even though
it succeeded?" → ABA, and `AtomicStampedReference` / `AtomicMarkableReference`.
With a `long` balance it's benign; with a popped-and-repushed node reference it
isn't.

**[P1] Livelock, starvation, fairness.** Deadlock is covered thoroughly;
its siblings aren't. `new ReentrantLock(true)` (fair mode) and why you almost
never want it — fairness serialises handoffs and destroys throughput.

**[P1] Interrupt policy.** `phase5`'s `InterruptibleAction` turns out to be a
bare six-line `@FunctionalInterface` with no javadoc at all — the only file in
the module that doesn't explain itself. It's a lambda type, not a lesson. The
rule it hints at is never stated: **either propagate `InterruptedException`, or
restore the flag with `Thread.currentThread().interrupt()` — never swallow it**,
because swallowing it destroys the only signal the thread has that someone asked
it to stop. Plus the shutdown protocol: `shutdown()` →
`awaitTermination(timeout)` → `shutdownNow()` → await again.

**[P2] `ForkJoinPool` and the common pool.** Work stealing, and the sharp edge
that `parallelStream()` runs on the shared `ForkJoinPool.commonPool()` — one
blocking call in a parallel stream starves every other parallel stream in the
JVM. Very common in real incidents.

**[P2] False sharing** and `@Contended`; cache-line effects on padded counters.

**[P2] Deeper virtual-thread mechanics.** Carrier pool sizing
(`jdk.virtualThreadScheduler.parallelism`, defaults to available processors);
why VTs do nothing for CPU-bound work; the `ThreadLocal` memory footprint when
you have a million of them; and — the honest nuance the `PinningDemo` javadoc
should carry — JDK 24's JEP 491 removed pinning for `synchronized`, but **native
(JNI/FFM) frames still pin, and so does class initialization** (blocking inside
a `<clinit>`, or waiting on another thread's). "Pinning is solved" is too
strong; "pinning by `synchronized` is solved" is the accurate sentence.

**[P2] Testing concurrency for real.** Stress loops prove absence of a race
only probabilistically. Worth knowing: `ThreadMXBean.findDeadlockedThreads()` (a
*deterministic* deadlock assertion — see §2.3), jcstress for memory-model tests,
and how to read a thread dump / JFR recording.

**[P2] Spring-side concurrency.** Two things here are already done and one is
missing — know which is which, because both are things to *point at*, not fix:

- `spring.threads.virtual.enabled: true` is **already set**
  (`concurrency/src/main/resources/application.yml:4-6`), so that module's
  Tomcat already serves requests on virtual threads. Nothing in the module
  explains or demonstrates it, which is the actual gap — a one-line README note
  turns an invisible config into an answer to "how would you adopt virtual
  threads in an existing Spring app?"
- `@Transactional` **self-invocation** is handled correctly by splitting
  `*TransactionalOps` classes out — but in the **postgres** module
  (`JointOverdraftTransactionalOps`, `TransferTransactionalOps`), not the
  concurrency one. It's a strength nobody will notice unless you point at it.
- `@Async` and its executor genuinely aren't covered anywhere.

### 2.3 Build this

**`phase7_primitives/`** — the live-coding classics, same house style (one class,
one test that proves the property, javadoc that says why):

```
TokenBucketRateLimiter    — refill by elapsed nanos, not a timer thread; test
                            proves burst-then-throttle and steady-state rate
SlidingWindowRateLimiter  — the "why isn't fixed-window enough" follow-up
ConcurrentLruCache        — capacity eviction under concurrent get/put; test
                            proves no lost entries and correct eviction order
BoundedBufferWithCondition— TellerQueue rewritten on ReentrantLock + notFull /
                            notEmpty Conditions. THE point: signal() on a
                            targeted Condition wakes only the right kind of
                            waiter, so the notifyAll thundering-herd from
                            phase6 disappears. Diff the two files in the
                            interview — that comparison is the whole answer.
BorrowablePool            — borrow with timeout, return in finally, close()
```

**`phase8_memorymodel/`** — the JMM made observable:

```
UnsafePublication  — a non-final field read as its default value from another
                     thread; run it a million times and it fires
FinalFieldFreeze   — the same object with a final field, which cannot
SafeCounterVisibility — a plain long that never converges vs a volatile one
```

**A deterministic deadlock test** — this is a genuine differentiator, and it's
about 20 lines. Instead of asserting "the fixed version didn't hang", assert
that the *broken* version genuinely deadlocks:

```java
// phase2_deadlock: prove the trap is real, not just that the fix is fast.
var broken = new NaiveTransferService();   // locks from-then-to, no ordering
// ...start two threads doing A→B and B→A...
long[] deadlocked = ManagementFactory.getThreadMXBean().findDeadlockedThreads();
assertThat(deadlocked).isNotNull();        // the JVM itself says so
```

Then add the mirror assertion — the same probe returns `null` — to
`LockOrderedTransferServiceTest`, which today asserts only
`assertTimeoutPreemptively(Duration.ofSeconds(15), ...)` plus balance
conservation, and never touches `ThreadMXBean`. You go from "my test times out
if it regresses" to "the JVM's own deadlock detector confirms my analysis" —
much stronger, and it's the difference between a test that hangs your build and
a test that names the bug.

**Add to `phase6`:** a `LongAdder` vs `AtomicLong` contention benchmark, and an
`AtomicStampedReference` ABA demo next to `AtomicAccount`.

### 2.4 Depth ladder

**Q: "Why is `volatile` not enough for `count++`?"**
- *Mid:* volatile gives visibility, not atomicity; `++` is read-modify-write.
- *Senior:* + the JMM framing — volatile establishes happens-before between the
  write and subsequent reads, which fixes *staleness*; it does nothing about two
  threads interleaving three bytecodes. Fix with `AtomicLong` (CAS) or a lock.
- *Staff:* + when you'd pick which. `AtomicLong` for low-to-moderate contention;
  `LongAdder` when the counter is hot and reads are rare (metrics); a lock when
  the invariant spans more than one field and can't be expressed as one CAS —
  which is why `AtomicAccount.withdraw` needs a retry loop and a multi-account
  transfer needs a lock.

**Q: "Two threads transfer money between the same two accounts in opposite
directions."**
- *Mid:* deadlock; fix by always locking in the same order.
- *Senior:* + names circular wait as one of four Coffman conditions, and notes
  ordering breaks *that specific one* while contention remains. Adds `tryLock`
  with a timeout so an unexpected hold fails fast instead of hanging a request
  thread, and releases in `finally`.
- *Staff:* + pushes it down a layer: in a real system the lock is the database's
  (`SELECT … FOR UPDATE` in ascending id order), because the JVM lock doesn't
  survive two instances of the service. Then names what ordering *doesn't* fix
  — lock convoys on a hot account — and reaches for optimistic
  concurrency + retry, or per-account queueing/sharding, when contention on one
  row becomes the bottleneck.

**Q: "You have 10 000 concurrent requests, each calling three downstream APIs."**
- *Mid:* virtual threads; one per task, don't pool them.
- *Senior:* + separates the two jobs the old fixed pool was doing. Virtual
  threads solve "don't create too many expensive threads"; they do nothing for
  "don't overload the downstream". Bound the *bottleneck* with a
  `Semaphore(200)` around the outgoing call, not the thread count.
- *Staff:* + what happens at the limit: queueing at the semaphore is unbounded
  latency, so add a timeout and shed load rather than queue forever; a circuit
  breaker so a dead dependency fails fast instead of consuming the permit pool;
  and the observation that 10 000 virtual threads each holding a JDBC connection
  is still 10 000 connections — the pool is the real limit, and virtual threads
  make it *easier* to hit.

### 2.5 Live-coding drill protocol

Run these against a 45-minute timer, out loud, writing tests first:

1. Thread-safe bank account with `withdraw` (check-then-act) — 15 min.
2. Token-bucket rate limiter — 25 min.
3. Bounded blocking queue, `wait/notify` — 25 min. Then: *"now with
   `Condition`s"* — 10 min.
4. Thread-safe LRU cache — 30 min. Then: *"now add TTL"*.
5. Transfer service with deadlock-free multi-account locking — 30 min.

Rules that mirror the real round: narrate the invariant *before* writing the
lock; write the failing concurrency test first; never leave a `catch
(InterruptedException)` empty; always release in `finally`.

---

## 3. Kafka

### 3.1 Already defensible

The reliability narrative is the strongest part of the repo: manual immediate
acks with the offset committed last; the idempotent *consumer* via a
primary-keyed `processed_event` row written in the same transaction as the debit
— including the sharp framing that **the constraint is the guarantee, the check
is the optimization**; the explicit separation of consumer idempotency from
producer idempotency (people conflate these constantly); "rejected is not
failed", so business declines don't pollute the DLT; non-blocking retries via
`@RetryableTopic` and *why* in-listener sleeping trips `max.poll.interval.ms`;
DLT records carrying original topic/offset/exception; `acks=all` being
meaningless without `min.insync.replicas=2`. Naming the dual-write gap instead
of pretending it isn't there is a mature move — keep it.

### 3.2 Gaps, ranked

**[P0] Rebalancing.** Not mentioned anywhere, and it's the #1 Kafka
operational question. You need cold:

- `heartbeat.interval.ms` (liveness, on a background thread) vs
  `session.timeout.ms` (how long before the coordinator declares you dead) vs
  `max.poll.interval.ms` (how long your *processing* may take before you're
  evicted). Three timeouts, three different failures.
- **Eager** (stop-the-world: everyone revokes everything, then reassigns) vs
  **cooperative-sticky** (incremental, only moved partitions revoked).
- **Static group membership** (`group.instance.id`) — a rolling restart no
  longer triggers a rebalance per pod.
- `ConsumerRebalanceListener.onPartitionsRevoked` — commit before you lose the
  partition, or you reprocess.
- **KIP-848**, the next-generation consumer group protocol: assignment moves
  off the clients and into the broker-side group coordinator, so rebalances
  become incremental and stop being stop-the-world. GA in Kafka **4.0** —
  brokers support it out of the box, but `classic` is still the *client*
  default, so a consumer opts in with `group.protocol=consumer`.
  Mentioning this in 2026 marks you as someone who tracks the ecosystem rather
  than reciting a 2019 blog post about eager rebalancing.

**[P0] Your own retry topic breaks your own ordering guarantee.** The README
claims per-account ordering (keyed by `accountId`, proved by `OrderingIT`) *and*
non-blocking retries via `@RetryableTopic`. Those are in direct tension: when
record #5 for account A fails and is republished to `payment-events-retry-0`,
records #6 and #7 for account A keep flowing on the main topic and are processed
*first*. Per-key ordering is gone precisely in the failure case where it matters.

There is no free fix — you pick:
- accept it, and make the downstream commutative/idempotent (fine for a debit
  keyed on an event id; not fine for a state machine);
- block-and-retry in place, accepting head-of-line blocking (with a
  `max.poll.interval.ms` budget);
- pause the partition (`Consumer.pause()`) and retry with backoff, preserving
  order at the cost of throughput for that partition only.

**Being the candidate who spots the tension in their own design is worth more
than a design with no tension in it.** Put this in the kafka README.

**[P2] Partition count is a one-way door — already covered, know that it is.**
`KafkaTopicConfig.java:18-29` already documents this in detail: you can add
partitions but never remove them, adding them re-maps `hash(key) % count` so
keys move and per-account ordering breaks across the boundary, hence "size for
maximum expected parallelism plus 20-30% headroom", and that 3 is a demo number
where a real payments topic would be 24–48. Don't rehearse this as a gap. The
only thing missing is the **mitigation menu**: a custom `Partitioner` with
consistent hashing so adding partitions moves a minority of keys, or an explicit
drain-and-migrate to a new topic.

**[P1] Transactions / EOS, properly.** The README correctly says EOS is
Kafka-to-Kafka only, but you should be able to explain the machinery when
pushed: `transactional.id` → transaction coordinator → `__transaction_state` →
commit markers written into the partitions → consumers with
`isolation.level=read_committed` only read up to the **LSO** (last stable
offset) → **zombie fencing** by producer epoch, so a partitioned-off old
instance can't write after a new one took over. And `sendOffsetsToTransaction`
for read-process-write.

**[P1] Log compaction.** Absent, and it's the natural answer to half of the
design questions in this domain: `cleanup.policy=compact` keeps the latest value
per key forever, `null` value = **tombstone** = delete. That's how you ship an
"account balance snapshot" topic or make a service's state rebuildable from
Kafka alone. Pair with `delete.retention.ms` and why compaction is eventual, not
immediate.

**[P1] Schema evolution.** The module uses `JsonSerializer` +
`spring.json.trusted.packages` — fine for a demo, and you should say so, then
say what production does: Avro/Protobuf + Schema Registry, and the compatibility
modes (BACKWARD lets new consumers read old data — the common default; FORWARD
the reverse; FULL both). The concrete rule: **adding an optional field with a
default is safe; renaming or removing a required field is not.**

**[P1] Consumer lag as the operational signal.** How you'd alert (lag by
partition, not aggregate — one stuck partition hides in an average), what you do
when it grows (is it a slow consumer, a hot partition, or a poison record?),
`kafka-consumer-groups --describe`, and why "just add consumers" stops helping
above the partition count.

**[P2] KIP-932 share groups (GA in Kafka 4.2).** Queue semantics on Kafka:
consumers in a *share group* cooperatively consume the same partition, with
per-record acquisition locks and individual acknowledgements — so consumer count
is no longer capped by partition count, and a slow record doesn't block the
partition. The precise answer to "we need 200 workers on a 3-partition topic".
Trade-off: you give up ordering entirely.

**[P2] Producer internals:** `linger.ms` / `batch.size` (throughput vs latency —
`linger.ms=0` is not free, batching is what makes Kafka fast),
`compression.type=zstd`, and the coupling already in the config —
`max.in.flight.requests.per.connection=5` is only ordering-safe **because**
`enable.idempotence=true`; without idempotence, >1 in flight plus retries can
reorder records within a partition. The config gets this right; the comment
doesn't explain it.

**[P2] Poll-loop budget:** `max.poll.records` × per-record processing time must
stay under `max.poll.interval.ms`. This is the actual mechanism behind the
README's head-of-line-blocking warning.

### 3.3 Build this

**Connect the two modules — the highest-value single change in the repo.** The
kafka README says the outbox is "left out on purpose"; the postgres module has
`OutboxRelay` sitting there logging to nowhere. Wire `OutboxRelay` to a real
`KafkaTemplate` and the "one thing this deliberately does not solve" becomes
"here's the solution, and here's the integration test that proves the event
survives a crash between the debit and the publish". That single change turns
three exercises into one system.

**`OrderingUnderRetryIT`** — prove the §3.2 tension exists. Three events for one
account; make the second fail once; assert the third is applied before the
retried second. A test that demonstrates a *limitation of your own design* is a
strong artifact.

**`RebalanceIT`** — start a second consumer instance in the same group mid-flow;
assert no record is lost and none double-applied (the idempotency table is what
saves you). Log the assignment before and after.

**`CompactionIT`** — a compacted `account-snapshots` topic; write two values for
one key, force compaction, assert only the latest survives; then write a
tombstone and assert the key is gone.

**Bump the broker image first.** `TestContainerConfig.java:33` pins
`apache/kafka:3.9.1` (not even the last 3.x — 3.9.2 shipped Feb 2026).
`RebalanceIT` works there, but
anything touching KIP-848 or share groups needs a 4.x image. Do the bump as its
own commit so you can say what broke, if anything — "I upgraded across the major
that removed ZooKeeper" is a better sentence than "I read that ZooKeeper was
removed".

**Config additions to defend:** `group.instance.id` for static membership,
`partition.assignment.strategy=CooperativeStickyAssignor` (or, on 4.x,
`group.protocol=consumer`), and `delivery.timeout.ms` in place of `retries: 3`
(§1.2).

### 3.4 Depth ladder

**Q: "How do you guarantee a payment is processed exactly once?"**
- *Mid:* enable exactly-once semantics / idempotent producer.
- *Senior:* you don't — you get at-least-once delivery and make the *effect*
  idempotent. Offset committed after the work; a `processed_event` row keyed on
  the event id inserted in the same transaction as the debit. The unique
  constraint is the guarantee.
- *Staff:* + draws the boundary: Kafka's EOS is real but covers only
  Kafka→Kafka with `read_committed` and offsets in the transaction; the instant
  you touch Postgres or an external rail you're outside it. Then names the
  residual window — the dual write between DB commit and result publish — and
  closes it with the transactional outbox, noting the relay is itself
  at-least-once so the *downstream* must be idempotent too. Idempotency doesn't
  disappear, it moves.

**Q: "One account generates 50% of your traffic."**
- *Mid:* hot partition; add partitions.
- *Senior:* adding partitions doesn't help a single hot *key* — the key always
  hashes to one partition, and rehashing breaks historical ordering anyway.
  Sub-key it (`accountId:bucket`) to spread load, and accept that ordering is
  now per-bucket, which is only safe for commutative operations.
- *Staff:* + asks whether per-account ordering is actually required, or whether
  the ledger's idempotency and commutativity already make it unnecessary — most
  double-entry postings commute. If ordering *is* required, keep the hot key on
  one partition and scale by making processing cheaper (batch the DB writes),
  or move that account to a share group / separate topic with its own SLO.

---

## 4. Postgres

### 4.1 Already defensible

Unusually deep for interview prep, and — critically — **proved against the
engine's own counters** rather than asserted: write skew reproduced under READ
COMMITTED and caught by SSI at SERIALIZABLE, with the retry framed correctly as
*part of* what SERIALIZABLE promises (and the SQLSTATE `40001` read off the
cause chain rather than trusting an exception subtype — a detail that shows real
scar tissue); append-only double-entry with balance as a projection; idempotency
as a UNIQUE constraint; HOT updates verified via `n_tup_hot_upd`; `ctid` proving
update = insert-new-version; `pg_try_advisory_xact_lock` and why `_try_` and
`_xact_` each matter; the transactional outbox proved atomic via a rollback
race; `SKIP LOCKED` as a job-queue primitive with 8 workers; bloat and the
long-transaction-blocks-vacuum failure mode; N+1 counted via Hibernate
`Statistics` instead of timed.

`LongTxnBloatIT` is the standout — "why is this table growing when the row count
is flat" is a real on-call question and most candidates have never seen it
happen.

### 4.2 Gaps, ranked

**[P0] Indexes and query plans — the single largest hole.** There is nothing on:

- B-tree structure, and why **composite index column order** follows the
  equality-then-range rule (an index on `(account_id, created_at)` serves
  `WHERE account_id = ? ORDER BY created_at`; `(created_at, account_id)` does
  not);
- **index-only scans** and their dependence on the **visibility map** — which
  ties straight back to the vacuum material already in phase 4: a bloated table
  with a stale visibility map silently loses index-only scans;
- **partial indexes** — `... WHERE NOT published` for the outbox relay,
  `... WHERE status = 'PENDING'` for the job queue;
- covering indexes (`INCLUDE`), expression indexes;
- reading `EXPLAIN (ANALYZE, BUFFERS)`: estimated vs actual rows (a 1000× gap
  means stale statistics — run `ANALYZE`), seq scan vs index scan vs **bitmap
  heap scan** and why a seq scan is sometimes correct;
- Postgres 18's **skip scan**, which relaxes the leading-column rule.

Right now `schema.sql` creates exactly **one** secondary index
(`idx_postings_account_id`); everything else is implicit from a `PRIMARY KEY` or
a `UNIQUE` constraint, and every foreign-key column —
`postings.transfer_id`, `transfers.from_account_id`, `transfers.to_account_id` —
is unindexed. There is no `EXPLAIN` anywhere in the repo. An interviewer *will*
go here.

**[P0] The balance is `SUM(postings)` forever.** The design is right, but
unbounded: reading a balance is O(postings per account), which is fine at 100
rows and unusable at 10 million. The follow-up is guaranteed: *"how do you read
a balance in a millisecond?"* The answer is **snapshots** —

```
account_balance_snapshots(account_id PK, as_of_posting_id, balance_minor, taken_at)
-- balance = snapshot.balance_minor
--         + SUM(postings WHERE account_id = ? AND id > snapshot.as_of_posting_id)
```

— written by a periodic job, so the read is O(postings since the last snapshot).
The immutable journal stays the source of truth; the snapshot is a cache that
can always be recomputed and audited against. Alternatively maintain a cached
balance in the same transaction as the posting, with a nightly job asserting
`cached == SUM(postings)` and alerting on drift. **Have this ready — it's the
first thing a payments interviewer asks about an append-only ledger.**

**[P1] Enforcing debits = credits in the database.** The README says
double-entry is "enforced by construction rather than a database CHECK
constraint" — which invites *"and how would you enforce it in the database?"*
The answer is a **deferred constraint trigger**
(`DEFERRABLE INITIALLY DEFERRED`) that runs at COMMIT and asserts
`SUM(amount_minor) = 0` per `transfer_id`. Deferred is the whole point: at
statement time the transfer is half-written and legitimately unbalanced. Also
worth: `CHECK (amount_minor <> 0)`, and `amount_minor BIGINT` being the correct
money type (never float; and `NUMERIC` when you need fractional minor units or
FX rates).

**[P1] Keyset pagination.** `LIMIT/OFFSET` reads and discards `OFFSET`
rows; page 5000 costs 5000 pages of work. Seek pagination is O(page size)
regardless of depth:

```sql
SELECT * FROM postings
WHERE account_id = :id AND (created_at, id) < (:lastCreatedAt, :lastId)
ORDER BY created_at DESC, id DESC
LIMIT :size;
```

Needs `CREATE INDEX ON postings (account_id, created_at DESC, id DESC)`. Add it
next to the existing `Pageable` endpoint and let `PaginationIT` assert both
return the same rows while only one degrades — that's the kind of side-by-side
this repo already does well elsewhere.

**[P1] Zero-downtime schema migration.** Standard fintech question, absent here.
`ALTER TABLE ... ADD COLUMN` with a non-volatile default is fast in modern
Postgres (no table rewrite since PG 11), but `ALTER TYPE` and adding a `NOT
NULL` take **ACCESS EXCLUSIVE**, which queues behind — and in front of — every
other query on the table, readers included. Plain `CREATE INDEX` is a step
gentler: it takes **SHARE**, which blocks every *write* for the whole build but
lets reads through. Knowing that distinction is the difference between "the
migration was slow" and "the migration took the site down."
The toolkit: `CREATE INDEX CONCURRENTLY`, `ADD CONSTRAINT ... NOT VALID` then
`VALIDATE CONSTRAINT`, always `SET lock_timeout` before DDL so a migration fails
fast instead of stalling production, and the expand/contract (dual-write) pattern
for column renames.

**[P1] Connection pooling.** HikariCP sizing (small! a pool of 10 usually beats
a pool of 100 — each connection is a Postgres *process*), `max_connections`,
and PgBouncer — specifically that **transaction-pooling mode breaks
session-scoped state**: session-level advisory locks, `SET`, and server-side
prepared statements. Directly relevant, since `phase3` uses advisory locks; note
that `pg_try_advisory_xact_lock` is transaction-scoped and therefore *is* safe
under transaction pooling, while `pg_advisory_lock` is not. That's a genuinely
sharp thing to say.

**[P1] Postgres-level deadlocks.** The concurrency module handles deadlock in
Java; the postgres module never shows Postgres's own detector — `deadlock_timeout`
(1s default), SQLSTATE `40P01`, and the fact that the DB *breaks* deadlocks by
killing a victim rather than hanging. An IT that deliberately deadlocks two
transactions and asserts `40P01` would complete the story that phase 1 starts
with `40001`.

**[P2] Lock modes.** `FOR UPDATE` vs `FOR NO KEY UPDATE` vs `FOR SHARE` vs `FOR
KEY SHARE`, and why an FK check takes `FOR KEY SHARE` on the parent — which is
how unrelated inserts end up blocking each other. `NOWAIT` as the third option
beside `SKIP LOCKED`. Reading `pg_locks`.

**[P2] Isolation-level fine print.** Postgres's REPEATABLE READ is snapshot
isolation and *does* prevent phantoms (unlike the SQL standard's definition);
SERIALIZABLE costs predicate locks (`SIReadLock`) and can abort **read-only**
transactions; `SET TRANSACTION READ ONLY DEFERRABLE` avoids that. And **EvalPlanQual**:
under READ COMMITTED, when an `UPDATE ... WHERE balance >= 100` hits a row a
concurrent transaction just changed, Postgres re-fetches the new version and
**re-evaluates the WHERE clause against it**. That's the actual mechanism behind
the README's "a plain UPDATE mostly avoids lost updates" — naming EvalPlanQual
is a genuine flex.

**[P2] Partitioning.** A ledger is the textbook case: declarative range
partitioning on `postings(created_at)` by month, partition pruning in the plan,
and dropping an old partition instead of `DELETE`-ing millions of rows (which
just creates bloat — connecting straight back to phase 4).

**[P2] Transaction ID wraparound.** The scariest Postgres failure mode: 32-bit
xids, freezing, `autovacuum_freeze_max_age`, and the "database is not accepting
commands to avoid wraparound data loss" shutdown. The cause is usually the same
one `LongTxnBloatIT` already demonstrates — something holding a transaction open
— so it's one paragraph away from material you already have.

**[P2] Replication and read-your-writes.** Streaming vs logical replication;
`synchronous_commit` levels (`off` / `local` / `on` / `remote_apply`) and the
durability-vs-latency dial; replica lag and the classic bug where a customer
transfers money and immediately reads a stale balance from a replica. Logical
replication also connects the outbox to Debezium/CDC, which the README already
name-drops.

### 4.3 Build this

**`phase5_indexing/`** — the missing pillar:

```
IndexPlanIT   — run EXPLAIN (ANALYZE, FORMAT JSON) from the test and ASSERT on
                the node type. Seed 100k postings; assert the account-history
                query uses an Index Scan; drop the index and assert Seq Scan.
                Same trick as NPlusOneIT (assert the mechanism, don't time it)
                and it's the most portfolio-worthy test in the repo.
CompositeOrderIT  — (account_id, created_at) serves the ORDER BY;
                    (created_at, account_id) doesn't. Prove it via the plan.
PartialIndexIT    — outbox relay query: partial index on WHERE NOT published
KeysetPaginationIT— offset page 5000 vs keyset page 5000, buffers read
```

**`phase6_operations/`** — the "have you run this in production" pillar:

```
BalanceSnapshotService + SnapshotIT   — the §4.2 [P0] answer, working
DeferredBalanceConstraintIT           — DEFERRABLE trigger; unbalanced transfer
                                        fails at COMMIT, not at INSERT
PgDeadlockIT                          — two transactions, opposite order,
                                        assert SQLSTATE 40P01
ConcurrentIndexIT                     — CREATE INDEX CONCURRENTLY while writes
                                        are in flight; assert no blocking
```

**Schema fixes worth making regardless** (small, and they show index thinking):

```sql
CREATE INDEX ON postings (account_id, created_at DESC, id DESC);  -- keyset
CREATE INDEX ON postings (transfer_id);            -- unindexed FK today
CREATE INDEX ON outbox (id) WHERE NOT published;   -- relay scan
CREATE INDEX ON payment_jobs (id) WHERE status = 'PENDING';  -- SKIP LOCKED
ALTER TABLE postings ADD CONSTRAINT amount_nonzero CHECK (amount_minor <> 0);
```

Then bump the Testcontainers image to `postgres:18` and mention `uuidv7()` as
the modern answer to "what if ids have to be generated outside the database" —
time-ordered, so unlike random UUIDv4 it doesn't scatter B-tree inserts across
every page and inflate WAL.

### 4.4 Depth ladder

**Q: "Two withdrawals against a shared overdraft limit both succeed."**
- *Mid:* race condition; lock the rows.
- *Senior:* it's **write skew** — each transaction reads a predicate the other
  is about to invalidate, and neither writes a row the other read, so a row lock
  wouldn't have helped even if you'd taken one. Fix with SERIALIZABLE (SSI
  detects the read/write dependency and aborts one with `40001`) plus a retry,
  or materialise the conflict onto a single lockable row.
- *Staff:* + costs and alternatives. SERIALIZABLE means every transaction needs
  a retry loop and predicate locks consume shared memory
  (`max_pred_locks_per_transaction`), so you don't turn it on globally — you
  scope it to the transactions with cross-row invariants. The cheaper
  alternative is to give the invariant a home: a `credit_limits` row you take
  `FOR UPDATE` on, converting write skew into ordinary lock contention. Trade
  throughput for a simpler failure mode.

**Q: "This table's disk usage grows forever but the row count is flat."**
- *Mid:* bloat; run VACUUM.
- *Senior:* MVCC — an `UPDATE` writes a new tuple version and marks the old one
  expired; the space is reclaimed only by VACUUM. If VACUUM is running and the
  count isn't dropping, something is holding an old snapshot. Check
  `pg_stat_activity` for long-running transactions and idle-in-transaction
  sessions.
- *Staff:* + the mechanism: VACUUM cannot remove any tuple version newer than
  the oldest running transaction's snapshot, so one leaked ORM session pins dead
  tuples across the entire database. Fixes: `idle_in_transaction_session_timeout`,
  monitoring `max(xact_start)`, and — if it's already unrecoverable —
  `pg_repack` rather than `VACUUM FULL`, which takes ACCESS EXCLUSIVE. Then
  names the endgame if it's ignored: xid wraparound and a forced shutdown.

---

## 5. What no module covers — and probably should

**Cross-cutting reliability patterns.** Circuit breakers, bulkheads,
timeouts-everywhere, retry with **jitter** (not just exponential backoff —
synchronised retries are a thundering herd), and the fact that a retry without
idempotency is a duplicate-payment generator. Spring Boot 4 ships `@Retryable`
and concurrency limiting in-framework now, so this is also a version-awareness
answer.

**Observability.** What you'd alert on for each module: consumer lag by
partition, DLT depth (should be zero — every record is a customer's money),
outbox age (oldest unpublished row — the single best health metric for the whole
pattern), p99 transfer latency, connection-pool saturation, replication lag,
`n_dead_tup` growth. Design rounds are graded on operational thinking; this is
the cheapest way to demonstrate it and it costs one README section.

**Testing philosophy.** You already have the strongest possible version of this
argument and don't state it: *no mocks anywhere, every assertion on an
observable outcome, and where possible on the engine's own counters
(`pg_stat_user_tables`, `ctid`, Hibernate `Statistics`, consumer offsets)
rather than on timing.* That is a genuine engineering-philosophy answer for the
"how do you know your code works" question. Write it down.

**CI.** There's no `.github/workflows/`. A green badge on a repo whose test
suite spins up real Kafka and Postgres containers is a strong credibility
signal for a take-home review, and it's ~20 lines.

---

## 6. The system design round

Nothing in the repo is shaped like a design answer, and this is the round where
the modules should pay off. Write `DESIGN.md` — one document, the money-movement
system the three modules are already fragments of — and rehearse it as a
45-minute whiteboard.

**The prompt to prepare for:** *"Design a system that moves money between
accounts, at 10k transfers/sec, that never loses or duplicates a payment."*

**Skeleton:**

1. **Clarify first.** Internal ledger transfers or external rails? Single
   currency? Synchronous confirmation or async? Consistency requirement on
   balance reads? — Interviewers grade the questions.
2. **API and idempotency at the edge.** `POST /transfers` with a caller-supplied
   `Idempotency-Key`. Store `(caller, key) → request_hash, status, response`;
   same key + same body → replay the stored response; same key + *different*
   body → reject, because the client has a bug and silently serving either
   result is worse than an error. (Stripe, the usual prior art here, returns
   **400** with an `idempotency_error` — "keys for idempotent requests can only
   be used with the same parameters they were first used with". Cite that rather
   than inventing a status code.) That last case is the detail that separates
   people who've implemented this from people who've read about it. Also: what happens when the first request is
   still **in flight** — you need a `PENDING` state and a retry-after, not just
   success/failure. → `postgres` phase 2.
3. **The ledger.** Append-only double-entry, balance derived, snapshots for read
   performance (§4.2), money as integer minor units. → `postgres` phase 2 + §4.3.
4. **Concurrency control.** Row locks in a deterministic order, or SERIALIZABLE
   plus retry; why a hot account is the real bottleneck and what you'd do about
   it. → `concurrency` phases 2 and 4, `postgres` phase 1.
5. **Async fan-out.** Outbox in the same transaction, relay to Kafka, downstream
   consumers (notifications, fraud, reporting) each with their own guarantees —
   strong for balances, eventual for reconciliation. → all three modules.
6. **Reconciliation.** Matching your ledger against the external rail's
   statement; how you detect and represent a break; why reconciliation is a
   *product* requirement and not a background job. → `kafka` reconciliation.
7. **Failure walk-through** — the part that's actually graded. Talk through: the
   process dies after the DB commit and before the publish; the broker is
   unavailable for 10 minutes; a consumer processes the same event twice; the
   downstream rail times out with no response (the worst case — you don't know
   whether it happened, so you need a *query* API or a reversal); a replica lags
   and a customer sees a stale balance; a bad deploy poisons the DLT.
8. **Scale.** Shard by `account_id`; cross-shard transfers need a saga
   (reserve → commit → compensate) because 2PC across shards is a liveness
   hazard; and the honest note that a compensating "refund" is a *new* ledger
   entry, never an erasure.

Drill separately: "design a rate limiter", "design a notification service",
"design an audit log" — all three reuse pieces you already have.

---

## 7. This repo as a portfolio artifact

It's better than most take-homes already. To finish the job:

- **The root `README.md` is already doing its job** — it opens with the thesis
  (`README.md:3-6`) and its second paragraph (`:14-20`) makes the
  real-infrastructure / read-the-engine's-own-counters argument. Don't rewrite
  it. Two things it doesn't do: it never uses the word *mock*, so the strongest
  version of the testing-philosophy claim in §5 is left implicit; and there's no
  *navigation* — no links to `DESIGN.md` or this file, and no per-module index
  of the interview questions each phase answers.
- **Link the modules to each other.** The outbox wiring (§3.3) is the one change
  that makes the three read as one system.
- **CI badge** (§5).
- **A `DESIGN.md`** (§6).
- **Per-module "questions this answers" list** — a bullet list of the actual
  interview questions each phase covers makes the repo skimmable by an
  interviewer in 90 seconds, which is all the time they'll give it.

---

## 8. A four-week plan

Assumes evenings. Front-loaded toward the rounds that fail people.

**Week 1 — accuracy + live coding.** Fix every P0 in §1 (a couple of hours,
removes the two things that would actively cost you). Then §2.3
`phase7_primitives` — rate limiter, LRU, `Condition`-based bounded buffer —
timed, tests first. Run the §2.5 drills.

**Week 2 — the Postgres hole.** `phase5_indexing`. Learn to read
`EXPLAIN (ANALYZE, BUFFERS)` by running it against your own seeded data until
the numbers stop surprising you. Add the snapshot service and keyset pagination.

**Week 3 — Kafka operations + design.** Rebalance semantics and the three
timeouts until you can recite them. `OrderingUnderRetryIT`. Wire the outbox to
Kafka. Then write `DESIGN.md` and talk through it out loud, timed.

**Week 4 — integration.** Mock interviews. Record yourself explaining one phase
per module in 5 minutes each. Prepare behavioural stories in STAR form: a
production incident, a trade-off you'd now make differently, a disagreement you
resolved with data. Bar-raiser rounds probe judgment, and "here's what I got
wrong and how I found out" outperforms a flawless narrative every time.

---

## 9. Self-test protocol

You know a topic when you can do all four without notes:

1. **Explain it in 60 seconds** to someone who doesn't know it.
2. **Name the trade-off** — every technique here costs something. If you can't
   say what, you've learned the answer and not the reasoning.
3. **Name the failure mode** — what breaks in production, and what the alert
   looks like.
4. **Answer the follow-up** — the depth ladders in §2.4 / §3.4 / §4.4 are the
   follow-ups. If your answer is the *mid* row, you have work to do.

The repo's READMEs are already written for #1 and #2. This document exists for
#3 and #4.
