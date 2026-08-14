# concurrency

A small Java 25 + Spring Boot 3 project built to rehearse Java-concurrency interview questions (the kind
Wise/Revolut-style interviews ask) on a single running story: a toy bank. Each phase below is written the way you'd
actually explain it out loud in an interview — the point isn't the code, it's being able to talk through *why* each line
is there.

Fast unit tests (no Docker) cover phases 1-3 and 5-8: they're written as plain Java classes because that's the shape of
a live-coding round — you're asked to implement a thread-safe class from scratch, not stand up a Spring app. Phase 4 is
the exception in both directions: its load balancer is a plain unit test like the rest, but its ledger wraps a transfer
service in a real Spring Boot REST API backed by Postgres and gets the module's one Testcontainers integration test —
fire concurrent HTTP requests at a real database and prove the books stay balanced.

## Quickstart

```powershell
cd Learning-Bank
.\mvnw.cmd -pl concurrency test      # phases 1-3, 5-8 + the load balancer, no Docker needed
.\mvnw.cmd -pl concurrency verify     # + phase 4's ledger Testcontainers integration test (needs Docker)
.\mvnw.cmd -pl concurrency spring-boot:run   # run the app on :8081 (needs a local Postgres, or use docker: see below)
```

To run the app standalone (outside of tests), start the local Postgres in `docker/docker-compose.yml` - it matches the
datasource already configured in `application.yml`:

```powershell
docker compose -f docker/docker-compose.yml up -d
```

Then:

```powershell
curl -X POST localhost:8081/api/accounts -H "Content-Type: application/json" -d "{\"owner\":\"alice\",\"initialBalanceMinor\":10000}"
curl -X POST localhost:8081/api/accounts -H "Content-Type: application/json" -d "{\"owner\":\"bob\",\"initialBalanceMinor\":0}"
curl -X POST localhost:8081/api/transfers -H "Content-Type: application/json" -d "{\"fromAccountId\":1,\"toAccountId\":2,\"amountMinor\":500}"
curl -X POST localhost:8081/api/gateway/validate?transactionId=TX-1
```

## Phase 1 — Thread safety basics (`phase1_threadsafety/`)

**The problem:** `balance += amount` looks like one operation but is really three — read, add, write. If two threads
interleave those steps, one thread's update can be silently lost. `UnsafeCounter` is deliberately broken to demonstrate
this; `Phase1ThreadSafetyTest` proves it by hammering it with 50 threads and showing the final total is *less* than the
arithmetic sum. That one test class holds the whole phase — the broken counter and both fixes run the identical stress
shape from shared constants, so the only thing that differs between them is the final assertion.

**Making a race assert-able, which is the interesting part.** The two fixed accounts assert a guarantee and are
therefore easy; the broken one asserts an *anomaly*, which the JMM permits but never promises, and it flaked in CI for
two independent reasons worth knowing. First, the JIT: a plain field carries no happens-before edge, so once C2 compiles
the deposit loop it may keep the balance in a register for all 10,000 iterations and write back once, shrinking the race
window to nothing — measured, a plain field loses updates on a JVM's first two or three runs and then reports the exact
total forever after. `UnsafeCounter.balance` is therefore `volatile`, which is *not* a fix (see phase 8: visibility,
never atomicity) but forbids the hoist, so every iteration is a real read-modify-write. Second, parallelism: on a single
CPU the harness's virtual threads never interleave, because a compute-only loop hits no blocking point and so each
thread runs to completion on the one carrier — 20 runs out of 20 came out exact when pinned to one core. That one can't
be retried away, so the test `assumeTrue`s two CPUs and skips below that. With both handled the stress loses updates on
the first attempt in 39 of 40 measured trials, and it retries up to five times before failing.

**Two different fixes, same problem:**

- `SynchronizedAccount` — every method is `synchronized`, which is shorthand for acquiring `this`'s intrinsic monitor
  lock. Two threads calling any synchronized method *on the same instance* can never interleave. It's an
  **object-level** lock — a *static* synchronized method would instead lock the `Class` object, which is a different,
  coarser lock shared by every instance. `withdraw` also guards the classic "check-then-act" race: checking `balance >=
  amount` and then decrementing has to be one atomic step, or two concurrent withdrawals can both pass the check and
  overdraw the account.
- `AtomicAccount` — no locks at all. `deposit` is a single `addAndGet` call, atomic by construction. `withdraw` still
  has a check-then-act shape, so it uses a **compare-and-swap retry loop**: read the balance, compute the new value, try
  to commit with `compareAndSet`, and if another thread beat us to it, retry with the fresh value. No thread ever
  blocks, but a hot account will spin instead of queueing.

**The follow-up — `AbaProblemDemo`:** *"when does CAS give you the wrong answer even though it succeeded?"* CAS asks "is
the value still what I read?", which is not the same question as "has nothing happened since I read?". A value that goes
A → B → A passes the comparison even though the world came and went in between. On `AtomicAccount` that's **harmless** —
a balance of 100 is a balance of 100 however it got back there, because the CAS guards an arithmetic invariant and
arithmetic has no history. Swap the `long` for a *reference* and the identical CAS becomes a use-after-free: the
textbook lock-free stack where a node is popped, another popped, then the first pushed back with a stale `next`. The fix
is `AtomicStampedReference`, which compares identity *and* a version counter, or `AtomicMarkableReference` when one bit
("logically deleted") is enough. The test does the A→B→A sequence on a single thread — ABA is about the sequence of
values, not about timing, so no race is needed to demonstrate it.

**What I'd say out loud:** "`volatile` only gives you visibility, not atomicity — it wouldn't fix `balance++`. For a
single counter, `AtomicLong` usually beats a lock because it's non-blocking; for multi-field invariants you can't
express as one CAS, you reach for a real lock."

## Phase 2 — Deadlock-free transfers (`phase2_deadlock/`)

**The trap:** `transfer(A, B)` locks A then B. A concurrent `transfer(B, A)` locks B then A. If both threads grab their
first lock and then block waiting for the second, each holds what the other needs — a **circular wait**, one of the four
Coffman conditions for deadlock.

**The fix — global lock ordering:** `LockOrderedTransferService` always locks the account with the **lower id first**,
regardless of which one is "from" and which is "to". Two accounts can then never be locked in opposite orders by two
different threads, so circular wait becomes structurally impossible — there's still real contention (both directions
fight over the same first lock), just never a deadlock.

**Belt and suspenders — `tryLock` with a timeout:** instead of a blocking `lock()`, it uses
`ReentrantLock.tryLock(500ms)`. If a lock is held by something unexpectedly slow, this fails fast with
`TransferTimeoutException` instead of tying up a thread forever — in a real payment system, returning "service
unavailable" after a bounded wait beats hanging the caller indefinitely. Locks are released in a `finally` block,
always, even on the exception path.

`Phase2DeadlockTest.LockOrderedTransferServiceTests` runs the exact deadlock-trap shape (thread A doing A→B, thread B
doing B→A, concurrently, thousands of times) wrapped in `assertTimeoutPreemptively` — if the ordering guarantee ever
regresses, the test fails loudly instead of hanging the build.

**Proving the trap is real, not just that the fix is fast.** A timeout is weak evidence: it can't tell "no deadlock"
from "a slow machine", and it can't tell a genuine fix from a lucky interleaving. So `NaiveTransferService` keeps the
*broken* version — locks in caller order, with a blocking `lock()` and no timeout — and
`Phase2DeadlockTest.NaiveTransferServiceTests` asserts it genuinely deadlocks, with
**`ThreadMXBean.findDeadlockedThreads()`** as the witness. That's the JVM's own lock-graph analysis naming both threads
as a cycle, not an inference from elapsed time. The two threads rendezvous on a `CyclicBarrier` between taking their
first lock and asking for their second, so the circular wait is *deterministic* rather than a race the test hopes wins.

Two details that make it safe to keep in a build:

- Those threads never finish, so they're **daemon** and are never `join()`ed. A non-daemon deadlocked thread would hang
  the build permanently — which is precisely the production failure being demonstrated, and a terrible property for a
  test.
- `findDeadlockedThreads()` is **JVM-global**, and Surefire runs every test class in one fork. So
  `Phase2DeadlockTest.LockOrderedTransferServiceTests`' mirror assertion — polled *while* its two threads contend,
  asserting the JVM never sees a cycle between them — is scoped to its own thread ids. An unscoped `isEmpty()` would
  pass or fail depending on class ordering. (Verify with `-Dsurefire.runOrder=reversealphabetical`, which puts the
  naive demo first.)

Use `findDeadlockedThreads()`, not `findMonitorDeadlockedThreads()`: the latter only sees cycles built from
`synchronized` monitors, and `LockedAccount` guards itself with a `ReentrantLock`.

## Phase 3 — Virtual threads & structured concurrency (`phase3_virtualthreads/`)

**The scenario:** validating a payment means calling three slow, independent checks — fraud, credit, sanctions.
`PaymentGatewayService` fires all three at once instead of one after another, three different ways, so they can be
compared directly.

**Why virtual threads:** each forked task runs on its own virtual thread by default. A platform thread costs ~1MB and is
1:1 with an OS thread, so pooling matters. A virtual thread is cheap enough that you don't pool it — you spin one up per
task and let it block on I/O (here, `Thread.sleep` standing in for an HTTP call) without tying up an OS thread
underneath it. **Interview differentiator:** never wrap virtual threads in a fixed-size pool — that just reintroduces
the scarcity you were trying to escape.

**But "unlimited threads" isn't "unlimited concurrency".** The old fixed pool was quietly doing two jobs at once:

1. Stop your own app from creating too many expensive platform threads.
2. Stop you from overloading something downstream (a database, a rate-limited API).

Virtual threads fix job 1 for free — they're cheap, so don't cap them. Job 2 is still real: if these checks hit an
actual API that can only take 200 requests/sec, firing 50,000 virtual threads at it will happily take it down. The JVM
won't stop you — it doesn't know or care about the API's limit.

So the fix for job 2 isn't a smaller thread pool, it's a limit on the actual bottleneck, e.g. a `Semaphore(200)` wrapped
around just the outgoing call. Blocked virtual threads waiting on that semaphore are still cheap, so this scales fine
even with thousands of callers queued behind it.

### Three ways to fan out the same three checks

**1. `validate()` — `StructuredTaskScope`, the primary approach.** Treats the three forks as **one unit of work**:
`scope.join()` doesn't return until all three finish, and if any one throws, the `Joiner` (`allSuccessfulOrThrow()`
here) cancels the others immediately, genuinely interrupting whatever they're blocked on. No orphaned virtual thread
keeps running after the method returns — that's the resource-leak problem structured concurrency exists to prevent.
`Phase3VirtualThreadsTest` proves both properties without touching the internals: wall-clock time for three successful
checks is close to the *slowest* one, not the sum (fan-out), and when one check fails fast, the other two — even
configured for 2 seconds — don't hold up the response (cancellation). Still a *preview* API in JDK 25 (JEP 505), which
is the whole reason the other two variants exist.

**2. `validateWithExecutorService()` — the pre-Java-21 fallback**, for when preview features aren't an option. A shared
`ExecutorService` backed by a small **fixed pool of platform threads** (`Executors.newFixedThreadPool(6)`), created once
for the service, not per call. Submitting three tasks to it is still concurrent — fan-out was never the hard part — but
everything structured concurrency gave for free now has to be written by hand: `ExecutorCompletionService` so a failure
is noticed the moment it happens instead of stuck behind an earlier, still-running task (`take()` returns whichever
finishes *next*, not submission order), an explicit `future.cancel(true)` loop when one fails, and a `shutdown()` wired
to `@PreDestroy` so the pool doesn't outlive the app. `Future.cancel(true)` here genuinely interrupts the running task.

**3. `validateWithCompletableFuture()` — the composition style** most Java codebases already reach for day to day, on
its own **dedicated executor** (`Executors.newFixedThreadPool(3)`, kept separate from variant 2's pool — isolating
unrelated workloads onto their own executors instead of sharing one is its own best practice).
`CompletableFuture.allOf(...)` on its own does **not** cancel siblings when one fails, so this wires it up by hand too:
every future gets a `whenComplete` callback that cancels its siblings the moment any one fails. **The sharp edge:**
`CompletableFuture.cancel(true)` doesn't interrupt anything — its own javadoc says so ("interrupts are not used to
control processing"). Cancelling `fraud`/`credit` marks those futures done immediately, so the method still returns
fast, but the underlying `Thread.sleep(2s)` calls keep sleeping for the full 2 seconds regardless, occupying two of the
pool's three threads the whole time. Fast return *and* real cancellation in variant 2; fast return but a lingering
thread leak here — the opposite trade-off. (`findRealFailure()` in the implementation exists because of this too: with
three futures racing to cancel each other, which exception `allOf()` itself reports isn't guaranteed, so the method
inspects each future directly afterward for the one that failed on its own merits rather than as a side effect of being
cancelled.)

|                                                       | `StructuredTaskScope`                | `ExecutorService`                     | `CompletableFuture`                              |
|-------------------------------------------------------|--------------------------------------|---------------------------------------|--------------------------------------------------|
| Cancel siblings on failure                            | automatic                            | manual (`future.cancel(true)`)        | manual (`whenComplete` + `cancel(true)`)         |
| Does cancel actually interrupt?                       | yes                                  | yes                                   | **no** — futures report done, task keeps running |
| Clean up on exit                                      | automatic (try-with-resources)       | manual `shutdown()` via `@PreDestroy` | manual `shutdown()` via `@PreDestroy`            |
| How many threads?                                     | not a question — cheap, one per task | a number you pick and tune            | a number you pick and tune                       |
| Composability (chaining, combining unrelated sources) | low — built for one fork/join unit   | low                                   | high                                             |

`GatewayController` exposes all three — `POST /api/gateway/validate` (StructuredTaskScope), `POST
/api/gateway/validate-legacy` (ExecutorService), and `POST /api/gateway/validate-completable-future` (CompletableFuture)
— with `Phase3VirtualThreadsTest`'s three nested blocks (`StructuredTaskScopeTests`, `ExecutorServiceTests`,
`CompletableFutureTests`) running the same three named scenarios against each, so any one of them stands alone in an
interview and the diffs between adjacent blocks show exactly what changes.

### When to reach for which — rule of thumb

- **`StructuredTaskScope` first**, whenever it's available, for anything shaped like "fork a few things, need all of
  them to succeed, want to bail out and clean up completely the moment one fails." Real cancellation and guaranteed
  cleanup, for free. This gateway check is exactly that shape.
- **A raw `ExecutorService`** when structured concurrency isn't an option, or when the job isn't really "one fork/join
  per call" but a long-lived, sized pool of workers serving many unrelated calls over time — e.g. gating access to a
  limited downstream resource. You keep real interruption via `Future.cancel(true)`, at the cost of owning the pool's
  lifecycle and sizing yourself.
- **`CompletableFuture`** when the shape is a *pipeline* of dependent async steps (`thenApply`, `thenCompose`, combining
  results from unrelated sources, `orTimeout`) rather than a flat fan-out-and-join — and pragmatically, because it's
  what most existing codebases already use, so it's often the path of least resistance for fitting into surrounding
  code. Just remember cancellation is cooperative-in-name-only: wiring it up stops the *caller* from waiting, not the
  *work* from running.

## Phase 4 — Load balancer + DB-backed ledger

### Load balancer (`phase4_loadbalancer/`)

A minimal TDD warm-up: `RoundRobinLoadBalancer` keeps its "next server" pointer in an `AtomicInteger` (`getAndIncrement`
is the atomic read-then-advance a plain `int` can't give you under concurrent callers). `RandomLoadBalancer` uses
`ThreadLocalRandom.current()` instead of a shared `java.util.Random` — a shared `Random` CAS-loops internally on every
call and becomes a contention point under load; giving each thread its own generator removes that entirely.

### Ledger service (`phase4_ledger/`)

The same lock-ordering idea from Phase 2, moved down a layer: instead of a `ReentrantLock` per account,
`TransferService` takes a Postgres row lock (`SELECT ... FOR UPDATE`, via `AccountRepository.findByIdForUpdate`) on both
accounts, always in ascending-id order, inside one `@Transactional` method. Two concurrent transfers between the same
pair of accounts — in either direction — always request the row locks in the same order, so the second transaction just
blocks on the `SELECT ... FOR UPDATE` until the first commits, instead of deadlocking.

**Pessimistic vs. optimistic, and why both are on `Account`:** the row lock (pessimistic) is what actually prevents the
race — nobody else can even read the row for update until we commit. `@Version` (optimistic) is kept as
defense-in-depth: if some future code path ever touched a row without going through `findByIdForUpdate`, Hibernate's
version check would still catch the lost update at flush time and throw instead of silently corrupting the balance. In
an interview: pessimistic locking trades throughput for certainty and is the right call when contention on the *same*
row is expected (like two transfers hitting the same account); optimistic locking is cheaper when conflicts are rare and
you'd rather retry than block.

`Phase4LedgerIT.TransferControllerTests` is the integration test: it spins up a real Postgres via Testcontainers,
creates two accounts over HTTP, then fires 100 concurrent `alice→bob` transfers interleaved with 100 concurrent
`bob→alice` transfers — the exact deadlock-trap shape from Phase 2, now exercised through the full Spring MVC + JPA
stack instead of a unit test. If the lock ordering were wrong, this would either hang or leave the books unbalanced;
instead it asserts both accounts end up exactly back at their starting balance.

## Phase 5 — Locking gotchas (`phase5_locking_gotchas/`)

Four small, standalone classic-interview-question demos.

**`ConcurrentLedger`** — an in-memory ledger with *no explicit lock anywhere*. `deposit` is `balances.merge(id, amount,
Long::sum)`; `withdraw` is `balances.compute(id, ...)` with an `AtomicBoolean` capturing whether there were sufficient
funds inside the remapping lambda. This is the direct answer to the interview trap called out by name in the source
material — "failing to use thread-safe collections like `ConcurrentHashMap` for in-memory ledgers." **Why it's safe:**
`compute`/`merge` hold `ConcurrentHashMap`'s internal per-bin lock for the duration of the remapping function, so
updates to the *same* key are atomic while different keys update fully concurrently — that's the "per-bucket
synchronization" the interview question is actually asking about, just handed to you by the collection instead of
hand-written like `phase2_deadlock.LockOrderedTransferService`. The one sharp edge worth naming: that lambda runs *while
the bin lock is held*, so it must be fast and must never call back into the same map.

**`StringLockBugDemo`** — two completely unrelated operations, `writeAuditLog` and `postLedgerEntry`, that both
mistakenly `synchronized ("shared-resource-lock")`. String literals are interned, so every occurrence of that literal in
the class file is the *same* object at runtime — the two operations silently serialize against each other despite
guarding nothing in common, for a reason invisible in either method's own code. `LockStripedRegistry` is the fix: a
`ConcurrentHashMap<String, Object>` handing out one private, never-interned lock object per key (`computeIfAbsent(key, k
-> new Object())`) — "lock striping." The test times both: the buggy pair takes ~sum (unwanted serialization), the
striped pair takes ~max (genuine concurrency).

**`BankRegistry`** — `static synchronized nextTransactionId()` (class-level lock: one counter, one lock, shared by every
instance and caller forever) next to instance `synchronized recordActivity()` (object-level lock: one per instance). The
test proves the three things this question is really asking: two different instances' `recordActivity()` calls never
block each other; two threads calling the static method *do* serialize; and calling the static method doesn't block a
concurrent instance-method call *on the same object* — they're different monitors entirely.

**`ExchangeRateService`** — hand-written double-checked-locking singleton. `instance` has to be `volatile`: without it,
a reading thread could observe a non-null reference to a *partially constructed* object, because nothing stops the
JVM/CPU reordering the constructor's writes ahead of the reference assignment. `volatile` is the memory barrier that
forbids that reordering. `ExchangeRateServiceHolder` sits next to it as the alternative most engineers actually reach
for: the initialization-on-demand holder idiom, which gets the identical guarantee (created at most once, safely
published to every thread) for free from the JVM's class-initialization lock — no `volatile`, no nested null checks,
nothing to get subtly wrong.

## Phase 6 — Modern JDK & async patterns (`phase6_async_patterns/`)

**`PinningDemo`** — `withSynchronized` blocks (`Thread.sleep`, standing in for I/O) inside a `synchronized` block;
`withReentrantLock` does the identical thing with a `ReentrantLock` instead. Before JDK 24, a virtual thread that blocks
while holding a `synchronized` monitor is **pinned** to its carrier platform thread for the whole block — the carrier
can't be released to run any other virtual thread, so exactly the scalability virtual threads are supposed to buy you
evaporates for the code path that needs it most (blocking I/O under a lock). `ReentrantLock` never pins, on any JDK
version, because `lock()`/`unlock()` are ordinary method calls, not a JVM monitor operation. Pinning affects
scalability, not correctness, so it isn't something a fast unit test can assert on reliably — the test here only proves
both stay race-free under concurrent virtual-thread load. To actually *see* the difference: run with
`-Djdk.tracePinnedThreads=short` and watch stderr — the `synchronized` version logs a pinned-thread warning on every
call, the `ReentrantLock` version logs nothing.

**`AuditContext`** — wraps a `ScopedValue<String>` correlation id and forks three "account check" subtasks under
`StructuredTaskScope` that each read it back. A `ThreadLocal` is mutable, has no defined end to its lifetime (forget to
`remove()` it and it leaks — especially dangerous on a pooled thread that outlives the request that set it), and doesn't
propagate to a new thread unless you reach for `InheritableThreadLocal`, which only copies the value once at
thread-creation time. A `ScopedValue` is immutable for the life of one `where(...).call(...)` block, automatically
unbound the instant that block exits, and — the part that matters here — automatically visible to subtasks forked from
inside that block, with zero propagation code written by hand. `ScopedValue` was finalized in JDK 25 (unlike
`StructuredTaskScope`, still preview) — verified with `javap` before building against it directly.

**`TellerQueue`** — a bounded producer/consumer buffer built directly on the intrinsic lock plus `wait()`/`notifyAll()`,
no `BlockingQueue`. `wait()` is always inside a `while` loop, never an `if`: the JVM can wake a waiting thread for no
reason at all (a "spurious wakeup"), and even a genuine `notifyAll()` only means the condition *might* have changed — by
the time this thread re-acquires the lock, another thread could already have taken the slot it was waiting for. The
`while` re-checks the real condition every time, instead of trusting that being woken means it's safe to proceed.
`notifyAll()`, not `notify()`: this queue has two different kinds of waiters on one monitor (producers waiting for
space, consumers waiting for an item); `notify()` wakes one arbitrary waiter with no way to target "a consumer"
specifically, so it can wake the wrong kind and lose a signal. `notifyAll()` wakes everyone, and each re-checks its own
condition in its own `while` loop. The test runs 5 producers and 5 consumers concurrently and asserts every item
submitted is consumed exactly once — no loss, no duplication.

**`CounterContention`** — `AtomicLong` and `LongAdder` counting the same events. "For a single counter, `AtomicLong`
usually beats a lock" is true and incomplete: `AtomicLong` is *one memory location*, so every incrementing thread CASes
the same cache line, most of those CASes fail and retry, and the line ping-pongs between cores exactly when throughput
matters. `LongAdder` spreads the count over striped cells and grows that array only when it detects collisions, so the
uncontended case stays cheap and the contended case gets an order of magnitude better. The trade-off is that **`sum()`
is not atomic** with respect to concurrent updates — no `compareAndSet`, no `getAndIncrement`, and a value read
mid-flight corresponds to no single instant. Hence the rule worth saying out loud: **`LongAdder` for a request counter,
never for an account balance.** Metrics are written constantly, read rarely, and nothing branches on them; a balance is
read *in order to decide* whether a withdrawal may proceed. The test asserts both counters reach the identical exact
total and only *logs* the timings — timing assertions belong in a JMH benchmark, not a build.

## Phase 7 — Live-coding primitives (`phase7_primitives/`)

Five classes that get asked by name in 45-minute coding rounds. Same house rule as everywhere else — one class, one test
that proves the property, javadoc that says *why*.

**`TokenBucketRateLimiter`** — capacity tokens to spend, refilled at a steady rate, which gives you **burst then
throttle**: a quiet caller may spend the whole bucket at once, then drops to the refill rate. There is deliberately **no
timer thread**; refill is computed from elapsed nanos whenever someone asks, so an idle limiter costs nothing and a
million per-API-key limiters cost a million small objects rather than a million scheduled tasks. The sharp edge is in
`refill()`: it advances its marker by *exactly the time it converted into tokens*, never to `now`. Snapping to `now`
discards the sub-token remainder on every call, and since callers poll far more often than one token-interval, the
limiter quietly delivers less than its configured rate. `pollingFasterThanTheRefillRateDoesNotLeakElapsedTime` is the
regression test for precisely that.

**`SlidingWindowRateLimiter`** — the "why isn't a fixed window enough?" answer, made concrete. A fixed-window counter
permits **twice the limit** across a boundary — 5 requests at 999ms and 5 more at 1001ms is 10 in two milliseconds, all
of it inside the stated policy, and the thing you were protecting sees a 2× spike while your dashboard reports
compliance. `cannotProduceTheDoubleRateBurstThatAFixedWindowAllows` asserts the sliding version can't. What it costs is
O(limit) memory per limiter against the token bucket's O(1) — knowing *which* limiter to reach for is the actual
question.

**`ConcurrentLruCache`** — `LinkedHashMap` in access-order mode behind one lock. The trap is that in access-order mode
**`get` is a mutating operation**: it relinks the entry as most-recent. So the obvious "optimisation" — a
`ReadWriteLock` with `get` on the read lock — is a *bug*, because read locks are shared and several threads would relink
the same intrusive list at once. If an interviewer offers you a read-write lock here, the answer is "not for LRU,
because the read path writes". Follow that to its conclusion and you get the senior answer to *now make it concurrent*:
**exact LRU and lock-free reads are mutually exclusive.** Caffeine and Guava resolve it by giving up exact LRU —
accesses go into per-thread ring buffers and get replayed onto the eviction policy asynchronously.

**`BoundedBufferWithCondition`** — `TellerQueue` rewritten on `ReentrantLock` plus two `Condition`s. **Read the two
files side by side; the diff is the whole answer.** One intrinsic monitor means one wait set holding two kinds of
waiter, which is why `TellerQueue` is *forced* to use `notifyAll()` and wake everyone to accomplish one handoff. Two
`Condition`s split that wait set, so `notEmpty.signal()` wakes exactly one consumer and it is guaranteed to be a thread
that can proceed — one wakeup per handoff instead of N. The subtlety most candidates miss: the `while` loop is **still
required**, because `signal()` only moves a thread to the lock queue and `ReentrantLock` is non-fair by default, so a
fresh caller can barge in and take the item first. And the price is real — `synchronized` releases its monitor on every
exit path for free, whereas one missing `finally` here wedges every other thread forever. `TellerQueue` is the safer
code; this is the faster and more expressive code. That trade is the answer, not "Condition is better".

**`BorrowablePool`** — fixed-size pool of expensive objects. The semaphore *is* the pool; the queue is only storage.
Borrowing takes a timeout, because an unbounded `acquire()` turns a saturated pool into a pile of parked request threads
and the symptom in production is "the service is dead", not "the pool is full". Returning is not the caller's job —
`borrow()` hands back an `AutoCloseable` lease so try-with-resources makes the `finally` impossible to forget. The sharp
edge: `close()` is **idempotent**. Leaking a permit shrinks the pool until it stops working, which you notice;
double-releasing *inflates* it past the size you configured, so the ceiling you built the pool to enforce quietly stops
existing and `max_connections` tells you instead. Semaphores don't check that the releaser was the acquirer, which is
what makes the guard necessary.

## Phase 8 — The Java Memory Model, made observable (`phase8_memorymodel/`)

The rest of this repo *uses* the JMM. This phase states it. Every other phase answers "what does this code do?"; these
three answer "why is it allowed to do that?", which is where the senior questions live.

**`UnsafePublication` vs `FinalFieldFreeze`** — the same publication race twice, differing by one keyword. Writing
`holder = new Holder(42)` is really three steps (allocate, write the field, publish the reference), and nothing in the
JMM forces step two to be visible before step three: no `volatile`, no lock, no happens-before edge, so the compiler may
reorder and the store buffer may drain out of order. A reader can therefore see the reference but read `value` as `0` —
a value nobody ever wrote. Making that field `final` forbids it outright: a **freeze action** at the end of the
constructor emits a store-store barrier, so any thread reading through a reference obtained after construction sees the
initialised value, with no synchronization and no cost on the read side. That is why immutable objects are safe to share
freely, and why *"never let `this` escape the constructor"* is a rule — an object observed before the freeze gets no
guarantee at all. The other caveat: `final` protects the *reference*, not the contents, so a `final List` still needs
its own synchronization.

**`StopFlagVisibility`** — visibility isolated from atomicity. Two identical spin loops, one plain flag and one
`volatile`. The plain read carries no happens-before edge, so the JIT may hoist it clean out of the loop, turning `while
(running)` into `if (running) while (true)`. People reach for "cached in a register or an L1 line", which is the right
intuition and the wrong mechanism — the optimisation usually happens in the compiler, long before hardware. And the
follow-up: `volatile` buys visibility, never atomicity, which is why it fixes a flag and cannot fix `count++`. **Use
`volatile` for flags, never for counters.**

**The methodological point, which is the actual interview material.** These tests are deliberately asymmetric.
`Phase8MemoryModelTest.FinalFieldFreezeTests` *asserts* the anomaly never happens, because the JMM guarantees it. Its
sibling `UnsafePublicationTests` only *reports* how often it saw one, because the JMM merely **permits** it — and on
x86, whose TSO model forbids store-store reordering in hardware, HotSpot usually declines to demonstrate it. On this
machine it reports 35,000 reads and zero sightings. A test asserting the bug appears would fail precisely on the
platforms where the code is safest.

So: **you cannot test your way to memory-model correctness.** A green suite proves your hardware declined to show you
the bug today, not that the bug isn't there — and the same class on an ARM server, where store-store reordering is
permitted and does occur, is a different story. You reason from the specification, or you use a tool built for it
(jcstress). Saying that out loud is worth more than any stress loop.

*(The plain-flag test is the mirror image and gets the same treatment: whether the JIT hoists depends on how long the
loop ran before C2 compiled it, so the outcome is logged, not asserted, and the reader runs on a daemon thread so a
successful hoist can't hang the build.)*

## Project layout

```
src/main/java/com/concurrencybank/
  phase1_threadsafety/   UnsafeCounter, SynchronizedAccount, AtomicAccount, AbaProblemDemo
  phase2_deadlock/       LockedAccount, LockOrderedTransferService, NaiveTransferService (deadlocks on purpose)
  phase3_virtualthreads/ PaymentGatewayService (3 fan-out variants), {Fraud,Credit,Sanctions}CheckClient, GatewayController
  phase4_loadbalancer/   LoadBalancer, RoundRobinLoadBalancer, RandomLoadBalancer
  phase4_ledger/         entity/repository/service/controller/dto/exception — the Postgres-backed ledger
  phase5_locking_gotchas/ ConcurrentLedger, StringLockBugDemo, LockStripedRegistry, BankRegistry, ExchangeRateService(Holder)
                          InterruptibleAction — a Runnable that may throw InterruptedException (see below)
  phase6_async_patterns/  PinningDemo, AuditContext, TellerQueue, CounterContention
  phase7_primitives/      TokenBucketRateLimiter, SlidingWindowRateLimiter, ConcurrentLruCache, BoundedBufferWithCondition, BorrowablePool
  phase8_memorymodel/     UnsafePublication, FinalFieldFreeze, StopFlagVisibility
src/test/java/com/concurrencybank/
  testutil/ConcurrencyHarness.java   shared "fire N threads at once" helper for stress tests
  testutil/DeadlockProbe.java        ThreadMXBean deadlock assertions, scoped to a test's own threads
  phase<N>/Phase<N><Name>Test.java   one test class per phase, one @Nested block per topic - fast units, no Docker
  phase4_ledger/TestContainerConfig.java  singleton Postgres, in the only phase that needs one
  phase4_ledger/BaseControllerIT.java     @SpringBootTest + TestRestTemplate, package-private beside its one subclass
  phase4_ledger/Phase4LedgerIT.java       Testcontainers integration tests (mvnw verify)
```

One test class per phase, rather than one per production class: these tests are read far more often than they are run,
and most of a phase is the same test twice with one thing changed. Keeping a phase in one file is what lets you see the
difference instead of holding two files in your head. The `@Nested` blocks keep each topic grouped in the IDE test tree
and still allow targeting a single one with ``-Dtest='Phase3VirtualThreadsTest$ExecutorServiceTests'``.

Phase 4 is the one phase with two test classes, and the split is Surefire/Failsafe rather than topical:
`Phase4LoadBalancerTest` is a plain unit test, `Phase4LedgerIT` needs a container. A `*Test` and a `*IT` cannot be the
same file, because they run in different phases of the build.

`InterruptibleAction` exists for a small but real reason: `Runnable.run()` cannot declare a checked exception, and the
lock-striping demo hands it work that blocks. Swallowing `InterruptedException` inside the helper would destroy the
interruption semantics phase 5 is trying to show, so the functional interface declares it instead.

## Commands

```powershell
.\mvnw.cmd -pl concurrency test      # Surefire: *Test (phases 1-3, 5-8 + phase 4's load balancer, no Docker)
.\mvnw.cmd -pl concurrency verify     # + Failsafe: *IT (phase 4's ledger, full app against Testcontainers Postgres)
.\mvnw.cmd -pl concurrency test -Dtest=Phase2DeadlockTest   # single phase
.\mvnw.cmd -pl concurrency verify -Dit.test=Phase4LedgerIT -Dtest=skip   # single IT
```

---

## Questions this module answers

Skim this if you have 90 seconds. Each phase exists because of a question.

| Question                                                                       | Where                                                    |
|--------------------------------------------------------------------------------|----------------------------------------------------------|
| Why isn't `count++` atomic, and does `volatile` fix it?                        | phase 1, phase 8                                         |
| Implement a thread-safe account with `withdraw`. Now without locks.            | phase 1 (`AtomicAccount`)                                |
| When does CAS succeed and still give you the wrong answer?                     | phase 1 (`AbaProblemDemo`)                               |
| `AtomicLong` or `LongAdder` for this counter?                                  | phase 6 (`CounterContention`)                            |
| Two threads transfer between the same two accounts, opposite directions.       | phase 2                                                  |
| Prove it actually deadlocks.                                                   | phase 2 (`Phase2DeadlockTest.NaiveTransferServiceTests`) |
| 10,000 concurrent requests, three downstream calls each.                       | phase 3                                                  |
| Virtual threads: when do they help, and when do they change nothing?           | phase 3                                                  |
| Three ways to fan out — which cancels correctly?                               | phase 3                                                  |
| Why is `synchronized("someString")` a bug?                                     | phase 5                                                  |
| Double-checked locking: why is `volatile` load-bearing?                        | phase 5                                                  |
| Write a rate limiter. Token bucket or sliding window?                          | phase 7                                                  |
| Write a thread-safe LRU cache. Now make it concurrent.                         | phase 7                                                  |
| Bounded blocking queue with `wait`/`notify`. Now with `Condition`s.            | phase 6 → phase 7                                        |
| Write a connection pool with borrow timeout.                                   | phase 7 (`BorrowablePool`)                               |
| Why is a `final` field visible without synchronization when a plain one isn't? | phase 8                                                  |
| Why might a loop never see another thread's write?                             | phase 8 (`StopFlagVisibility`)                           |
