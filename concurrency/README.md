# concurrency

A small Java 25 + Spring Boot 3 project built to rehearse Java-concurrency
interview questions (the kind Wise/Revolut-style interviews ask) on a single
running story: a toy bank. Each phase below is written the way you'd actually
explain it out loud in an interview — the point isn't the code, it's being
able to talk through *why* each line is there.

Fast unit tests (no Docker) cover phases 1-3 and 5-6: they're written as
plain Java classes because that's the shape of a live-coding round — you're
asked to implement a thread-safe class from scratch, not stand up a Spring
app. Phase 4 wraps a transfer service in a real Spring Boot REST API backed
by Postgres, and gets the one Testcontainers integration test: fire
concurrent HTTP requests at a real database and prove the books stay
balanced.

## Quickstart

```powershell
cd Learning-Bank
.\mvnw.cmd -pl concurrency test      # phases 1-3 + load balancer unit tests, no Docker needed
.\mvnw.cmd -pl concurrency verify     # + phase 4 Testcontainers integration tests (needs Docker)
.\mvnw.cmd -pl concurrency spring-boot:run   # run the app on :8081 (needs a local Postgres, or use docker: see below)
```

To run the app standalone (outside of tests), start the local Postgres in
`docker/docker-compose.yml` - it matches the datasource already configured in
`application.yml`:

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

**The problem:** `balance += amount` looks like one operation but is really
three — read, add, write. If two threads interleave those steps, one
thread's update can be silently lost. `UnsafeCounter` is deliberately broken
to demonstrate this; `UnsafeCounterTest` proves it by hammering it with 50
threads and showing the final total is *less* than the arithmetic sum.

**Two different fixes, same problem:**
- `SynchronizedAccount` — every method is `synchronized`, which is shorthand
  for acquiring `this`'s intrinsic monitor lock. Two threads calling any
  synchronized method *on the same instance* can never interleave. It's an
  **object-level** lock — a *static* synchronized method would instead lock
  the `Class` object, which is a different, coarser lock shared by every
  instance. `withdraw` also guards the classic "check-then-act" race: checking
  `balance >= amount` and then decrementing has to be one atomic step, or two
  concurrent withdrawals can both pass the check and overdraw the account.
- `AtomicAccount` — no locks at all. `deposit` is a single `addAndGet` call,
  atomic by construction. `withdraw` still has a check-then-act shape, so it
  uses a **compare-and-swap retry loop**: read the balance, compute the new
  value, try to commit with `compareAndSet`, and if another thread beat us to
  it, retry with the fresh value. No thread ever blocks, but a hot account
  will spin instead of queueing.

**What I'd say out loud:** "`volatile` only gives you visibility, not
atomicity — it wouldn't fix `balance++`. For a single counter, `AtomicLong`
usually beats a lock because it's non-blocking; for multi-field invariants
you can't express as one CAS, you reach for a real lock."

## Phase 2 — Deadlock-free transfers (`phase2_deadlock/`)

**The trap:** `transfer(A, B)` locks A then B. A concurrent `transfer(B, A)`
locks B then A. If both threads grab their first lock and then block waiting
for the second, each holds what the other needs — a **circular wait**, one of
the four Coffman conditions for deadlock.

**The fix — global lock ordering:** `LockOrderedTransferService` always
locks the account with the **lower id first**, regardless of which one is
"from" and which is "to". Two accounts can then never be locked in opposite
orders by two different threads, so circular wait becomes structurally
impossible — there's still real contention (both directions fight over the
same first lock), just never a deadlock.

**Belt and suspenders — `tryLock` with a timeout:** instead of a blocking
`lock()`, it uses `ReentrantLock.tryLock(500ms)`. If a lock is held by
something unexpectedly slow, this fails fast with `TransferTimeoutException`
instead of tying up a thread forever — in a real payment system, returning
"service unavailable" after a bounded wait beats hanging the caller
indefinitely. Locks are released in a `finally` block, always, even on the
exception path.

`LockOrderedTransferServiceTest` runs the exact deadlock-trap shape (thread A
doing A→B, thread B doing B→A, concurrently, thousands of times) wrapped in
`assertTimeoutPreemptively` — if the ordering guarantee ever regresses, the
test fails loudly instead of hanging the build.

## Phase 3 — Virtual threads & structured concurrency (`phase3_virtualthreads/`)

**The scenario:** validating a payment means calling three slow, independent
checks — fraud, credit, sanctions. `PaymentGatewayService` fires all three at
once instead of one after another, three different ways, so they can be
compared directly.

**Why virtual threads:** each forked task runs on its own virtual thread by
default. A platform thread costs ~1MB and is 1:1 with an OS thread, so
pooling matters. A virtual thread is cheap enough that you don't pool it —
you spin one up per task and let it block on I/O (here, `Thread.sleep`
standing in for an HTTP call) without tying up an OS thread underneath it.
**Interview differentiator:** never wrap virtual threads in a fixed-size pool
— that just reintroduces the scarcity you were trying to escape.

**But "unlimited threads" isn't "unlimited concurrency".** The old fixed pool
was quietly doing two jobs at once:
1. Stop your own app from creating too many expensive platform threads.
2. Stop you from overloading something downstream (a database, a rate-limited API).

Virtual threads fix job 1 for free — they're cheap, so don't cap them. Job 2
is still real: if these checks hit an actual API that can only take 200
requests/sec, firing 50,000 virtual threads at it will happily take it down.
The JVM won't stop you — it doesn't know or care about the API's limit.

So the fix for job 2 isn't a smaller thread pool, it's a limit on the actual
bottleneck, e.g. a `Semaphore(200)` wrapped around just the outgoing call.
Blocked virtual threads waiting on that semaphore are still cheap, so this
scales fine even with thousands of callers queued behind it.

### Three ways to fan out the same three checks

**1. `validate()` — `StructuredTaskScope`, the primary approach.** Treats the
three forks as **one unit of work**: `scope.join()` doesn't return until all
three finish, and if any one throws, the `Joiner`
(`allSuccessfulOrThrow()` here) cancels the others immediately, genuinely
interrupting whatever they're blocked on. No orphaned virtual thread keeps
running after the method returns — that's the resource-leak problem
structured concurrency exists to prevent. `PaymentGatewayServiceTest` proves
both properties without touching the internals: wall-clock time for three
successful checks is close to the *slowest* one, not the sum (fan-out), and
when one check fails fast, the other two — even configured for 2 seconds —
don't hold up the response (cancellation). Still a *preview* API in JDK 25
(JEP 499), which is the whole reason the other two variants exist.

**2. `validateWithExecutorService()` — the pre-Java-21 fallback**, for when
preview features aren't an option. A shared `ExecutorService` backed by a
small **fixed pool of platform threads** (`Executors.newFixedThreadPool(6)`),
created once for the service, not per call. Submitting three tasks to it is
still concurrent — fan-out was never the hard part — but everything
structured concurrency gave for free now has to be written by hand:
`ExecutorCompletionService` so a failure is noticed the moment it happens
instead of stuck behind an earlier, still-running task
(`take()` returns whichever finishes *next*, not submission order), an
explicit `future.cancel(true)` loop when one fails, and a `shutdown()` wired
to `@PreDestroy` so the pool doesn't outlive the app. `Future.cancel(true)`
here genuinely interrupts the running task.

**3. `validateWithCompletableFuture()` — the composition style** most Java
codebases already reach for day to day, on its own **dedicated executor**
(`Executors.newFixedThreadPool(3)`, kept separate from variant 2's pool —
isolating unrelated workloads onto their own executors instead of sharing
one is its own best practice). `CompletableFuture.allOf(...)` on its own
does **not** cancel siblings when one fails, so this wires it up by hand too:
every future gets a `whenComplete` callback that cancels its siblings the
moment any one fails. **The sharp edge:** `CompletableFuture.cancel(true)`
doesn't interrupt anything — its own javadoc says so ("interrupts are not
used to control processing"). Cancelling `fraud`/`credit` marks those futures
done immediately, so the method still returns fast, but the underlying
`Thread.sleep(2s)` calls keep sleeping for the full 2 seconds regardless,
occupying two of the pool's three threads the whole time. Fast return *and*
real cancellation in variant 2; fast return but a lingering thread leak here
— the opposite trade-off. (`findRealFailure()` in the implementation exists
because of this too: with three futures racing to cancel each other, which
exception `allOf()` itself reports isn't guaranteed, so the method inspects
each future directly afterward for the one that failed on its own merits
rather than as a side effect of being cancelled.)

| | `StructuredTaskScope` | `ExecutorService` | `CompletableFuture` |
|---|---|---|---|
| Cancel siblings on failure | automatic | manual (`future.cancel(true)`) | manual (`whenComplete` + `cancel(true)`) |
| Does cancel actually interrupt? | yes | yes | **no** — futures report done, task keeps running |
| Clean up on exit | automatic (try-with-resources) | manual `shutdown()` via `@PreDestroy` | manual `shutdown()` via `@PreDestroy` |
| How many threads? | not a question — cheap, one per task | a number you pick and tune | a number you pick and tune |
| Composability (chaining, combining unrelated sources) | low — built for one fork/join unit | low | high |

`GatewayController` exposes all three —
`POST /api/gateway/validate` (StructuredTaskScope),
`POST /api/gateway/validate-legacy` (ExecutorService), and
`POST /api/gateway/validate-completable-future` (CompletableFuture) — with
`PaymentGatewayServiceTest`, `PaymentGatewayServiceExecutorTest`, and
`PaymentGatewayServiceCompletableFutureTest` running the same three named
scenarios against each, so any one of the three files stands alone in an
interview and the diffs between files show exactly what changes.

### When to reach for which — rule of thumb

- **`StructuredTaskScope` first**, whenever it's available, for anything
  shaped like "fork a few things, need all of them to succeed, want to bail
  out and clean up completely the moment one fails." Real cancellation and
  guaranteed cleanup, for free. This gateway check is exactly that shape.
- **A raw `ExecutorService`** when structured concurrency isn't an option, or
  when the job isn't really "one fork/join per call" but a long-lived, sized
  pool of workers serving many unrelated calls over time — e.g. gating access
  to a limited downstream resource. You keep real interruption via
  `Future.cancel(true)`, at the cost of owning the pool's lifecycle and
  sizing yourself.
- **`CompletableFuture`** when the shape is a *pipeline* of dependent async
  steps (`thenApply`, `thenCompose`, combining results from unrelated
  sources, `orTimeout`) rather than a flat fan-out-and-join — and pragmatically,
  because it's what most existing codebases already use, so it's often the
  path of least resistance for fitting into surrounding code. Just remember
  cancellation is cooperative-in-name-only: wiring it up stops the *caller*
  from waiting, not the *work* from running.

## Phase 4 — Load balancer + DB-backed ledger

### Load balancer (`phase4_loadbalancer/`)

A minimal TDD warm-up: `RoundRobinLoadBalancer` keeps its "next server"
pointer in an `AtomicInteger` (`getAndIncrement` is the atomic
read-then-advance a plain `int` can't give you under concurrent callers).
`RandomLoadBalancer` uses `ThreadLocalRandom.current()` instead of a shared
`java.util.Random` — a shared `Random` CAS-loops internally on every call and
becomes a contention point under load; giving each thread its own generator
removes that entirely.

### Ledger service (`phase4_ledger/`)

The same lock-ordering idea from Phase 2, moved down a layer: instead of a
`ReentrantLock` per account, `TransferService` takes a Postgres row lock
(`SELECT ... FOR UPDATE`, via `AccountRepository.findByIdForUpdate`) on both
accounts, always in ascending-id order, inside one `@Transactional` method.
Two concurrent transfers between the same pair of accounts — in either
direction — always request the row locks in the same order, so the second
transaction just blocks on the `SELECT ... FOR UPDATE` until the first
commits, instead of deadlocking.

**Pessimistic vs. optimistic, and why both are on `Account`:** the row lock
(pessimistic) is what actually prevents the race — nobody else can even read
the row for update until we commit. `@Version` (optimistic) is kept as
defense-in-depth: if some future code path ever touched a row without going
through `findByIdForUpdate`, Hibernate's version check would still catch the
lost update at flush time and throw instead of silently corrupting the
balance. In an interview: pessimistic locking trades throughput for
certainty and is the right call when contention on the *same* row is
expected (like two transfers hitting the same account); optimistic locking
is cheaper when conflicts are rare and you'd rather retry than block.

`TransferControllerIT` is the integration test: it spins up a real Postgres
via Testcontainers, creates two accounts over HTTP, then fires 100 concurrent
`alice→bob` transfers interleaved with 100 concurrent `bob→alice` transfers —
the exact deadlock-trap shape from Phase 2, now exercised through the full
Spring MVC + JPA stack instead of a unit test. If the lock ordering were
wrong, this would either hang or leave the books unbalanced; instead it
asserts both accounts end up exactly back at their starting balance.

## Phase 5 — Locking gotchas (`phase5_locking_gotchas/`)

Four small, standalone classic-interview-question demos.

**`ConcurrentLedger`** — an in-memory ledger with *no explicit lock anywhere*.
`deposit` is `balances.merge(id, amount, Long::sum)`; `withdraw` is
`balances.compute(id, ...)` with an `AtomicBoolean` capturing whether there
were sufficient funds inside the remapping lambda. This is the direct answer
to the interview trap called out by name in the source material — "failing
to use thread-safe collections like `ConcurrentHashMap` for in-memory
ledgers." **Why it's safe:** `compute`/`merge` hold `ConcurrentHashMap`'s
internal per-bin lock for the duration of the remapping function, so updates
to the *same* key are atomic while different keys update fully concurrently
— that's the "per-bucket synchronization" the interview question is
actually asking about, just handed to you by the collection instead of
hand-written like `phase2_deadlock.LockOrderedTransferService`. The one
sharp edge worth naming: that lambda runs *while the bin lock is held*, so it
must be fast and must never call back into the same map.

**`StringLockBugDemo`** — two completely unrelated operations,
`writeAuditLog` and `postLedgerEntry`, that both mistakenly
`synchronized ("shared-resource-lock")`. String literals are interned, so
every occurrence of that literal in the class file is the *same* object at
runtime — the two operations silently serialize against each other despite
guarding nothing in common, for a reason invisible in either method's own
code. `LockStripedRegistry` is the fix: a `ConcurrentHashMap<String, Object>`
handing out one private, never-interned lock object per key
(`computeIfAbsent(key, k -> new Object())`) — "lock striping." The test times
both: the buggy pair takes ~sum (unwanted serialization), the striped pair
takes ~max (genuine concurrency).

**`BankRegistry`** — `static synchronized nextTransactionId()` (class-level
lock: one counter, one lock, shared by every instance and caller forever) next
to instance `synchronized recordActivity()` (object-level lock: one per
instance). The test proves the three things this question is really asking:
two different instances' `recordActivity()` calls never block each other;
two threads calling the static method *do* serialize; and calling the static
method doesn't block a concurrent instance-method call *on the same object*
— they're different monitors entirely.

**`ExchangeRateService`** — hand-written double-checked-locking singleton.
`instance` has to be `volatile`: without it, a reading thread could observe a
non-null reference to a *partially constructed* object, because nothing
stops the JVM/CPU reordering the constructor's writes ahead of the reference
assignment. `volatile` is the memory barrier that forbids that reordering.
`ExchangeRateServiceHolder` sits next to it as the alternative most engineers
actually reach for: the initialization-on-demand holder idiom, which gets
the identical guarantee (created at most once, safely published to every
thread) for free from the JVM's class-initialization lock — no `volatile`,
no nested null checks, nothing to get subtly wrong.

## Phase 6 — Modern JDK & async patterns (`phase6_async_patterns/`)

**`PinningDemo`** — `withSynchronized` blocks (`Thread.sleep`, standing in
for I/O) inside a `synchronized` block; `withReentrantLock` does the identical
thing with a `ReentrantLock` instead. Before JDK 24, a virtual thread that
blocks while holding a `synchronized` monitor is **pinned** to its carrier
platform thread for the whole block — the carrier can't be released to run
any other virtual thread, so exactly the scalability virtual threads are
supposed to buy you evaporates for the code path that needs it most (blocking
I/O under a lock). `ReentrantLock` never pins, on any JDK version, because
`lock()`/`unlock()` are ordinary method calls, not a JVM monitor operation.
Pinning affects scalability, not correctness, so it isn't something a fast
unit test can assert on reliably — the test here only proves both stay
race-free under concurrent virtual-thread load. To actually *see* the
difference: run with `-Djdk.tracePinnedThreads=short` and watch stderr — the
`synchronized` version logs a pinned-thread warning on every call, the
`ReentrantLock` version logs nothing.

**`AuditContext`** — wraps a `ScopedValue<String>` correlation id and forks
three "account check" subtasks under `StructuredTaskScope` that each read it
back. A `ThreadLocal` is mutable, has no defined end to its lifetime (forget
to `remove()` it and it leaks — especially dangerous on a pooled thread that
outlives the request that set it), and doesn't propagate to a new thread
unless you reach for `InheritableThreadLocal`, which only copies the value
once at thread-creation time. A `ScopedValue` is immutable for the life of
one `where(...).call(...)` block, automatically unbound the instant that
block exits, and — the part that matters here — automatically visible to
subtasks forked from inside that block, with zero propagation code written
by hand. `ScopedValue` was finalized in JDK 25 (unlike `StructuredTaskScope`,
still preview) — verified with `javap` before building against it directly.

**`TellerQueue`** — a bounded producer/consumer buffer built directly on the
intrinsic lock plus `wait()`/`notifyAll()`, no `BlockingQueue`. `wait()` is
always inside a `while` loop, never an `if`: the JVM can wake a waiting
thread for no reason at all (a "spurious wakeup"), and even a genuine
`notifyAll()` only means the condition *might* have changed — by the time
this thread re-acquires the lock, another thread could already have taken
the slot it was waiting for. The `while` re-checks the real condition every
time, instead of trusting that being woken means it's safe to proceed.
`notifyAll()`, not `notify()`: this queue has two different kinds of waiters
on one monitor (producers waiting for space, consumers waiting for an item);
`notify()` wakes one arbitrary waiter with no way to target "a consumer"
specifically, so it can wake the wrong kind and lose a signal.
`notifyAll()` wakes everyone, and each re-checks its own condition in its own
`while` loop. The test runs 5 producers and 5 consumers concurrently and
asserts every item submitted is consumed exactly once — no loss, no
duplication.

## Project layout

```
src/main/java/com/concurrencybank/
  phase1_threadsafety/   UnsafeCounter, SynchronizedAccount, AtomicAccount
  phase2_deadlock/       LockedAccount, LockOrderedTransferService
  phase3_virtualthreads/ PaymentGatewayService (3 fan-out variants), {Fraud,Credit,Sanctions}CheckClient, GatewayController
  phase4_loadbalancer/   LoadBalancer, RoundRobinLoadBalancer, RandomLoadBalancer
  phase4_ledger/         entity/repository/service/controller/dto/exception — the Postgres-backed ledger
  phase5_locking_gotchas/ ConcurrentLedger, StringLockBugDemo, LockStripedRegistry, BankRegistry, ExchangeRateService(Holder)
  phase6_async_patterns/  PinningDemo, AuditContext, TellerQueue
src/test/java/com/concurrencybank/
  testutil/ConcurrencyHarness.java   shared "fire N threads at once" helper for stress tests
  phase1-3,5-6 *Test.java             fast unit tests, no Docker
  phase4_ledger/*IT.java              Testcontainers integration tests (mvnw verify)
```

## Commands

```powershell
.\mvnw.cmd -pl concurrency test      # Surefire: *Test (phases 1-3, 5-6, pure logic + concurrency stress, no Docker)
.\mvnw.cmd -pl concurrency verify     # + Failsafe: *IT (phase 4, full app against Testcontainers Postgres)
.\mvnw.cmd -pl concurrency test -Dtest=LockOrderedTransferServiceTest   # single unit test
.\mvnw.cmd -pl concurrency verify -Dit.test=TransferControllerIT -Dtest=skip   # single IT
```

Docker Engine 29+ needs `api.version=1.44` in
`src/test/resources/docker-java.properties` (already there) or Testcontainers
gets misleading empty 400s from the daemon.
