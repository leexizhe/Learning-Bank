# Learning-Bank

[![build](https://github.com/leexizhe/Learning-Bank/actions/workflows/build.yml/badge.svg)](https://github.com/leexizhe/Learning-Bank/actions/workflows/build.yml)

Interview-prep projects built around one running story — a toy bank — with a
module per topic. Each module is a self-contained Spring Boot app with its own
README written the way you'd actually explain it out loud: the code is the easy
part, being able to say *why* each line is there is the interview.

| Module | Topic | Port |
|---|---|---|
| [`concurrency`](concurrency/README.md) | Thread safety, deadlock-free transfers, virtual threads, lock striping, the live-coding primitives, the Java Memory Model | 8081 |
| [`kafka`](kafka/README.md) | Event-driven payments: ordering, idempotent consumers, the transactional outbox, retries, dead-letter topics, rebalancing | 8082 |
| [`postgres`](postgres/README.md) | PostgreSQL internals: MVCC and write skew, the append-only ledger, advisory locks, vacuum and bloat, indexes and query plans, online DDL | 8083 |

Two documents cut across all three:

- **[`DESIGN.md`](DESIGN.md)** — the money-movement system these modules are
  slices of, written as a 45-minute system-design answer. Every claim in it
  points at code here and a test that proves it.
- **[`INTERVIEW-PREP.md`](INTERVIEW-PREP.md)** — a ranked gap analysis of this
  repo: what is still missing, what an interviewer probes that there is no answer
  for yet, and the depth ladders (mid / senior / staff) for the questions that
  matter.

The three modules differ in how much they can prove without a container.
`concurrency` is mostly plain-Java unit tests, because that's the shape of a
live-coding round. `kafka` and `postgres` have **no unit-test layer at all** —
nothing they teach means anything without a real broker or a real database, so
every test is a Testcontainers integration test.

## How this is tested, and why it's the interesting part

**There is not a single mock in this repository.** No Mockito, no stubs, no
in-memory fakes standing in for a broker or a database. That is a deliberate
constraint, and it is the answer to "how do you know your code works":

- **Every assertion is on an observable outcome** — an HTTP response, a row in
  Postgres, a record actually consumed off a topic. Never on "was this method
  called".
- **Where possible, on the engine's own counters** rather than on timing:
  `pg_stat_user_tables` for dead tuples, `ctid` to prove an update writes a new
  tuple version, Hibernate's `Statistics` to count the N+1, `EXPLAIN (ANALYZE,
  BUFFERS, FORMAT JSON)` to assert which plan the planner actually chose,
  `ThreadMXBean.findDeadlockedThreads()` to have the JVM itself confirm a
  deadlock. **Assert the mechanism, don't time it** — a timing assertion tells
  you the machine was busy.
- **Races are made deterministic, not probable.** Tests inject a clock, or pass a
  "run this at the dangerous moment" seam, or rendezvous on a barrier — rather
  than sleeping and hoping for an interleaving.
- **Some tests assert a limitation**, not a feature: that this repo's own retry
  topic breaks its own ordering guarantee, that a deliberately-unordered lock
  acquisition genuinely deadlocks. A design with no tension in it is usually one
  nobody has looked at hard enough.
- **And where a claim can't honestly be asserted, it isn't.** The Java Memory
  Model tests assert the *guarantee* (a `final` field is never seen
  uninitialised) and merely *report* the *anomaly*, because the JMM only permits
  it — on x86 the hardware usually declines to demonstrate it. You cannot test
  your way to memory-model correctness.

## Quickstart

Requires JDK 25 and Docker.

```powershell
.\mvnw.cmd verify                    # every module, full Testcontainers suite (slow)
.\mvnw.cmd -pl postgres verify       # just one topic
.\mvnw.cmd -pl concurrency test      # the fast unit tests, no Docker
```

`verify` needs no running containers — Testcontainers starts throwaway ones per
module. The compose stack below is only for driving an app by hand:

```powershell
docker compose -f docker/docker-compose.yml up -d
.\mvnw.cmd -pl postgres spring-boot:run
```

That brings up one Postgres serving all three databases plus a single-broker
Kafka, so all three apps can run at once on 8081 / 8082 / 8083.
`docker/init/01-databases.sql` creates the per-module roles and databases, and
only runs on first creation of the volume — after editing it, use
`docker compose -f docker/docker-compose.yml down -v` to re-run it.

The same `down -v` is **required once** after the Postgres 16 → 18 bump: a
`pgdata` directory initialised by an older major version refuses to start under
a newer one (`database files are incompatible with server`). Postgres has no
in-place major upgrade without `pg_upgrade`, which is worth knowing for its own
sake — it's why a production major-version upgrade is a planned migration rather
than a new image tag.

## Adding a topic

Create the directory, add a `pom.xml` inheriting from the root, and add one
`<module>` line. Everything shared — Boot version, JDK 25, Lombok, the JPA and
Testcontainers dependencies, Surefire/Failsafe wiring — is inherited from the
parent pom, so a new module's pom is usually just its artifactId and whatever
makes it different.

## Layout

```
pom.xml                     the shared parent: versions, common deps, plugin config
docker/
  docker-compose.yml        one Postgres (three databases) + Kafka
  init/01-databases.sql     per-module roles and databases
concurrency/  kafka/  postgres/
```

Note that `concurrency` compiles with `--enable-preview`: `StructuredTaskScope`
is still a preview API in JDK 25 (JEP 505). That flag is set in that module's
pom, deliberately not the parent's — preview class files are pinned to the exact
JDK feature release that compiled them, and the other modules have no reason to
carry that constraint.

The consequence worth knowing: `javac --enable-preview` stamps **every** class
file in the module, not just the ones touching the preview API, so all of them
require the flag at runtime and **none of them will load on JDK 26**. That's why
CI pins `java-version: 25` exactly rather than "25 or later", and why the flag
appears in four places in `concurrency/pom.xml` — compiler, Surefire, Failsafe
and `spring-boot:run`.
