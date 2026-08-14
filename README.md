# Learning-Bank

[![build](https://github.com/leexizhe/Learning-Bank/actions/workflows/build.yml/badge.svg)](https://github.com/leexizhe/Learning-Bank/actions/workflows/build.yml)

Interview-prep projects built around one running story — a toy bank — with a module per topic. Each module is a
self-contained Spring Boot app with its own README written the way you'd actually explain it out loud: the code is the
easy part, being able to say *why* each line is there is the interview.

| Module                                 | Topic                                                                                                                                        | Port |
|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|------|
| [`concurrency`](concurrency/README.md) | Thread safety, deadlock-free transfers, virtual threads, lock striping, the live-coding primitives, the Java Memory Model                    | 8081 |
| [`kafka`](kafka/README.md)             | Event-driven payments: ordering, idempotent consumers, the transactional outbox, retries, dead-letter topics, rebalancing                    | 8082 |
| [`postgres`](postgres/README.md)       | PostgreSQL internals: MVCC and write skew, the append-only ledger, advisory locks, vacuum and bloat, indexes and query plans, online DDL     | 8083 |

One document cuts across the modules:

- **[`DESIGN.md`](DESIGN.md)** — the money-movement system these modules are slices of, written as a 45-minute
  system-design answer. Every claim in it points at code here and a test that proves it.

The modules differ in how much they can prove without a container. `concurrency` is mostly plain-Java unit tests,
because that's the shape of a live-coding round. `kafka` and `postgres` have **no unit-test layer at all** — nothing
they teach means anything without a real broker or a real database, so every test is a Testcontainers integration test.

## How this is tested, and why it's the interesting part

**There is not a single mock in this repository.** No Mockito, no stubs, no in-memory fakes standing in for a broker or
a database. That is a deliberate constraint, and it is the answer to "how do you know your code works":

- **Every assertion is on an observable outcome** — an HTTP response, a row in Postgres, a record actually consumed off
  a topic. Never on "was this method called".
- **Where possible, on the engine's own counters** rather than on timing: `pg_stat_user_tables` for dead tuples, `ctid`
  to prove an update writes a new tuple version, Hibernate's `Statistics` to count the N+1, `EXPLAIN (ANALYZE, BUFFERS,
  FORMAT JSON)` to assert which plan the planner actually chose, `ThreadMXBean.findDeadlockedThreads()` to have the JVM
  itself confirm a deadlock. **Assert the mechanism, don't time it** — a timing assertion tells you the machine was
  busy.
- **Races are made deterministic, not probable.** Tests inject a clock, or pass a "run this at the dangerous moment"
  seam, or rendezvous on a barrier — rather than sleeping and hoping for an interleaving.
- **Some tests assert a limitation**, not a feature: that this repo's own retry topic breaks its own ordering guarantee,
  that a deliberately-unordered lock acquisition genuinely deadlocks. A design with no tension in it is usually one
  nobody has looked at hard enough.
- **And where a claim can't honestly be asserted, it isn't.** The Java Memory Model tests assert the *guarantee* (a
  `final` field is never seen uninitialised) and merely *report* the *anomaly*, because the JMM only permits it — on x86
  the hardware usually declines to demonstrate it. You cannot test your way to memory-model correctness.

## Quickstart

Requires JDK 25 and Docker.

```powershell
.\mvnw.cmd verify                    # every module, full Testcontainers suite (slow)
.\mvnw.cmd -pl postgres verify       # just one topic
.\mvnw.cmd -pl concurrency test      # the fast unit tests, no Docker
.\mvnw.cmd spotless:apply            # the thing to run when verify fails on formatting
```

Two things worth knowing before the first build. **Spotless is a build gate, not a suggestion**: `spotless:check` is
bound to the `package` phase, so any formatting drift fails `verify` — locally and in CI alike. `spotless:apply` fixes
it. And **Docker Engine 29+ needs `api.version=1.44`** in each module's `src/test/resources/docker-java.properties`
(already there) or Testcontainers gets misleading empty 400s back from the daemon.

`verify` needs no running containers — Testcontainers starts throwaway ones per module. The compose stack below is only
for driving an app by hand:

```powershell
docker compose -f docker/docker-compose.yml up -d
.\mvnw.cmd -pl postgres spring-boot:run
```

That brings up one Postgres serving all three databases plus a single-broker Kafka, so every app can run at once on
8081 / 8082 / 8083. `docker/init/01-databases.sql` creates the per-module roles and databases, run by the `pg-init`
one-shot service as soon as Postgres reports healthy.

**Why a service and not `/docker-entrypoint-initdb.d`.** That directory is only read when the data directory is empty,
so adding a module meant `down -v` — destroying every working database to create one new one. `pg-init` runs on
every `up` instead, and the script is written to be idempotent so a stack that is already set up is left untouched.
Neither `CREATE ROLE` nor `CREATE DATABASE` takes `IF NOT EXISTS`, so the roles are guarded by a `DO` block and the
databases by psql's `\gexec` — `CREATE DATABASE` cannot run inside a transaction block, and a PL/pgSQL body is always
one.

A `down -v` is **required once** on an existing checkout, for two reasons at the same time. A `pgdata` directory
initialised by Postgres 16 refuses to start under 18 (`database files are incompatible with server`) — there is no
in-place major upgrade without `pg_upgrade`, which is worth knowing for its own sake, because it's why a production
major-version upgrade is a planned migration rather than a new image tag. And the 18 images relocated the data
directory: the volume now mounts at `/var/lib/postgresql`, not `/var/lib/postgresql/data`, so that `pg_upgrade --link`
can see both versions without crossing a mount boundary. A volume still mounted at the old path is detected as an
"unused mount/volume" and the entrypoint **refuses to start at all** rather than quietly initialising an empty cluster
beside your data.

## Adding a topic

Create the directory, add a `pom.xml` inheriting from the root, and add one `<module>` line. Everything shared — Boot
version, JDK 25, Lombok, the JPA and Testcontainers dependencies, Surefire/Failsafe wiring — is inherited from the
parent pom, so a new module's pom is usually just its artifactId and whatever makes it different.

## Layout

```
pom.xml                     the shared parent: versions, common deps, plugin config
mvnw / mvnw.cmd             the only Maven wrapper in the repo — every build starts here
DESIGN.md                   the money-movement system these modules are slices of
docker/
  docker-compose.yml        one Postgres (three databases) + Kafka
  init/01-databases.sql     per-module roles and databases
concurrency/  kafka/  postgres/     the modules
```

Note that `concurrency` compiles with `--enable-preview`: `StructuredTaskScope` is still a preview API in JDK 25 (JEP
505). That flag is set in that module's pom, deliberately not the parent's — preview class files are pinned to the exact
JDK feature release that compiled them, and the other modules have no reason to carry that constraint.

The consequence worth knowing: `javac --enable-preview` stamps **every** class file in the module, not just the ones
touching the preview API, so all of them require the flag at runtime and **none of them will load on JDK 26**. That's
why CI pins `java-version: 25` exactly rather than "25 or later", and why the flag appears in four places in
`concurrency/pom.xml` — compiler, Surefire, Failsafe and `spring-boot:run`.
