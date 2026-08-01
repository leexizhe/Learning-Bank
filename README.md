# Learning-Bank

Interview-prep projects built around one running story — a toy bank — with a
module per topic. Each module is a self-contained Spring Boot app with its own
README written the way you'd actually explain it out loud: the code is the easy
part, being able to say *why* each line is there is the interview.

| Module | Topic | Port |
|---|---|---|
| [`concurrency`](concurrency/README.md) | Thread safety, deadlock-free transfers, virtual threads, lock striping, load balancing | 8081 |
| [`kafka`](kafka/README.md) | Event-driven payments: ordering, idempotent consumers, retries and dead-letter topics | 8082 |
| [`postgres`](postgres/README.md) | PostgreSQL internals: MVCC and write skew, the append-only ledger, advisory locks, the transactional outbox, vacuum and bloat | 8083 |

The three differ in how much they can prove without a container. `concurrency`
is mostly plain-Java unit tests, because that's the shape of a live-coding round.
`kafka` and `postgres` have **no unit-test layer at all** — nothing they teach
means anything without a real broker or a real database, so every test is a
Testcontainers integration test, and several read the engine's own internal
counters (`pg_stat_user_tables`, `ctid`, consumer-group offsets) rather than
trusting that a fix "should" work.

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
