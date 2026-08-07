# acra — ingesting ACRA Bizfile business profiles

Pulls the **Business Profile (Company)** endpoint from Singapore's ACRA Bizfile API marketplace into Postgres, and
serves it back from there. Port **8084**.

Two calls upstream, and neither is quite standard:

```
POST /authorizeServer/oauth/token?grant_type=client_credentials   # Basic auth -> access token
GET  /api/acra/entityQuery/businessProfile?uen=16888888A          # token: <access_token>
```

## The token question, which is the whole design

The sandbox answers the client-credentials grant with:

```json
{ "access_token": "qvOFt_…", "token_type": "Bearer", "expires_in": 1799, "scope": "read" }
```

**1799 seconds — half an hour, less a second.** That single number rules out both of the approaches you reach for first:

- **A token per request** costs two round trips instead of one on every lookup, and re-mints something that had 1798
  seconds of life left.
- **A token stored as a long-lived key** is dead within the hour. Every request after that 401s until a human pastes in
  a new one.

So it lives in memory, in one `AtomicReference` inside `AcraTokenProvider`, and three things decide when to throw it
away — in descending order of authority:

1. **`expires_in` from the response**, minus a 60s skew. `1799 - 60 = 1739s` of use. The number is *read*, never
   hardcoded, so a shorter lifetime in production needs no code change.
2. **A configured fallback** (`acra.expiry-fallback`, 30m) if the field is ever missing. Deliberately short: guessing
   wrong costs one extra token call.
3. **A 401 from the profile endpoint**, which invalidates the cache and retries exactly once. This is the layer that
   makes the other two safe — nothing in the design depends on 1799 being correct forever, or on our arithmetic being
   right.

The long-lived secret is the **client id and secret**, not the token. Those come from `ACRA_CLIENT_ID` /
`ACRA_CLIENT_SECRET` with no default in `application.yml`, so a missing credential fails the context at startup instead
of at the first request. The access token is never persisted and never logged.

Two smaller details, both non-standard and both deliberate:

  the query string with an empty JSON body. The code matches the sandbox.
- The grant says `token_type: Bearer`, but the profile endpoint does **not** want `Authorization: Bearer`. It wants the
  raw token in a header literally named `token`.

## The response shape, and the trap in it

Read off a live call, not documentation — there does not appear to be any public. A profile arrives wrapped in an
`entities` array:

```json
{"entities": [{
  "uen": "16888888A", "entityName": "ABC ENTERPRISE",
  "registrationDate": "2016-08-18", "statusOfBusiness": "LIVE",
  "constitutionOfBusiness": "SOLE-PROPRIETOR",
  "principalPlaceOfBusiness": {…}, "primaryActivity": {…},
  "authorisedRepresentative": {…}, "partner": {…}
}]}
```

**An unknown UEN returns `200 OK` with `{"entities":[]}` — not a 404.** That is the trap, and it is the reason
`ProfileMapper.requireEntity` exists. An integration that trusts the status code stores an empty row for every typo, and
then serves that row from cache for a week without another call to correct it.
`ProfileErrorIT.anUnknownUenIs404EvenThoughAcraAnswered200` is the test that holds this down.

## Storage: five columns and a payload

```sql
CREATE TABLE business_profile (
    uen VARCHAR(20) PRIMARY KEY, entity_name VARCHAR(500), entity_status VARCHAR(100),
    entity_type VARCHAR(200), registration_date DATE,
    payload JSONB NOT NULL, fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`payload` is the source of truth and holds the response whole. The four promoted columns are a convenience for the
queries you actually run, and **every one is nullable** — a field ACRA renames costs a null column and a warning line,
never a failed ingest and never lost data. Addresses, activity codes and representatives are never mapped at all; they
live in the payload and cost no code.

`jsonb`, not `json`: parsed once on write rather than on every read, and the only one of the two that can be indexed.
The tradeoff is that jsonb does not preserve key order or insignificant whitespace, which is why `ProfilePersistenceIT`
asserts the payload is *semantically* equal to what ACRA sent rather than byte-identical to it. Asserting on the text
would be asserting on an implementation detail of the storage type.

The UEN is the primary key. No surrogate id — ACRA already guarantees uniqueness, and a generated id would only invite
two rows for one company.

## Endpoints

```
GET /api/profiles/{uen}                 read-through: stored copy if fresher than acra.profile-ttl (7d),
                                        otherwise fetch from ACRA, upsert, return
GET /api/profiles/{uen}?refresh=true    skip the freshness check, always call ACRA
GET /api/profiles/{uen}/raw             the stored ACRA response, verbatim
```

`404` means ACRA has no such UEN. `502` — not 500 — means ACRA is broken or unreachable: that distinction tells a caller
retrying is reasonable, instead of sending someone to read our logs for someone else's outage. A failed refresh leaves
the previously stored row completely intact; it is not served either, because silently returning week-old data as if it
were current should be an explicit choice with its own flag, not an accident of error handling.

The HTTP call to ACRA happens **outside** any transaction — see `BusinessProfileTransactionalOps`. Wrapping it would
leave a pooled connection idle-in-transaction for as long as a government API takes to answer, turning a slow upstream
into pool exhaustion that takes down endpoints which never touch ACRA at all.

## Testing without mocking the government

This repository has a no-mocks rule, and ACRA is the one collaborator that genuinely cannot be run in a container the
way Postgres and Kafka can. The substitute is the next most honest thing: `FakeAcraServer`, a real
`com.sun.net.httpserver.HttpServer` on a real socket, speaking real HTTP to an unmodified `RestClient`, serving
**responses recorded verbatim from the sandbox**.

Recording them rather than writing them mattered. The invented sample used `entity_name` at the top level; the real one
nests `entityName` under `entities`. A hand-written fixture tests the shape you imagined — which is the same shape you
wrote the code against, so it can only ever agree with itself.

What makes the server useful is that it *counts*. Every token assertion is on traffic it actually received:

| Test | The claim |
|---|---|
| `TokenCachingIT` | ten profile lookups hit the token endpoint **once**, and all ten carried the *same* token |
| `TokenExpiryIT` | still cached at 1738s, refreshed at 1739s — the skew arithmetic, exact |
| `TokenStampedeIT` | 16 threads released together off a barrier produce **one** authentication |
| `TokenReauthIT` | after a 401 the retry carries a **different** token, and a persistent 401 stops after exactly one retry |
| `ProfileCacheIT` | a second lookup inside the TTL makes **zero** upstream requests |
| `ProfilePersistenceIT` | payload round-trips; a re-fetch updates the row rather than inserting a second |
| `ProfileErrorIT` | `{"entities":[]}` is a 404 and writes nothing; a failed refresh leaves the old row intact |

Every expiry test moves an injected `MutableClock` instead of sleeping. That is not only faster — a sleeping version of
the 1739-second boundary test would take half an hour — it is the only version that proves anything. A timing assertion
that passes tells you the machine was not busy.

`SandboxSmokeIT` is the only test that touches real ACRA, and the only one that can tell you the rest of the suite is
testing the right contract: everything else would keep passing if ACRA renamed every field tomorrow. It is skipped
unless `ACRA_CLIENT_ID` is set, so CI never sees it.

```powershell
.\mvnw.cmd -pl acra verify                              # offline, no credentials needed

$env:ACRA_CLIENT_ID = "..."; $env:ACRA_CLIENT_SECRET = "..."
.\mvnw.cmd -pl acra verify "-Dit.test=SandboxSmokeIT" "-DfailIfNoSpecifiedTests=false"
```

The smoke test prints the response it got. That output is the input to keeping `ProfileMapper`'s pointers honest.

## Running it

```powershell
docker compose -f ..\docker\docker-compose.yml up -d
$env:ACRA_CLIENT_ID = "..."; $env:ACRA_CLIENT_SECRET = "..."
.\mvnw.cmd -pl acra spring-boot:run

curl "http://localhost:8084/api/profiles/16888888A"      # calls ACRA, stores, returns
curl "http://localhost:8084/api/profiles/16888888A"      # served from Postgres, no ACRA call
```

The `acrabank` role and database come from `docker/init/01-databases.sql`, which only runs on first creation of the
volume — an existing stack needs `docker compose -f ..\docker\docker-compose.yml down -v` once to pick it up.

## Known limits

- **No history.** Each fetch overwrites the row. "What did this company look like in March" needs an append-only
  `business_profile_history` table; the JSONB snapshot means the data is already captured in the right shape to populate
  one.
- **The sandbox UEN is a sole proprietorship**, so `partner` and `authorisedRepresentative` are what came back. A real
  company profile carries officers, shareholders and share capital instead. Nothing in the storage layer cares, but the
  promoted columns have only ever been proven against this shape.
- Only Business Profile. The other EIQ endpoints are not wired up.
