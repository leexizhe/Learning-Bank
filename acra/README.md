# acra — ACRA Bizfile business profiles

A standalone Spring Boot app (its own `pom.xml`, port **8084**) that pulls the **Business Profile (Company)** endpoint
from Singapore's ACRA Bizfile API into Postgres and returns it.

```
com/acra/
  AcraApplication.java
  config/      AcraProperties (the acra.* block), RestClientConfig (the RestClient bean)
  controller/  AcraController      GET /api/profiles/{uen}
  service/     AcraService         token -> fetch -> save
  entity/      AcraProfile         table acra_profile
  repository/  AcraProfileRepository
```

## The two upstream calls

```
POST /authorizeServer/oauth/token?grant_type=client_credentials   # Basic auth -> access token
GET  /api/acra/entityQuery/businessProfile?uen=16888888A          # token: <access_token>
```

Two things are non-standard and both are deliberate:

- The grant is **not** RFC 6749. The spec puts `grant_type` in a form-encoded body; this API wants it in the query
  string with an empty JSON body.
- The response says `token_type: Bearer`, but the profile endpoint does **not** want `Authorization: Bearer`. It wants
  the raw token in a header literally named `token`.

A token is minted per request. It could be cached — the sandbox advertises `expires_in: 1799` — but caching it means
owning an expiry policy, and this project is deliberately the simple version.

## The trap in the response

An unknown UEN returns **`200 OK` with `{"entities":[]}`**, not a 404. Trusting the status code stores an empty row for
every typo, so `AcraService` checks the array before writing and answers 404 itself.

## Storage

```sql
CREATE TABLE acra_profile (
    uen VARCHAR(20) PRIMARY KEY, entity_name VARCHAR(500),
    payload JSONB NOT NULL, fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`payload` is the source of truth and holds the response whole, so a field ACRA renames never costs data.
`entity_name` is promoted only so the common query doesn't need JSON operators.

**To run without a database**, comment out the two marked lines in `AcraService.fetch` — the endpoint then returns the
ACRA response as a straight pass-through.

## Endpoint

```
GET /api/profiles/{uen}     200 the ACRA payload | 404 no such UEN | 502 ACRA broken or unreachable
```

## Running it

```powershell
docker compose -f ..\docker\docker-compose.yml up -d
$env:ACRA_CLIENT_ID = "..."; $env:ACRA_CLIENT_SECRET = "..."
.\mvnw.cmd spring-boot:run

curl "http://localhost:8084/api/profiles/16888888A"
```

The `acrabank` role and database come from `docker/init/01-databases.sql`.

## Tests

```powershell
.\mvnw.cmd verify      # no credentials needed
```

`AcraIT` runs the whole flow against a Testcontainers Postgres and `FakeAcraServer` — a real `com.sun.net.httpserver`
on a real socket serving responses recorded from the sandbox, counting the requests it receives. No mocks.
