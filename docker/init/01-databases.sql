-- Runs as the superuser on every `docker compose up`, driven by the pg-init service rather than by
-- /docker-entrypoint-initdb.d. That directory only executes on an empty data directory, which made adding a module a
-- `down -v` - destroying every existing database to create one new one.
--
-- The price of running every time is that every statement has to be idempotent, and Postgres offers no IF NOT EXISTS
-- for either object here. Each module keeps the database name / user / password its application.yml already expects, so
-- consolidating onto one Postgres needs no config changes.

-- Roles: guarded inside a DO block, which is the only way to get a conditional in plain SQL. format() with %I
-- (identifier) and %L (literal) rather than string concatenation - EXECUTE on an unquoted name is how injection gets
-- in, and the quoting rules for the two positions genuinely differ.
DO $$
DECLARE
    module text;
BEGIN
    FOREACH module IN ARRAY ARRAY['acrabank', 'concurrencybank', 'kafkabank', 'postgresbank'] LOOP
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = module) THEN
            EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', module, module);
        END IF;
    END LOOP;
END
$$;

-- Databases cannot use the same trick: CREATE DATABASE is forbidden inside a transaction block, and a PL/pgSQL body is
-- always one. \gexec is the psql-side answer - it takes the rows a query returns and runs each one as its own
-- statement, so a query matching nothing executes nothing.
SELECT format('CREATE DATABASE %I OWNER %I', module, module)
FROM unnest(ARRAY['acrabank', 'concurrencybank', 'kafkabank', 'postgresbank']) AS module
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = module)
\gexec
