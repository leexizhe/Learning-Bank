-- Runs once, on first creation of the pgdata volume, as the superuser.
-- Each module keeps the database name / user / password its application.yml
-- already expects, so consolidating onto one Postgres needed no config changes.
--
-- Editing this file has no effect on an existing volume: the entrypoint only
-- runs /docker-entrypoint-initdb.d on an empty data directory. Use
--   docker compose -f docker/docker-compose.yml down -v
-- to drop the volume and re-run it.

CREATE ROLE concurrencybank LOGIN PASSWORD 'concurrencybank';
CREATE DATABASE concurrencybank OWNER concurrencybank;

CREATE ROLE kafkabank LOGIN PASSWORD 'kafkabank';
CREATE DATABASE kafkabank OWNER kafkabank;

CREATE ROLE postgresbank LOGIN PASSWORD 'postgresbank';
CREATE DATABASE postgresbank OWNER postgresbank;
