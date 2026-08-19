-- Creates the dedicated least-privilege role used by the ETL scripts.
--
-- WHY THIS EXISTS: until 2026-08 the ETL authenticated as the personal
-- superuser-ish role `stant`, which is shared with unrelated projects. On
-- 2026-03-17 an `ALTER ROLE stant WITH PASSWORD ...` run while setting up a
-- different project silently invalidated the credential in .env and took the
-- pipeline down for five months (see Docs/code-review-2026-08-04.md).
-- A dedicated role means another project's password reset cannot do that again.
--
-- Run once, as a superuser:
--   psql -U postgres -f scripts/create_etl_role.sql
--
-- The password is set interactively by the \password prompt at the end, so it
-- never appears in this file, in shell history, or in the Postgres server log.
-- Put the same value in .env as DB_USER=aqi_etl / DB_PASSWORD=<what you typed>.

-- Idempotent: safe to re-run.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'aqi_etl') THEN
        CREATE ROLE aqi_etl LOGIN;
    END IF;
END
$$;

GRANT CONNECT ON DATABASE aqi_db TO aqi_etl;

\c aqi_db

-- The ETL reads and writes rows but never changes the schema, so no CREATE.
GRANT USAGE ON SCHEMA public TO aqi_etl;
GRANT SELECT, INSERT, UPDATE ON locations, daily_readings TO aqi_etl;

-- Both tables use SERIAL primary keys; INSERT needs the sequences.
GRANT USAGE, SELECT ON SEQUENCE locations_id_seq, daily_readings_id_seq TO aqi_etl;

-- Keep the grants working if the tables are ever recreated by database_setup.sql
-- (which runs as the owning role, not as aqi_etl).
-- Default privileges attach to the CREATING role, so they must be declared for
-- every role that might create objects here -- postgres (migrations) and stant
-- (the personal role that owns the original tables). Getting this wrong is why
-- hourly_readings was initially unreadable by aqi_etl.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE ON TABLES TO aqi_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO aqi_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE stant IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE ON TABLES TO aqi_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE stant IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO aqi_etl;

\echo ''
\echo 'Now set the password for aqi_etl (it will be prompted for, not echoed):'
\password aqi_etl
