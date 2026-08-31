-- db/create_api_role.sql
--
-- Creates a dedicated, read-only Postgres role for the FastAPI query layer.
-- See api/dependencies.py -- it connects via settings.api_database_url,
-- using this role, NOT the full-privilege postgres_user that PostgresLoader
-- uses for ETL writes.

CREATE ROLE api_reader WITH LOGIN PASSWORD '5KeNyXML0xzaz8yEQPIraFwYK4EMVF5D';

GRANT CONNECT ON DATABASE market_data TO api_reader;
GRANT USAGE ON SCHEMA market_data TO api_reader;

GRANT SELECT ON market_data.ohlcv TO api_reader;
GRANT SELECT ON market_data.ohlcv_features TO api_reader;
GRANT SELECT ON market_data.pipeline_runs TO api_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA market_data
    GRANT SELECT ON TABLES TO api_reader;