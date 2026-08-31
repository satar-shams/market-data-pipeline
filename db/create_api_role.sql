-- db/create_api_role.sql
-- Run this manually after provisioning the database.
-- Replace <API_READER_PASSWORD> with the actual password set in Render env vars.

CREATE ROLE api_reader WITH LOGIN PASSWORD '<API_READER_PASSWORD>';

GRANT CONNECT ON DATABASE market_data TO api_reader;
GRANT USAGE ON SCHEMA market_data TO api_reader;
GRANT SELECT ON market_data.ohlcv TO api_reader;
GRANT SELECT ON market_data.ohlcv_features TO api_reader;
GRANT SELECT ON market_data.pipeline_runs TO api_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA market_data
    GRANT SELECT ON TABLES TO api_reader;
