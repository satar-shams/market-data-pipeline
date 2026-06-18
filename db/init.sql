-- db/init.sql
-- Runs automatically when the PostgreSQL container starts for the first time.

-- ── Schema ────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS market_data;

-- ── Master partitioned table ──────────────────────────────────────────────────
-- Partitioned by LIST on ticker column.
-- Each ticker gets its own physical partition for query performance.
CREATE TABLE IF NOT EXISTS market_data.ohlcv (
    id          BIGSERIAL,
    ticker      VARCHAR(10)     NOT NULL,
    timestamp   DATE            NOT NULL,
    open        NUMERIC(12, 4)  NOT NULL,
    high        NUMERIC(12, 4)  NOT NULL,
    low         NUMERIC(12, 4)  NOT NULL,
    close       NUMERIC(12, 4)  NOT NULL,
    volume      BIGINT          NOT NULL,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker, timestamp)     -- composite PK includes partition key
) PARTITION BY LIST (ticker);

-- ── Partitions ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_data.ohlcv_aapl
    PARTITION OF market_data.ohlcv FOR VALUES IN ('AAPL');

CREATE TABLE IF NOT EXISTS market_data.ohlcv_msft
    PARTITION OF market_data.ohlcv FOR VALUES IN ('MSFT');

CREATE TABLE IF NOT EXISTS market_data.ohlcv_googl
    PARTITION OF market_data.ohlcv FOR VALUES IN ('GOOGL');

CREATE TABLE IF NOT EXISTS market_data.ohlcv_amzn
    PARTITION OF market_data.ohlcv FOR VALUES IN ('AMZN');

CREATE TABLE IF NOT EXISTS market_data.ohlcv_spy
    PARTITION OF market_data.ohlcv FOR VALUES IN ('SPY');

CREATE TABLE IF NOT EXISTS market_data.ohlcv_nvda
    PARTITION OF market_data.ohlcv FOR VALUES IN ('NVDA');

-- ── Indexes ───────────────────────────────────────────────────────────────────
-- Timestamp index for time-range queries (most common access pattern)
CREATE INDEX IF NOT EXISTS idx_ohlcv_timestamp
    ON market_data.ohlcv (timestamp DESC);

-- Ticker + timestamp compound index for single-ticker time-range queries
CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker_timestamp
    ON market_data.ohlcv (ticker, timestamp DESC);

-- ── Pipeline metadata table ───────────────────────────────────────────────────
-- Tracks every pipeline run: when it ran, what it pulled, success/failure.
-- Essential for idempotent loads and debugging.
CREATE TABLE IF NOT EXISTS market_data.pipeline_runs (
    id              BIGSERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    tickers         TEXT[]          NOT NULL,
    rows_extracted  INTEGER,
    rows_loaded     INTEGER,
    status          VARCHAR(20)     NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    error_message   TEXT,
    duration_sec    NUMERIC(8, 2)
);

-- ── Verification ──────────────────────────────────────────────────────────────
-- Runs at init time so you can see in Docker logs that setup succeeded.
DO $$
BEGIN
    RAISE NOTICE 'market_data schema initialized successfully.';
    RAISE NOTICE 'Partitions created for: AAPL, MSFT, GOOGL, AMZN, SPY, NVDA';
END $$;