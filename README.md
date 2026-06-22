# Market Data Pipeline

A production-grade ETL pipeline for ingesting, transforming, and storing equity market data — built as a portfolio project demonstrating data engineering and ML-readiness practices.

## Overview

This pipeline extracts daily OHLCV (Open/High/Low/Close/Volume) data for multiple tickers from Yahoo Finance, engineers technical indicators commonly used in financial ML models, and loads both raw and derived data into a partitioned PostgreSQL database with full idempotency and run tracking.

**Current phase: Phase 1 — Foundation (Extract → Transform → Load)**

Planned phases:
- **Phase 2:** Apache Airflow orchestration, scheduled runs
- **Phase 3:** FastAPI service layer for querying processed data
- **Phase 4:** Streamlit dashboard, cloud deployment (AWS free tier / Railway)

## Architecture

yfinance API

│

▼

┌─────────────┐     ┌──────────────────┐     ┌─────────────────────┐

│  Extract    │ ──▶ │   Transform      │ ──▶ │   Load               │

│  (multi-    │     │  (returns, SMA,  │     │  (PostgreSQL,        │

│  ticker,    │     │  volatility,     │     │  partitioned,        │

│  retry      │     │  RSI, volume MA) │     │  idempotent upsert)  │

│  logic)     │     │                  │     │                      │

└─────────────┘     └──────────────────┘     └─────────────────────┘

│

┌─────────┴─────────┐

▼                   ▼

market_data.ohlcv   market_data.ohlcv_features

(raw, immutable)     (engineered, FK to ohlcv)

## Tech Stack

| Layer | Technology |
|---|---|
| Extraction | Python, yfinance |
| Transformation | pandas, NumPy |
| Storage | PostgreSQL 16 (partitioned by ticker) |
| Configuration | Pydantic Settings |
| Containerization | Docker, Docker Compose |
| Testing | pytest |

## Key Design Decisions

- **Partitioned by ticker (`PARTITION BY LIST`)** — each ticker lives in its own physical partition for query performance and clean data management at scale.
- **Raw/feature separation** — `ohlcv` (raw, immutable) and `ohlcv_features` (engineered) are separate tables linked by a foreign key. This mirrors feature-store architecture used in production ML systems: raw data is never mutated, and features can be recomputed or versioned independently.
- **Idempotent loads** — uses `INSERT ... ON CONFLICT DO NOTHING` on a composite `(ticker, timestamp)` primary key. Re-running the pipeline never produces duplicate rows.
- **Retry logic with backoff** — the extractor retries failed downloads (configurable attempts/delay), since external APIs and unreliable networks fail intermittently in practice.
- **Partial-failure visibility** — if some tickers fail extraction, the pipeline still loads what succeeded and explicitly records the run as `partial_success` rather than silently reporting full success.
- **Pipeline run tracking** — every execution (success, partial, or failed) is logged to a `pipeline_runs` table with row counts, duration, and error messages — a basic but real observability layer.

## Features Engineered

| Feature | Description |
|---|---|
| `daily_return` | Day-over-day percentage price change |
| `sma_20` / `sma_50` | 20-day and 50-day simple moving averages (trend) |
| `volatility_20d` | 20-day rolling standard deviation of returns (risk) |
| `volume_ma_20` | 20-day average trading volume (liquidity) |
| `rsi_14` | 14-day Relative Strength Index (momentum) |

## Project Structure

market-data-pipeline/

├── config/

│   └── settings.py            # Centralized Pydantic settings, reads .env

├── etl/

│   ├── extract/

│   │   ├── base.py            # Abstract base extractor

│   │   └── yfinance_extractor.py

│   ├── transform/

│   │   └── ohlcv_transformer.py

│   └── load/

│       └── postgres_loader.py

├── db/

│   └── init.sql                # Schema, partitions, indexes

├── tests/

│   └── unit/

│       └── test_transformer.py

├── docker-compose.yml

└── .env.example

## Setup

### Prerequisites
- Python 3.12+
- Docker & Docker Compose

### Installation

```bash
# Clone and enter the repo
git clone https://github.com/satar-shams/market-data-pipeline.git
cd market-data-pipeline

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# edit .env with your database credentials

# Start PostgreSQL
docker compose up -d
```

### Running the Pipeline

```bash
# Run the full extract → transform → load pipeline
python -m etl.load.postgres_loader
```

### Running Tests

```bash
pip install -r requirements-dev.txt
pytest tests/unit/ -v
```

## Known Issues

- **Stale proxy environment variables** can cause `yfinance` to fail instantly with a misleading "possibly delisted" error for every ticker. If extraction fails in under 1 second for all tickers, check `env | grep -i proxy` and run `unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY`.

## Roadmap

- [x] Multi-ticker extraction with retry logic
- [x] Technical indicator feature engineering
- [x] Partitioned PostgreSQL storage with idempotent loads
- [x] Unit tests for transformation logic
- [ ] Airflow DAG for scheduled orchestration
- [ ] FastAPI endpoints for querying processed data
- [ ] Integration tests for extract/load layers
- [ ] Cloud deployment

## Author

Satar Shamsi — [LinkedIn](https://linkedin.com/in/satar-shamsi/) · [GitHub](https://github.com/satar-shams)