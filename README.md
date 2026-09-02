# Market Data Pipeline

A production-grade ETL pipeline for ingesting, transforming, and storing equity market data — built as a portfolio project demonstrating data engineering and ML-readiness practices.


## Overview

This pipeline extracts daily OHLCV (Open/High/Low/Close/Volume) data for multiple tickers from Yahoo Finance, engineers technical indicators commonly used in financial ML models, loads both raw and derived data into a partitioned PostgreSQL database with full idempotency and run tracking, and exposes the processed data through a read-only REST API. The pipeline is orchestrated end-to-end with Apache Airflow, running as a scheduled, dependency-aware DAG rather than a manually triggered script.

**Status: Live and deployed** — [https://market-data-pipeline-c3ut.onrender.com](https://market-data-pipeline-c3ut.onrender.com)

- ## Live Demo

The API is deployed and queryable at **https://market-data-pipeline-c3ut.onrender.com**

| Link                                                                                           | Description                |
| ---------------------------------------------------------------------------------------------- | -------------------------- |
| [/docs](https://market-data-pipeline-c3ut.onrender.com/docs)                                   | Interactive Swagger UI     |
| [/health](https://market-data-pipeline-c3ut.onrender.com/health)                               | API + DB health check      |
| [/tickers](https://market-data-pipeline-c3ut.onrender.com/tickers)                             | Available tickers          |
| [/ohlcv/AAPL?limit=5](https://market-data-pipeline-c3ut.onrender.com/ohlcv/AAPL?limit=5)       | Sample OHLCV data          |
| [/features/AAPL?limit=5](https://market-data-pipeline-c3ut.onrender.com/features/AAPL?limit=5) | Sample engineered features |
| [/pipeline-runs](https://market-data-pipeline-c3ut.onrender.com/pipeline-runs)                 | Pipeline run history       |

> **Note:** Render's free tier spins down inactive services after 15 minutes. The first request after inactivity may take 30–60 seconds to wake up.

## Architecture

```
                              yfinance API
                                   │
                                   ▼
                          ┌─────────────┐
                          │  Extract    │
                          │  (multi-    │
                          │  ticker,    │
                          │  retry      │
                          │  logic)     │
                          └──────┬──────┘
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
            ┌──────────────┐         ┌──────────────────┐
            │  Load (raw)  │         │   Transform       │
            │              │         │  (returns, SMA,   │
            │              │         │  volatility, RSI, │
            │              │         │  volume MA)       │
            └──────┬───────┘         └─────────┬─────────┘
                   │                            ▼
                   │                   ┌──────────────────┐
                   │                   │  Load (features)  │
                   │                   └─────────┬─────────┘
                   ▼                             ▼
         market_data.ohlcv            market_data.ohlcv_features
         (raw, immutable)              (engineered, FK to ohlcv)
```

Orchestrated as an Airflow DAG (`market_data_pipeline`) with the following task dependency graph:

```
extract_task ──┬──> load_raw_task
               └──> transform_task ──> load_features_task
```

Each Airflow task is a thin wrapper around the existing, independently-tested `YFinanceExtractor`, `OHLCVTransformer`, and `PostgresLoader` classes. Airflow does not reimplement pipeline logic — it schedules, sequences, retries, and provides observability over logic that already has its own unit test coverage.

## Tech Stack

| Layer            | Technology                            |
| ---------------- | ------------------------------------- |
| API              | FastAPI                               |
| Orchestration    | Apache Airflow 2.9                    |
| Extraction       | Python, yfinance                      |
| Transformation   | pandas, NumPy                         |
| Storage          | PostgreSQL 16 (partitioned by ticker) |
| Configuration    | Pydantic Settings                     |
| Containerization | Docker, Docker Compose                |
| Testing          | pytest                                |

## Key Design Decisions

- **Partitioned by ticker (`PARTITION BY LIST`)** — each ticker lives in its own physical partition for query performance and clean data management at scale.
- **Raw/feature separation** — `ohlcv` (raw, immutable) and `ohlcv_features` (engineered) are separate tables linked by a foreign key. This mirrors feature-store architecture used in production ML systems: raw data is never mutated, and features can be recomputed or versioned independently.
- **Idempotent loads** — uses `INSERT ... ON CONFLICT DO NOTHING` on a composite `(ticker, timestamp)` primary key. Re-running the pipeline, whether manually or via a re-triggered Airflow run, never produces duplicate rows.
- **Retry logic with backoff** — the extractor retries failed downloads (configurable attempts/delay), since external APIs and unreliable networks fail intermittently in practice.
- **Partial-failure visibility** — if some tickers fail extraction, the pipeline still loads what succeeded and explicitly records the run as `partial_success` rather than silently reporting full success.
- **Pipeline run tracking** — every execution (success, partial, or failed) is logged to a `pipeline_runs` table with row counts, duration, and error messages — a basic but real observability layer.
- **Cross-task data handoff via Parquet, not XCom payloads** — each Airflow task writes its DataFrame output to a Parquet file and passes only the file path through XCom. Airflow's XCom backend is designed for small values, not multi-thousand-row datasets; this keeps the metadata database lightweight and mirrors how data handoff is handled in production orchestration.
- **Isolated Airflow environment, isolated task execution** — Airflow itself runs in its own virtual environment (`.venv-airflow`), separate from the project's main environment (`.venv`). This avoids a real dependency conflict: Airflow's supported SQLAlchemy version trails the project's SQLAlchemy 2.0.x, so the two cannot safely share a single dependency set. Task *execution* goes a step further: every DAG task runs via `ExternalPythonOperator`, which spawns a subprocess using the project's own `.venv` interpreter rather than Airflow's. This means Airflow's environment never needs pandas, yfinance, or the project's SQLAlchemy version at all — it only needs its own dependencies. Task functions are fully self-contained (all imports inside the function body, project root added to `sys.path` explicitly) since the external subprocess shares no state with the DAG file's top-level scope.
- **Full run auditing, including failures** — a final `record_run_task`, with `trigger_rule="all_done"`, always executes regardless of whether upstream tasks succeeded or failed. It inspects each task's final state and writes one row to `pipeline_runs` per DAG run — success, partial success, or failure, with an `error_message` describing which tasks failed. A run-tracking table that only logs successes isn't very useful for debugging.
- **Database-level read-only enforcement for the API** — the FastAPI service connects to Postgres as `api_reader`, a dedicated role with `SELECT`-only grants on `ohlcv`, `ohlcv_features`, and `pipeline_runs` (see `db/create_api_role.sql`). This is deliberately separate from the full-privilege user `PostgresLoader` uses for writes. The API's own code also defines no write endpoints and uses parameterized queries throughout — but the database-level restriction is the layer that holds even if application code has a bug, rather than relying on "the code only does SELECT" as the only line of defense.
- **404 vs. empty list, treated as genuinely different states** — `GET /ohlcv/{ticker}` and `GET /features/{ticker}` return 404 if the ticker has never appeared in the database at all, but 200 with an empty `data` list if the ticker exists but no rows fall within a requested date range. These represent different situations for a client -- "this resource doesn't exist" is not the same as "this resource has no data matching your filter."

## Features Engineered

| Feature             | Description                                         |
| ------------------- | --------------------------------------------------- |
| `daily_return`      | Day-over-day percentage price change                |
| `sma_20` / `sma_50` | 20-day and 50-day simple moving averages (trend)    |
| `volatility_20d`    | 20-day rolling standard deviation of returns (risk) |
| `volume_ma_20`      | 20-day average trading volume (liquidity)           |
| `rsi_14`            | 14-day Relative Strength Index (momentum)           |

## Project Structure

```
market-data-pipeline/
├── api/
│   ├── main.py                     # FastAPI app, mounts routers, health check
│   ├── dependencies.py             # Read-only DB session dependency (api_reader role)
│   ├── schemas/
│   │   ├── market.py                # Pydantic models: tickers, ohlcv, features
│   │   └── pipeline_runs.py         # Pydantic models: pipeline run audit records
│   └── routers/
│       ├── market.py                # /tickers, /ohlcv/{ticker}, /features/{ticker}
│       └── pipeline_runs.py         # /pipeline-runs
├── airflow/
│   └── dags/
│       └── market_etl_dag.py       # DAG definition: extract -> transform -> load
├── config/
│   └── settings.py                 # Centralized Pydantic settings, reads .env
├── etl/
│   ├── extract/
│   │   ├── base.py                 # Abstract base extractor
│   │   └── yfinance_extractor.py
│   ├── transform/
│   │   └── ohlcv_transformer.py
│   └── load/
│       └── postgres_loader.py
├── db/
│   ├── init.sql                    # Schema, partitions, indexes
│   └── create_api_role.sql         # Read-only role for the API (api_reader)
├── tests/
│   └── unit/
│       ├── test_transformer.py
│       └── test_api.py             # API tests, DB session mocked via dependency_overrides
├── docker-compose.yml
└── .env.example
```

Note: `airflow/airflow.db`, `airflow/logs/`, and `.venv-airflow/` are generated locally by Airflow and are not tracked in version control — only the DAG definition itself is committed.

## API Endpoints

All endpoints are read-only (`GET` only — no write operations exist at any layer, see Key Design Decisions above).

| Endpoint                 | Description                                                          |
| ------------------------ | -------------------------------------------------------------------- |
| `GET /health`            | Liveness + DB connectivity check                                     |
| `GET /tickers`           | List all distinct tickers in the database                            |
| `GET /ohlcv/{ticker}`    | Raw OHLCV data; supports `start_date`, `end_date`, `limit`, `offset` |
| `GET /features/{ticker}` | Engineered features; same filtering/pagination as above              |
| `GET /pipeline-runs`     | Recent pipeline run history from the audit table, including failures |

Interactive documentation (Swagger UI) is available at `/docs` once the server is running.

## Setup

### Prerequisites
- Python 3.12+
- Docker & Docker Compose

### Installation (main pipeline)

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

### Installation (Airflow orchestration)

Airflow runs in its own isolated virtual environment due to a dependency conflict between Airflow's required SQLAlchemy version and the main project's SQLAlchemy 2.0.x.

```bash
python3 -m venv .venv-airflow
source .venv-airflow/bin/activate

AIRFLOW_VERSION=2.9.1
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Set AIRFLOW_HOME automatically on every activation of this venv
echo 'export AIRFLOW_HOME='"$(pwd)"'/airflow' >> .venv-airflow/bin/activate
source .venv-airflow/bin/activate  # re-source to pick up AIRFLOW_HOME

airflow db init
airflow users create --username admin --firstname <first> --lastname <last> --role Admin --email <email>
```

### Installation (API read-only role)

The API connects as a dedicated Postgres role (`api_reader`) with `SELECT`-only privileges, kept separate from the full-privilege user the ETL pipeline uses for writes.

```bash
# Generate a strong password rather than using a guessable one, even locally
python -c "import secrets; print(secrets.token_urlsafe(24))"

# Fill that password into db/create_api_role.sql, then run it:
docker cp db/create_api_role.sql market_postgres:/tmp/create_api_role.sql
docker exec -it market_postgres psql -U <postgres_user> -d market_data -f /tmp/create_api_role.sql

# Add the same password to .env:
# API_DB_PASSWORD=<the password you generated>
```

### Running the Pipeline

**Manually (without Airflow):**
```bash
source .venv/bin/activate
python -m etl.load.postgres_loader
```

**Via Airflow (orchestrated):**
```bash
source .venv-airflow/bin/activate

# Terminal 1
airflow webserver -p 8080

# Terminal 2
airflow scheduler
```
Then visit `http://localhost:8080`, log in, and trigger the `market_data_pipeline` DAG — or let it run on its `@daily` schedule.

**Testing a full DAG run from the CLI (bypasses the scheduler, useful for debugging):**
```bash
airflow dags test market_data_pipeline <YYYY-MM-DD>
```

**Via the API (query processed data):**
```bash
source .venv/bin/activate
uvicorn api.main:app --reload
```
Then visit `http://localhost:8000/docs` for interactive API documentation, or query directly:
```bash
curl http://localhost:8000/health
curl http://localhost:8000/tickers
curl "http://localhost:8000/ohlcv/AAPL?start_date=2026-06-01&end_date=2026-06-30&limit=50"
curl http://localhost:8000/pipeline-runs
```

### Running Tests

```bash
pip install -r requirements-dev.txt
pytest tests/unit/ -v
```

## Known Issues

- **Stale proxy environment variables** can cause `yfinance` to fail instantly with a misleading "possibly delisted" error for every ticker. If extraction fails in under 1 second for all tickers, check `env | grep -i proxy` and run `unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY`.
- **`duration_sec` is only accurate for real (scheduled or UI-triggered) DAG runs.** It's calculated from `dag_run.start_date`, which for `airflow dags test <date>` CLI runs is anchored to the backdated logical/execution date (midnight), not the actual wall-clock time the test was run. This can produce inflated durations (hours instead of seconds) for CLI test runs specifically. Runs triggered through the scheduler or the web UI report accurate durations.
- **Airflow runs locally, not on always-on infrastructure.** The scheduler only fires scheduled runs while the host machine is on. The FastAPI query layer and PostgreSQL database are deployed to Render and always available — only the ETL ingestion step requires a local machine to run.

## Roadmap

- [x] Multi-ticker extraction with retry logic
- [x] Technical indicator feature engineering
- [x] Partitioned PostgreSQL storage with idempotent loads
- [x] Unit tests for transformation logic
- [x] Airflow DAG for scheduled orchestration
- [x] Task execution isolated to main project venv via `ExternalPythonOperator`
- [x] Full run auditing (`pipeline_runs`) wired into the DAG, including failure states
- [x] FastAPI endpoints for querying processed data
- [x] Read-only database role enforcing least-privilege access for the API
- [x] API test coverage (mocked DB session, no live Postgres required)
- [ ] Integration tests for extract/load layers
- [x] Cloud deployment (Render — PostgreSQL + FastAPI web service)

## Author

Satar Shamsi — [LinkedIn](https://linkedin.com/in/satar-shamsi/) · [GitHub](https://github.com/satar-shams)