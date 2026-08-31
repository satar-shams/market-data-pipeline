# tests/unit/test_api.py
"""
Unit tests for the FastAPI query layer (api/main.py and routers).

These tests mock the database session via FastAPI's dependency_overrides
rather than hitting a real Postgres instance. This keeps the suite fast
and runnable without Docker being up -- matching the isolation level of
the existing pipeline unit tests (tests/unit/test_transformer.py).

Integration-level tests against a real database are a deliberate follow-up,
not covered here.
"""

from datetime import date, datetime
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.dependencies import get_db


class FakeRow:
    """Mimics a SQLAlchemy Row well enough for our code's use of row._mapping."""

    def __init__(self, mapping: dict):
        self._mapping = mapping


class FakeResult:
    """Mimics a SQLAlchemy CursorResult: iterable, plus .first()."""

    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def client(mock_db):
    def _override_get_db():
        yield mock_db

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# ── /health ───────────────────────────────────────────────────────────────

def test_health_ok(client, mock_db):
    mock_db.execute.return_value = FakeResult([])
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "database": "connected"}


def test_health_db_unreachable(client, mock_db):
    mock_db.execute.side_effect = Exception("connection refused")
    resp = client.get("/health")
    # The endpoint itself should still respond 200 -- it reports DB status
    # in the body, it doesn't fail the whole request just because Postgres
    # is unreachable. That distinction matters: a health check that crashes
    # when the thing it's checking is down is not a useful health check.
    assert resp.status_code == 200
    assert resp.json()["database"] == "unreachable"


# ── /tickers ──────────────────────────────────────────────────────────────

def test_list_tickers(client, mock_db):
    mock_db.execute.return_value = FakeResult([("AAPL",), ("MSFT",)])
    resp = client.get("/tickers")
    assert resp.status_code == 200
    body = resp.json()
    assert body["tickers"] == ["AAPL", "MSFT"]
    assert body["count"] == 2


# ── /ohlcv/{ticker} ──────────────────────────────────────────────────────

def test_get_ohlcv_ticker_not_found(client, mock_db):
    # _ticker_exists query returns no rows -> .first() is None -> 404
    mock_db.execute.return_value = FakeResult([])
    resp = client.get("/ohlcv/ZZZZ")
    assert resp.status_code == 404
    assert "ZZZZ" in resp.json()["detail"]


def test_get_ohlcv_success(client, mock_db):
    exists_result = FakeResult([FakeRow({"exists": 1})])
    data_result = FakeResult([
        FakeRow({
            "ticker": "AAPL", "timestamp": date(2026, 7, 1),
            "open": 100.0, "high": 105.0, "low": 99.0,
            "close": 104.0, "volume": 1000,
        })
    ])
    # Two execute() calls happen in order: the existence check, then the
    # real data query -- side_effect as a list returns them in sequence.
    mock_db.execute.side_effect = [exists_result, data_result]

    resp = client.get("/ohlcv/AAPL?limit=5")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ticker"] == "AAPL"
    assert body["count"] == 1
    assert body["data"][0]["close"] == 104.0


def test_get_ohlcv_pagination_bounds(client, mock_db):
    # limit is constrained to 1-1000 via Query(ge=1, le=1000) -- FastAPI
    # should reject out-of-range values with 422 before our code even runs.
    resp = client.get("/ohlcv/AAPL?limit=0")
    assert resp.status_code == 422

    resp = client.get("/ohlcv/AAPL?limit=5000")
    assert resp.status_code == 422


# ── /features/{ticker} ───────────────────────────────────────────────────

def test_get_features_not_found(client, mock_db):
    mock_db.execute.return_value = FakeResult([])
    resp = client.get("/features/ZZZZ")
    assert resp.status_code == 404


def test_get_features_success(client, mock_db):
    exists_result = FakeResult([FakeRow({"exists": 1})])
    data_result = FakeResult([
        FakeRow({
            "ticker": "AAPL", "timestamp": date(2026, 7, 1),
            "daily_return": 0.012, "sma_20": 290.5, "sma_50": 285.0,
            "volatility_20d": 0.018, "volume_ma_20": 55000000, "rsi_14": 62.3,
        })
    ])
    mock_db.execute.side_effect = [exists_result, data_result]

    resp = client.get("/features/AAPL?limit=5")
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"][0]["rsi_14"] == 62.3


# ── /pipeline-runs ───────────────────────────────────────────────────────

def test_pipeline_runs_list(client, mock_db):
    mock_db.execute.return_value = FakeResult([
        FakeRow({
            "id": 1, "run_at": datetime(2026, 7, 1, 10, 0, 0),
            "tickers": ["AAPL", "MSFT"], "rows_extracted": 100,
            "rows_loaded": 100, "status": "success",
            "error_message": None, "duration_sec": 5.2,
        })
    ])
    resp = client.get("/pipeline-runs")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["data"][0]["status"] == "success"


def test_pipeline_runs_includes_failures(client, mock_db):
    # Failed runs should be returned just like successful ones -- this is
    # the whole point of record_run_task using trigger_rule="all_done".
    mock_db.execute.return_value = FakeResult([
        FakeRow({
            "id": 5, "run_at": datetime(2026, 7, 2, 20, 31, 8),
            "tickers": ["AAPL"], "rows_extracted": 0,
            "rows_loaded": 0, "status": "failed",
            "error_message": "task_states={'extract': 'failed'}",
            "duration_sec": 73868.1,
        })
    ])
    resp = client.get("/pipeline-runs")
    assert resp.status_code == 200
    assert resp.json()["data"][0]["status"] == "failed"