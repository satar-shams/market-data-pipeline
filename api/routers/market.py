# api/routers/market.py

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.market import (
    TickerListResponse,
    OHLCVResponse,
    OHLCVRecord,
    FeaturesResponse,
    FeatureRecord,
)

router = APIRouter(tags=["market"])


def _ticker_exists(db: Session, table: str, ticker: str) -> bool:
    result = db.execute(
        text(f"SELECT 1 FROM market_data.{table} WHERE ticker = :ticker LIMIT 1"),
        {"ticker": ticker},
    )
    return result.first() is not None


@router.get("/tickers", response_model=TickerListResponse)
def list_tickers(db: Session = Depends(get_db)):
    """
    Returns every distinct ticker currently present in market_data.ohlcv.
    """
    result = db.execute(
        text("SELECT DISTINCT ticker FROM market_data.ohlcv ORDER BY ticker")
    )
    tickers = [row[0] for row in result]
    return TickerListResponse(tickers=tickers, count=len(tickers))


@router.get("/ohlcv/{ticker}", response_model=OHLCVResponse)
def get_ohlcv(
    ticker: str,
    start_date: Optional[date] = Query(None, description="Inclusive start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Inclusive end date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000, description="Max rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip, for pagination"),
    db: Session = Depends(get_db),
):
    """
    Raw OHLCV data for a single ticker, optionally filtered by date range.

    Returns 404 if the ticker has never appeared in the database at all.
    Returns 200 with an empty list if the ticker exists but no rows fall
    within the requested date range -- these are different situations:
    "this resource doesn't exist" vs "this resource has no matching data".
    """
    ticker = ticker.upper()

    if not _ticker_exists(db, "ohlcv", ticker):
        raise HTTPException(
            status_code=404,
            detail=f"Ticker '{ticker}' not found in market_data.ohlcv",
        )

    query = """
        SELECT ticker, timestamp, open, high, low, close, volume
        FROM market_data.ohlcv
        WHERE ticker = :ticker
    """
    params = {"ticker": ticker, "limit": limit, "offset": offset}

    if start_date:
        query += " AND timestamp >= :start_date"
        params["start_date"] = start_date
    if end_date:
        query += " AND timestamp <= :end_date"
        params["end_date"] = end_date

    query += " ORDER BY timestamp DESC LIMIT :limit OFFSET :offset"

    result = db.execute(text(query), params)
    rows = [OHLCVRecord.model_validate(row._mapping) for row in result]

    return OHLCVResponse(ticker=ticker, count=len(rows), limit=limit, offset=offset, data=rows)


@router.get("/features/{ticker}", response_model=FeaturesResponse)
def get_features(
    ticker: str,
    start_date: Optional[date] = Query(None, description="Inclusive start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Inclusive end date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000, description="Max rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip, for pagination"),
    db: Session = Depends(get_db),
):
    """
    Engineered features for a single ticker, optionally filtered by date range.
    Same 404-vs-empty-list distinction as /ohlcv/{ticker}.
    """
    ticker = ticker.upper()

    if not _ticker_exists(db, "ohlcv_features", ticker):
        raise HTTPException(
            status_code=404,
            detail=f"Ticker '{ticker}' not found in market_data.ohlcv_features",
        )

    query = """
        SELECT ticker, timestamp, daily_return, sma_20, sma_50,
               volatility_20d, volume_ma_20, rsi_14
        FROM market_data.ohlcv_features
        WHERE ticker = :ticker
    """
    params = {"ticker": ticker, "limit": limit, "offset": offset}

    if start_date:
        query += " AND timestamp >= :start_date"
        params["start_date"] = start_date
    if end_date:
        query += " AND timestamp <= :end_date"
        params["end_date"] = end_date

    query += " ORDER BY timestamp DESC LIMIT :limit OFFSET :offset"

    result = db.execute(text(query), params)
    rows = [FeatureRecord.model_validate(row._mapping) for row in result]

    return FeaturesResponse(ticker=ticker, count=len(rows), limit=limit, offset=offset, data=rows)