# api/schemas/market.py

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class TickerListResponse(BaseModel):
    tickers: List[str]
    count: int


class OHLCVRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticker: str
    timestamp: date
    open: float
    high: float
    low: float
    close: float
    volume: int


class OHLCVResponse(BaseModel):
    ticker: str
    count: int
    limit: int
    offset: int
    data: List[OHLCVRecord]


class FeatureRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticker: str
    timestamp: date
    daily_return: Optional[float]
    sma_20: Optional[float]
    sma_50: Optional[float]
    volatility_20d: Optional[float]
    volume_ma_20: Optional[float]
    rsi_14: Optional[float]


class FeaturesResponse(BaseModel):
    ticker: str
    count: int
    limit: int
    offset: int
    data: List[FeatureRecord]