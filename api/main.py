# api/main.py

import logging

from fastapi import FastAPI, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
from pydantic import BaseModel

from api.dependencies import get_db
from api.routers import market, pipeline_runs
from config.settings import settings

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

app = FastAPI(
    title="Market Data Pipeline API",
    description="Read-only query layer over market_data.ohlcv, "
                 "market_data.ohlcv_features, and market_data.pipeline_runs.",
    version="0.1.0",
)

app.include_router(market.router)
app.include_router(pipeline_runs.router)


class HealthResponse(BaseModel):
    status: str
    database: str


@app.get("/health", response_model=HealthResponse, tags=["health"])
def health_check(db: Session = Depends(get_db)):
    """
    Liveness/readiness check -- confirms the API process is up AND
    that it can actually reach Postgres, not just that FastAPI is running.
    """
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "unreachable"

    return HealthResponse(status="ok", database=db_status)