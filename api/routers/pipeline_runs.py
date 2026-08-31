# api/routers/pipeline_runs.py

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.pipeline_runs import PipelineRunsResponse, PipelineRunRecord

router = APIRouter(prefix="/pipeline-runs", tags=["pipeline-runs"])


@router.get("", response_model=PipelineRunsResponse)
def list_pipeline_runs(
    limit: int = Query(20, ge=1, le=200, description="Max rows to return"),
    db: Session = Depends(get_db),
):
    """
    Most recent pipeline runs, newest first. Backed by market_data.pipeline_runs,
    which is written by record_run_task in the Airflow DAG on every run --
    success, partial success, or failure.
    """
    result = db.execute(
        text("""
            SELECT id, run_at, tickers, rows_extracted, rows_loaded,
                   status, error_message, duration_sec
            FROM market_data.pipeline_runs
            ORDER BY id DESC
            LIMIT :limit
        """),
        {"limit": limit},
    )
    rows = [PipelineRunRecord.model_validate(row._mapping) for row in result]
    return PipelineRunsResponse(count=len(rows), data=rows)