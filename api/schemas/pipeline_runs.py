# api/schemas/pipeline_runs.py

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class PipelineRunRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_at: datetime
    tickers: List[str]
    rows_extracted: int
    rows_loaded: int
    status: str
    error_message: Optional[str]
    duration_sec: float


class PipelineRunsResponse(BaseModel):
    count: int
    data: List[PipelineRunRecord]