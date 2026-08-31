"""
market_etl_dag.py

Orchestrates the existing extract -> transform -> load pipeline
(YFinanceExtractor, OHLCVTransformer, PostgresLoader) as an Airflow DAG.

Task execution runs via ExternalPythonOperator, pointed at the project's
own .venv (not Airflow's environment). This keeps Airflow's dependency set
isolated from the pipeline's runtime dependencies (pandas, yfinance,
SQLAlchemy 2.0.x), avoiding the version conflicts that motivated running
Airflow in its own separate .venv-airflow in the first place.

XCom values needed by a task (e.g. an upstream file path) are resolved via
Jinja templating in op_kwargs, BEFORE the task function is serialized and
sent to the external subprocess. Task functions never call ti.xcom_pull()
themselves -- the external subprocess has no live connection to Airflow's
metadata DB, so that pattern is unreliable across the process boundary.
"""

import sys
import os
from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.python import ExternalPythonOperator

# ── Make project root importable (used only by code running in-process,
#    e.g. if we ever add a lightweight PythonOperator here) ────────────────
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Path to the PROJECT's own venv interpreter -- this is what actually runs
# the task logic, completely separate from Airflow's own .venv-airflow.
PROJECT_PYTHON = os.path.join(PROJECT_ROOT, ".venv", "bin", "python")

TMP_DIR = "/tmp/market_data_pipeline"
os.makedirs(TMP_DIR, exist_ok=True)


# ── Task functions ───────────────────────────────────────────────────────────
# Each of these must be fully self-contained: all imports happen INSIDE the
# function body, because the function is serialized and executed in a
# separate subprocess that does not share this file's top-level imports.

def extract_task(run_tag: str, project_root: str) -> str:
    import os
    import sys
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from etl.extract.yfinance_extractor import YFinanceExtractor

    extractor = YFinanceExtractor()
    raw_df = extractor.extract_all()

    tmp_dir = "/tmp/market_data_pipeline"
    os.makedirs(tmp_dir, exist_ok=True)
    path = os.path.join(tmp_dir, f"raw_{run_tag}.parquet")
    raw_df.to_parquet(path, index=False)

    print(f"Extracted {len(raw_df)} rows -> {path}")
    return path


def load_raw_task(raw_path: str, project_root: str) -> int:
    import sys
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    import pandas as pd
    from etl.load.postgres_loader import PostgresLoader

    raw_df = pd.read_parquet(raw_path)
    loader = PostgresLoader()
    rows_inserted = loader.load(raw_df)

    print(f"Loaded {rows_inserted} raw rows into market_data.ohlcv")
    return rows_inserted


def transform_task(raw_path: str, run_tag: str, project_root: str) -> str:
    import os
    import sys
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    import pandas as pd
    from etl.transform.ohlcv_transformer import OHLCVTransformer

    raw_df = pd.read_parquet(raw_path)
    transformer = OHLCVTransformer()
    features_df = transformer.transform(raw_df)

    tmp_dir = "/tmp/market_data_pipeline"
    features_path = os.path.join(tmp_dir, f"features_{run_tag}.parquet")
    features_df.to_parquet(features_path, index=False)

    print(f"Transformed -> {len(features_df)} rows -> {features_path}")
    return features_path


def load_features_task(features_path: str, project_root: str) -> int:
    import sys
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    import pandas as pd
    from etl.load.postgres_loader import PostgresLoader

    features_df = pd.read_parquet(features_path)
    loader = PostgresLoader()
    rows_inserted = loader.load_features(features_df)

    print(f"Loaded {rows_inserted} feature rows into market_data.ohlcv_features")
    return rows_inserted


def record_run_task(
    raw_path: str,
    features_path: str,
    rows_loaded_raw,
    rows_loaded_features,
    extract_state: str,
    load_raw_state: str,
    transform_state: str,
    load_features_state: str,
    run_start: str,
    project_root: str,
) -> None:
    """
    Always runs (trigger_rule='all_done'), regardless of whether upstream
    tasks succeeded or failed. Writes one row to market_data.pipeline_runs
    per DAG run, giving a full audit trail including failed runs --
    a run-tracking table that only records successes isn't very useful.
    """
    import sys
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from datetime import datetime as dt, timezone
    import pandas as pd
    from etl.load.postgres_loader import PostgresLoader
    from config.settings import settings

    states = [extract_state, load_raw_state, transform_state, load_features_state]
    if all(s == "success" for s in states):
        status = "success"
    elif any(s == "success" for s in states):
        status = "partial_success"
    else:
        status = "failed"

    rows_extracted = 0
    if raw_path:
        try:
            rows_extracted = len(pd.read_parquet(raw_path))
        except Exception:
            rows_extracted = 0

    rows_loaded = int(rows_loaded_raw or 0) + int(rows_loaded_features or 0)

    try:
        start = dt.fromisoformat(run_start)
        duration = (dt.now(timezone.utc) - start).total_seconds()
    except Exception:
        duration = 0.0

    error_message = None
    if status != "success":
        task_states = dict(
            extract=extract_state,
            load_raw=load_raw_state,
            transform=transform_state,
            load_features=load_features_state,
        )
        error_message = f"task_states={task_states}"

    loader = PostgresLoader()
    loader.record_pipeline_run(
        tickers=settings.tickers_list,
        rows_extracted=rows_extracted,
        rows_loaded=rows_loaded,
        status=status,
        duration_sec=duration,
        error_message=error_message,
    )
    print(f"Pipeline run recorded | status={status} | rows_extracted={rows_extracted} | rows_loaded={rows_loaded}")


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner": "satar",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="market_data_pipeline",
    description="Daily extract -> transform -> load pipeline for OHLCV market data",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "market-data"],
) as dag:

    t1 = ExternalPythonOperator(
        task_id="extract_task",
        python=PROJECT_PYTHON,
        python_callable=extract_task,
        op_kwargs={"run_tag": "{{ run_id }}", "project_root": PROJECT_ROOT},
    )

    t2 = ExternalPythonOperator(
        task_id="load_raw_task",
        python=PROJECT_PYTHON,
        python_callable=load_raw_task,
        op_kwargs={
            "raw_path": "{{ ti.xcom_pull(task_ids='extract_task') or '' }}",
            "project_root": PROJECT_ROOT,
        },
    )

    t3 = ExternalPythonOperator(
        task_id="transform_task",
        python=PROJECT_PYTHON,
        python_callable=transform_task,
        op_kwargs={
            "raw_path": "{{ ti.xcom_pull(task_ids='extract_task') or '' }}",
            "run_tag": "{{ run_id }}",
            "project_root": PROJECT_ROOT,
        },
    )

    t4 = ExternalPythonOperator(
        task_id="load_features_task",
        python=PROJECT_PYTHON,
        python_callable=load_features_task,
        op_kwargs={
            "features_path": "{{ ti.xcom_pull(task_ids='transform_task') or '' }}",
            "project_root": PROJECT_ROOT,
        },
    )

    t5 = ExternalPythonOperator(
        task_id="record_run_task",
        python=PROJECT_PYTHON,
        python_callable=record_run_task,
        trigger_rule="all_done",
        op_kwargs={
            "raw_path": "{{ ti.xcom_pull(task_ids='extract_task') or '' }}",
            "features_path": "{{ ti.xcom_pull(task_ids='transform_task') or '' }}",
            "rows_loaded_raw": "{{ ti.xcom_pull(task_ids='load_raw_task') or 0 }}",
            "rows_loaded_features": "{{ ti.xcom_pull(task_ids='load_features_task') or 0 }}",
            "extract_state": "{{ dag_run.get_task_instance('extract_task').state if dag_run.get_task_instance('extract_task') else 'unknown' }}",
            "load_raw_state": "{{ dag_run.get_task_instance('load_raw_task').state if dag_run.get_task_instance('load_raw_task') else 'unknown' }}",
            "transform_state": "{{ dag_run.get_task_instance('transform_task').state if dag_run.get_task_instance('transform_task') else 'unknown' }}",
            "load_features_state": "{{ dag_run.get_task_instance('load_features_task').state if dag_run.get_task_instance('load_features_task') else 'unknown' }}",
            "run_start": "{{ dag_run.start_date }}",
            "project_root": PROJECT_ROOT,
        },
    )

    # Dependency graph:
    # extract -> load_raw
    # extract -> transform -> load_features
    # [load_raw, load_features] -> record_run   (always, even on failure)
    t1 >> t2
    t1 >> t3 >> t4
    [t2, t4] >> t5