"""
market_data_pipeline_dag.py

Orchestrates the existing extract -> transform -> load pipeline
(YFinanceExtractor, OHLCVTransformer, PostgresLoader) as an Airflow DAG.

This DAG does not reimplement pipeline logic — it only sequences
and schedules the classes already built and tested in Phase 1.
"""

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Make project root importable ────────────────────────────────────────────
# This DAG file lives in airflow/dags/, but etl/ and config/ live at the
# project root, one level above airflow/. We add that root to sys.path
# so `from etl.extract... import ...` works inside Airflow's process.
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Temp storage for passing DataFrames between tasks (as Parquet files)
TMP_DIR = "/tmp/market_data_pipeline"
os.makedirs(TMP_DIR, exist_ok=True)


# ── Task functions ───────────────────────────────────────────────────────────

def extract_task(**context):
    """
    Runs YFinanceExtractor, saves the raw OHLCV DataFrame to a Parquet file,
    and returns the file path (Airflow auto-pushes this to XCom).
    """
    from etl.extract.yfinance_extractor import YFinanceExtractor

    extractor = YFinanceExtractor()
    raw_df = extractor.extract_all()

    # Use the DAG run_id to make the filename unique per run
    run_id = context["run_id"]
    path = os.path.join(TMP_DIR, f"raw_{run_id}.parquet")
    raw_df.to_parquet(path, index=False)

    print(f"Extracted {len(raw_df)} rows -> {path}")
    return path


def load_raw_task(**context):
    """
    Pulls the raw data path from extract_task via XCom, loads it into Postgres.
    """
    import pandas as pd
    from etl.load.postgres_loader import PostgresLoader

    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="extract_task")

    raw_df = pd.read_parquet(raw_path)
    loader = PostgresLoader()
    rows_inserted = loader.load(raw_df)

    print(f"Loaded {rows_inserted} raw rows into market_data.ohlcv")


def transform_task(**context):
    """
    Pulls the raw data path from extract_task, runs OHLCVTransformer,
    saves the features DataFrame to Parquet, returns that path.
    """
    import pandas as pd
    from etl.transform.ohlcv_transformer import OHLCVTransformer

    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="extract_task")

    raw_df = pd.read_parquet(raw_path)
    transformer = OHLCVTransformer()
    features_df = transformer.transform(raw_df)

    run_id = context["run_id"]
    features_path = os.path.join(TMP_DIR, f"features_{run_id}.parquet")
    features_df.to_parquet(features_path, index=False)

    print(f"Transformed -> {len(features_df)} rows -> {features_path}")
    return features_path


def load_features_task(**context):
    """
    Pulls the features data path from transform_task, loads it into Postgres.
    """
    import pandas as pd
    from etl.load.postgres_loader import PostgresLoader

    ti = context["ti"]
    features_path = ti.xcom_pull(task_ids="transform_task")

    features_df = pd.read_parquet(features_path)
    loader = PostgresLoader()
    rows_inserted = loader.load_features(features_df)

    print(f"Loaded {rows_inserted} feature rows into market_data.ohlcv_features")


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
    schedule="@daily",     # run once per day
    catchup=False,         # do NOT backfill every day since start_date — only run going forward
    tags=["etl", "market-data"],
) as dag:

    t1 = PythonOperator(task_id="extract_task", python_callable=extract_task)
    t2 = PythonOperator(task_id="load_raw_task", python_callable=load_raw_task)
    t3 = PythonOperator(task_id="transform_task", python_callable=transform_task)
    t4 = PythonOperator(task_id="load_features_task", python_callable=load_features_task)

    # Dependency graph:
    # extract -> load_raw
    # extract -> transform -> load_features
    t1 >> t2
    t1 >> t3 >> t4