# etl/load/postgres_loader.py

import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from config.settings import settings

logger = logging.getLogger(__name__)


class PostgresLoader:
    """
    Loads OHLCV DataFrames into the partitioned market_data.ohlcv table.
    Uses upsert logic (INSERT ... ON CONFLICT DO NOTHING) for idempotency —
    running the pipeline twice on the same data produces no duplicates.
    """

    def __init__(self, engine: Engine = None):
        self.engine = engine or create_engine(
            settings.database_url,
            pool_pre_ping=True,    # verifies connection is alive before using it
            pool_size=5,
            max_overflow=10,
        )
        logger.info("PostgresLoader initialized | db=%s", settings.postgres_db)

    def load(self, df: pd.DataFrame) -> int:
        """
        Upserts a DataFrame into market_data.ohlcv.
        Skips rows that already exist (same ticker + timestamp).

        Args:
            df: DataFrame with columns [ticker, timestamp, open, high, low, close, volume]

        Returns:
            Number of rows actually inserted.
        """
        if df.empty:
            logger.warning("Empty DataFrame passed to loader — nothing to load.")
            return 0

        if not self._validate(df):
            raise ValueError("DataFrame failed schema validation before load.")

        df = self._prepare(df)
        rows_before = self._count_rows()

        try:
            with self.engine.begin() as conn:   # begin() auto-commits or rolls back
                for _, row in df.iterrows():
                    conn.execute(
                        text("""
                            INSERT INTO market_data.ohlcv
                                (ticker, timestamp, open, high, low, close, volume)
                            VALUES
                                (:ticker, :timestamp, :open, :high, :low, :close, :volume)
                            ON CONFLICT (ticker, timestamp) DO NOTHING
                        """),
                        {
                            "ticker":    row["ticker"],
                            "timestamp": row["timestamp"],
                            "open":      row["open"],
                            "high":      row["high"],
                            "low":       row["low"],
                            "close":     row["close"],
                            "volume":    int(row["volume"]),
                        }
                    )

        except SQLAlchemyError as e:
            logger.error("Database error during load: %s", str(e), exc_info=True)
            raise

        rows_after = self._count_rows()
        rows_inserted = rows_after - rows_before

        logger.info(
            "Load complete | rows_inserted=%d | rows_skipped=%d | total_in_db=%d",
            rows_inserted,
            len(df) - rows_inserted,
            rows_after,
        )
        return rows_inserted

    def record_pipeline_run(
        self,
        tickers: list,
        rows_extracted: int,
        rows_loaded: int,
        status: str,
        duration_sec: float,
        error_message: str = None,
    ) -> None:
        """
        Writes a record to pipeline_runs after every execution.
        Gives you a full audit trail of every pipeline run.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO market_data.pipeline_runs
                            (tickers, rows_extracted, rows_loaded, status, error_message, duration_sec)
                        VALUES
                            (:tickers, :rows_extracted, :rows_loaded, :status, :error_message, :duration_sec)
                    """),
                    {
                        "tickers":        tickers,
                        "rows_extracted": rows_extracted,
                        "rows_loaded":    rows_loaded,
                        "status":         status,
                        "error_message":  error_message,
                        "duration_sec":   round(duration_sec, 2),
                    }
                )
            logger.info("Pipeline run recorded | status=%s", status)
        except SQLAlchemyError as e:
            logger.error("Failed to record pipeline run: %s", str(e), exc_info=True)

    def _prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalizes DataFrame before insert:
        - Ensures timestamp is a date object, not datetime
        - Drops any duplicate ticker+timestamp pairs within the batch
        """
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.date
        df = df.drop_duplicates(subset=["ticker", "timestamp"])
        return df

    def _validate(self, df: pd.DataFrame) -> bool:
        required = {"ticker", "timestamp", "open", "high", "low", "close", "volume"}
        missing = required - set(df.columns)
        if missing:
            logger.error("Missing columns before load: %s", missing)
            return False
        return True

    def _count_rows(self) -> int:
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM market_data.ohlcv"))
            return result.scalar()


# ── Entrypoint for manual runs ────────────────────────────────────────────────
if __name__ == "__main__":
    import time
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    from etl.extract.yfinance_extractor import YFinanceExtractor

    start = time.time()
    status = "success"
    error_msg = None
    df = pd.DataFrame()

    try:
        extractor = YFinanceExtractor()
        df = extractor.extract_all()

        loader = PostgresLoader()
        rows_loaded = loader.load(df)

    except Exception as e:
        status = "failed"
        error_msg = str(e)
        logger.error("Pipeline failed: %s", str(e), exc_info=True)
        raise

    finally:
        duration = time.time() - start
        PostgresLoader().record_pipeline_run(
            tickers=settings.tickers_list,
            rows_extracted=len(df),
            rows_loaded=rows_loaded if status == "success" else 0,
            status=status,
            duration_sec=duration,
            error_message=error_msg,
        )