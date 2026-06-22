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
    
    def load_features(self, df: pd.DataFrame) -> int:
        """
        Upserts engineered features into market_data.ohlcv_features.
        Same idempotency pattern as load() — safe to re-run.

        Args:
            df: DataFrame with columns
                [ticker, timestamp, daily_return, sma_20, sma_50,
                 volatility_20d, volume_ma_20, rsi_14]

        Returns:
            Number of rows actually inserted.
        """
        if df.empty:
            logger.warning("Empty DataFrame passed to load_features — nothing to load.")
            return 0

        required = {
            "ticker", "timestamp", "daily_return", "sma_20",
            "sma_50", "volatility_20d", "volume_ma_20", "rsi_14"
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing feature columns before load: {missing}")

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.date
        df = df.drop_duplicates(subset=["ticker", "timestamp"])

        rows_before = self._count_rows(table="ohlcv_features")

        try:
            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    conn.execute(
                        text("""
                            INSERT INTO market_data.ohlcv_features
                                (ticker, timestamp, daily_return, sma_20, sma_50,
                                 volatility_20d, volume_ma_20, rsi_14)
                            VALUES
                                (:ticker, :timestamp, :daily_return, :sma_20, :sma_50,
                                 :volatility_20d, :volume_ma_20, :rsi_14)
                            ON CONFLICT (ticker, timestamp) DO NOTHING
                        """),
                        {
                            "ticker":         row["ticker"],
                            "timestamp":      row["timestamp"],
                            "daily_return":   row["daily_return"],
                            "sma_20":         row["sma_20"],
                            "sma_50":         row["sma_50"],
                            "volatility_20d": row["volatility_20d"],
                            "volume_ma_20":   row["volume_ma_20"],
                            "rsi_14":         row["rsi_14"],
                        }
                    )

        except SQLAlchemyError as e:
            logger.error("Database error during feature load: %s", str(e), exc_info=True)
            raise

        rows_after = self._count_rows(table="ohlcv_features")
        rows_inserted = rows_after - rows_before

        logger.info(
            "Feature load complete | rows_inserted=%d | rows_skipped=%d | total_in_db=%d",
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
    
    def _count_rows(self, table: str = "ohlcv") -> int:
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM market_data.{table}"))
            return result.scalar()

# ── Entrypoint for manual runs ────────────────────────────────────────────────
if __name__ == "__main__":
    import time
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    from etl.extract.yfinance_extractor import YFinanceExtractor
    from etl.transform.ohlcv_transformer import OHLCVTransformer

    start = time.time()
    status = "success"
    error_msg = None
    raw_df = pd.DataFrame()
    rows_loaded_raw = 0
    rows_loaded_features = 0

    try:
        # ── Extract ──────────────────────────────────────────────────────────
        extractor = YFinanceExtractor()
        raw_df = extractor.extract_all()

        # ── Load raw OHLCV ───────────────────────────────────────────────────
        loader = PostgresLoader()
        rows_loaded_raw = loader.load(raw_df)

        # ── Transform ────────────────────────────────────────────────────────
        transformer = OHLCVTransformer()
        features_df = transformer.transform(raw_df)

        # ── Load features ────────────────────────────────────────────────────
        rows_loaded_features = loader.load_features(features_df)

    except Exception as e:
        status = "failed"
        error_msg = str(e)
        logger.error("Pipeline failed: %s", str(e), exc_info=True)
        raise

    finally:
        duration = time.time() - start
        PostgresLoader().record_pipeline_run(
            tickers=settings.tickers_list,
            rows_extracted=len(raw_df),
            rows_loaded=rows_loaded_raw + rows_loaded_features,
            status=status,
            duration_sec=duration,
            error_message=error_msg,
        )