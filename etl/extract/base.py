# etl/extract/base.py
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import logging
import pandas as pd
from typing import List, Optional

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """
    Abstract base class for all market data extractors.

    Any new data source (Alpha Vantage, Polygon.io, etc.) must implement
    this interface. This ensures the ETL pipeline can swap data sources
    without modifying downstream transform/load logic.
    """

    def __init__(self, tickers: List[str], lookback_days: int = 365):
        """
        Args:
            tickers:       List of ticker symbols to extract (e.g., ['AAPL', 'MSFT'])
            lookback_days: Number of calendar days of history to pull
        """
        if not tickers:
            raise ValueError("Ticker list cannot be empty.")
        if lookback_days <= 0:
            raise ValueError("lookback_days must be a positive integer.")

        self.tickers = [t.upper().strip() for t in tickers]
        self.lookback_days = lookback_days
        self.start_date: datetime = datetime.utcnow() - timedelta(days=lookback_days)
        self.end_date: datetime = datetime.utcnow()

        logger.info(
            "Extractor initialized | tickers=%s | period=%s to %s",
            self.tickers,
            self.start_date.date(),
            self.end_date.date(),
        )

    @abstractmethod
    def extract_ticker(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data for a single ticker.

        Args:
            ticker: Ticker symbol string (e.g., 'AAPL')

        Returns:
            DataFrame with columns [open, high, low, close, volume, ticker, timestamp]
            or None if extraction fails.
        """
        raise NotImplementedError

    def extract_all(self) -> pd.DataFrame:
        """
        Iterates over all configured tickers and concatenates results.
        Skips failed tickers with a warning rather than crashing the pipeline.

        Returns:
            Combined DataFrame for all successfully extracted tickers.
        """
        results = []

        for ticker in self.tickers:
            logger.info("Extracting ticker: %s", ticker)
            try:
                df = self.extract_ticker(ticker)
                if df is None or df.empty:
                    logger.warning("No data returned for ticker: %s — skipping.", ticker)
                    continue
                results.append(df)
                logger.info("Extracted %d rows for %s", len(df), ticker)
            except Exception as e:
                logger.error(
                    "Extraction failed for %s: %s", ticker, str(e), exc_info=True
                )
                continue  # Pipeline continues with remaining tickers

        if not results:
            raise RuntimeError(
                f"Extraction produced no data for any ticker: {self.tickers}"
            )

        combined = pd.concat(results, ignore_index=True)
        logger.info(
            "Extraction complete | total rows=%d | tickers_succeeded=%d/%d",
            len(combined),
            len(results),
            len(self.tickers),
        )
        return combined

    @staticmethod
    def validate_schema(df: pd.DataFrame) -> bool:
        """
        Validates that the DataFrame conforms to the expected OHLCV schema.
        Called before loading to catch upstream changes early.
        """
        required_columns = {"open", "high", "low", "close", "volume", "ticker", "timestamp"}
        missing = required_columns - set(df.columns)
        if missing:
            logger.error("Schema validation failed. Missing columns: %s", missing)
            return False
        return True