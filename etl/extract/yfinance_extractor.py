# etl/extract/yfinance_extractor.py

import logging
import pandas as pd
import yfinance as yf
from typing import List, Optional

from etl.extract.base import BaseExtractor
from config.settings import settings

logger = logging.getLogger(__name__)


class YFinanceExtractor(BaseExtractor):
    """
    Concrete extractor implementation using the yfinance library.
    Pulls OHLCV data for multiple tickers over a configured lookback window.
    """

    def __init__(self, tickers: List[str] = None, lookback_days: int = None):
        super().__init__(
            tickers=tickers or settings.tickers_list,
            lookback_days=lookback_days or settings.lookback_days,
        )

    def extract_ticker(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Downloads OHLCV data for a single ticker via yfinance.
        Returns None if yfinance returns empty data (bad ticker, delisted, etc.)
        """
        raw = yf.download(
            ticker,
            start=self.start_date.date(),
            end=self.end_date.date(),
            progress=False,   # suppresses yfinance's console progress bar
            auto_adjust=True, # adjusts OHLCV for splits and dividends
        )

        if raw.empty:
            return None

        # yfinance returns a MultiIndex column when auto_adjust=True
        # Flatten it to single-level columns
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        df = raw[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.columns = ["open", "high", "low", "close", "volume"]  # enforce lowercase
        df["ticker"] = ticker
        df["timestamp"] = df.index                               # date index → column
        df = df.reset_index(drop=True)

        # Schema validation before returning
        if not self.validate_schema(df):
            logger.error("Schema validation failed for ticker: %s", ticker)
            return None

        return df


# ── Entrypoint for manual runs ───────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    extractor = YFinanceExtractor()
    df = extractor.extract_all()

    print(f"\n✓ Extraction complete: {len(df)} rows, {df['ticker'].nunique()} tickers")
    print(df.groupby("ticker")[["open", "close", "volume"]].tail(2))