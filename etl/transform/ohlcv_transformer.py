# etl/transform/ohlcv_transformer.py

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class OHLCVTransformer:
    """
    Transforms raw OHLCV data into ML-ready features.
    All rolling/window calculations are computed per-ticker via groupby
    to avoid leaking data across different stocks.
    """

    def __init__(
        self,
        sma_windows: list = None,
        volatility_window: int = 20,
        volume_ma_window: int = 20,
        rsi_window: int = 14,
    ):
        self.sma_windows = sma_windows or [20, 50]
        self.volatility_window = volatility_window
        self.volume_ma_window = volume_ma_window
        self.rsi_window = rsi_window

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main entrypoint. Applies all feature engineering steps in sequence.

        Args:
            df: Raw OHLCV DataFrame with columns
                [ticker, timestamp, open, high, low, close, volume]

        Returns:
            DataFrame with original columns plus engineered features.
        """
        if df.empty:
            logger.warning("Empty DataFrame passed to transformer.")
            return df

        df = df.copy()
        df = df.sort_values(["ticker", "timestamp"]).reset_index(drop=True)

        df = self._add_returns(df)
        df = self._add_sma(df)
        df = self._add_volatility(df)
        df = self._add_volume_ma(df)
        df = self._add_rsi(df)

        rows_before = len(df)
        df = df.dropna().reset_index(drop=True)
        rows_dropped = rows_before - len(df)

        logger.info(
            "Transform complete | rows_in=%d | rows_out=%d | rows_dropped_nan=%d | features_added=%d",
            rows_before,
            len(df),
            rows_dropped,
            len(df.columns) - 7,   # 7 = original OHLCV columns
        )
        return df

    def _add_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Daily percentage return: (close_today - close_yesterday) / close_yesterday
        The single most important feature in any financial time series model.
        """
        df["daily_return"] = df.groupby("ticker")["close"].pct_change()
        return df

    def _add_sma(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Simple Moving Average — smooths price to reveal trend direction.
        SMA20 = short-term trend, SMA50 = medium-term trend.
        Price crossing above/below these is a classic trading signal.
        """
        for window in self.sma_windows:
            col_name = f"sma_{window}"
            df[col_name] = (
                df.groupby("ticker")["close"]
                .transform(lambda x: x.rolling(window=window).mean())
            )
        return df

    def _add_volatility(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rolling standard deviation of daily returns.
        Higher volatility = higher risk = directly useful for position sizing
        and risk-aware ML models.
        """
        df["volatility_20d"] = (
            df.groupby("ticker")["daily_return"]
            .transform(lambda x: x.rolling(window=self.volatility_window).std())
        )
        return df

    def _add_volume_ma(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        20-day average volume. Used to detect unusual trading activity —
        volume spikes often precede or confirm price moves.
        """
        df["volume_ma_20"] = (
            df.groupby("ticker")["volume"]
            .transform(lambda x: x.rolling(window=self.volume_ma_window).mean())
        )
        return df

    def _add_rsi(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Relative Strength Index (14-day default).
        Momentum oscillator: 0-100 scale.
        RSI > 70 = traditionally "overbought", RSI < 30 = "oversold".

        Formula:
            RS  = avg_gain / avg_loss
            RSI = 100 - (100 / (1 + RS))
        """
        def compute_rsi(close_prices: pd.Series, window: int) -> pd.Series:
            delta = close_prices.diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)

            avg_gain = gain.rolling(window=window).mean()
            avg_loss = loss.rolling(window=window).mean()

            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

            # When avg_loss is 0, RSI should be 100 (pure upward momentum)
            rsi = rsi.where(avg_loss != 0, 100)
            return rsi

        df["rsi_14"] = (
            df.groupby("ticker")["close"]
            .transform(lambda x: compute_rsi(x, self.rsi_window))
        )
        return df


# ── Entrypoint for manual runs ────────────────────────────────────────────────
if __name__ == "__main__":
    import logging as _logging
    from config.settings import settings
    from etl.extract.yfinance_extractor import YFinanceExtractor

    _logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    extractor = YFinanceExtractor()
    raw_df = extractor.extract_all()

    transformer = OHLCVTransformer()
    features_df = transformer.transform(raw_df)

    print(f"\n✓ Transform complete: {len(features_df)} rows, {len(features_df.columns)} columns")
    print(f"Columns: {list(features_df.columns)}")
    print("\nSample (AAPL, last 3 rows):")
    print(
        features_df[features_df["ticker"] == "AAPL"]
        .tail(3)[["timestamp", "close", "daily_return", "sma_20", "rsi_14"]]
    )