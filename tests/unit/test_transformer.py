# tests/unit/test_transformer.py

import pytest
import pandas as pd
import numpy as np

from etl.transform.ohlcv_transformer import OHLCVTransformer


@pytest.fixture
def sample_ohlcv_df() -> pd.DataFrame:
    """
    Builds a small, deterministic OHLCV dataset for one ticker.
    60 days of data — enough to fully compute SMA50 (needs 50 prior rows)
    with 10 valid rows left over to actually assert on.
    """
    dates = pd.date_range(start="2026-01-01", periods=60, freq="D")

    # Simple upward-trending price series — easy to reason about by hand
    close_prices = np.linspace(100, 159, 60)  # 100, 101, 102, ... 159

    df = pd.DataFrame({
        "ticker": ["TEST"] * 60,
        "timestamp": dates,
        "open": close_prices - 0.5,
        "high": close_prices + 1.0,
        "low": close_prices - 1.0,
        "close": close_prices,
        "volume": [1_000_000] * 60,
    })
    return df


@pytest.fixture
def multi_ticker_df(sample_ohlcv_df) -> pd.DataFrame:
    """
    Two tickers with identical date ranges but different price levels.
    Used to verify groupby logic doesn't leak data between tickers.
    """
    df_a = sample_ohlcv_df.copy()
    df_a["ticker"] = "AAA"

    df_b = sample_ohlcv_df.copy()
    df_b["ticker"] = "BBB"
    df_b["close"] = df_b["close"] * 10   # very different price scale

    return pd.concat([df_a, df_b], ignore_index=True)


class TestOHLCVTransformer:

    def test_transform_returns_dataframe(self, sample_ohlcv_df):
        """Sanity check: transform runs without error and returns a DataFrame."""
        transformer = OHLCVTransformer()
        result = transformer.transform(sample_ohlcv_df)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_transform_drops_nan_rows(self, sample_ohlcv_df):
        """
        SMA50 needs 50 prior rows. With 60 input rows, exactly 50 should
        be dropped (rows 0-49 lack enough history), leaving 10.
        """
        transformer = OHLCVTransformer()
        result = transformer.transform(sample_ohlcv_df)
        assert len(result) == 11

    def test_no_nan_values_remain(self, sample_ohlcv_df):
        """After transform, zero NaN values should exist anywhere in the output."""
        transformer = OHLCVTransformer()
        result = transformer.transform(sample_ohlcv_df)
        assert result.isnull().sum().sum() == 0

    def test_daily_return_calculation(self, sample_ohlcv_df):
        """
        First surviving row after dropna corresponds to original index 49
        (close=149), compared against index 48 (close=148).
        """
        transformer = OHLCVTransformer()
        result = transformer.transform(sample_ohlcv_df)

        expected_return = (149 - 148) / 148
        actual_return = result.iloc[0]["daily_return"]

        assert abs(actual_return - expected_return) < 1e-6

    def test_sma_20_calculation(self, sample_ohlcv_df):
        """
        First valid row = original index 49 (close=149).
        SMA20 = average of the trailing 20 closes: indices 30 through 49.
        """
        transformer = OHLCVTransformer()
        result = transformer.transform(sample_ohlcv_df)

        original_closes = sample_ohlcv_df["close"].values
        expected_sma20 = original_closes[30:50].mean()
        actual_sma20 = result.iloc[0]["sma_20"]

        assert abs(actual_sma20 - expected_sma20) < 1e-6

    def test_rsi_within_valid_range(self, sample_ohlcv_df):
        """
        RSI must always be between 0 and 100 — this is a hard mathematical
        constraint. Any value outside this range indicates a bug.
        """
        transformer = OHLCVTransformer()
        result = transformer.transform(sample_ohlcv_df)

        assert (result["rsi_14"] >= 0).all()
        assert (result["rsi_14"] <= 100).all()

    def test_rsi_no_inf_values(self, sample_ohlcv_df):
        """
        Division by zero in the RSI formula (when avg_loss == 0) must be
        handled — this directly tests the .where(avg_loss != 0, 100) fix.
        """
        transformer = OHLCVTransformer()
        result = transformer.transform(sample_ohlcv_df)

        assert not np.isinf(result["rsi_14"]).any()

    def test_pure_uptrend_rsi_is_100(self, sample_ohlcv_df):
        """
        Our test fixture is a strictly increasing price series — every day
        is a gain, zero days are losses. RSI should be exactly 100 throughout,
        confirming the zero-division edge case is handled correctly.
        """
        transformer = OHLCVTransformer()
        result = transformer.transform(sample_ohlcv_df)

        assert (result["rsi_14"] == 100).all()

    def test_groupby_does_not_leak_across_tickers(self, multi_ticker_df):
        """
        Critical test: AAA and BBB have different price levels.
        If groupby logic is broken, AAA's SMA20 could be contaminated
        with BBB's much larger close values, or vice versa.
        """
        transformer = OHLCVTransformer()
        result = transformer.transform(multi_ticker_df)

        aaa_rows = result[result["ticker"] == "AAA"]
        bbb_rows = result[result["ticker"] == "BBB"]

        # BBB's prices are 10x AAA's — SMA20 should reflect that scale difference
        # If they were ever mixed, this ratio would break
        ratio = bbb_rows.iloc[0]["sma_20"] / aaa_rows.iloc[0]["sma_20"]
        assert abs(ratio - 10) < 0.5

    def test_empty_dataframe_returns_empty(self):
        """Edge case: empty input should not crash, just return empty output."""
        transformer = OHLCVTransformer()
        empty_df = pd.DataFrame(columns=["ticker", "timestamp", "open", "high", "low", "close", "volume"])
        result = transformer.transform(empty_df)
        assert result.empty

    def test_custom_sma_windows(self, sample_ohlcv_df):
        """Verify the transformer respects custom window configuration."""
        transformer = OHLCVTransformer(sma_windows=[10])
        result = transformer.transform(sample_ohlcv_df)

        assert "sma_10" in result.columns
        assert "sma_50" not in result.columns