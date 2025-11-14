# src/data_extract/gold_builder/builder_all_ohlcv.py

from __future__ import annotations
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np
import yfinance as yf


def _fetch_monthly_ohlcv_for_ticker(
    ticker: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cache_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Fetch monthly OHLCV for a single ticker between [start, end].

    - Uses yfinance with interval="1mo"
    - Optionally caches per-ticker data in cache_dir / f"{ticker}.parquet"
    - Returns DataFrame with columns: ['date','open','high','low','close','adj_close','volume','ticker']
    """
    ticker = str(ticker).upper()

    # ----- Cache path -----
    df_cached = None
    cache_path = None
    if cache_dir is not None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / f"{ticker}.parquet"
        if cache_path.exists():
            try:
                tmp = pd.read_parquet(cache_path)
                # Only accept cache if it has a 'date' column
                if "date" in tmp.columns:
                    tmp["date"] = pd.to_datetime(tmp["date"])
                    df_cached = tmp
                else:
                    print(f"[WARN] Cache for {ticker} missing 'date' column – ignoring old cache.")
                    df_cached = None
            except Exception as e:
                print(f"[WARN] Failed to read cache for {ticker}: {e}")
                df_cached = None

    # If we have valid cached data, just filter to the requested window
    if df_cached is not None and not df_cached.empty:
        mask = (df_cached["date"] >= start) & (df_cached["date"] <= end)
        df = df_cached.loc[mask].copy()
        if not df.empty:
            return df

    # Otherwise fetch from Yahoo Finance
    print(f"[INFO] Fetching monthly OHLCV for {ticker} from {start.date()} to {end.date()}")

    data = yf.download(
        tickers=ticker,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        interval="1mo",
        auto_adjust=False,
        progress=False,
    )

    if data.empty:
        return pd.DataFrame(columns=["date","open","high","low","close","adj_close","volume","ticker"])

    df = data.reset_index().rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )

    df["date"] = pd.to_datetime(df["date"])
    df["ticker"] = ticker

    # Update/merge cache if caching is enabled
    if cache_dir is not None and cache_path is not None:
        if df_cached is not None and not df_cached.empty:
            combined = (
                pd.concat([df_cached, df], axis=0, ignore_index=True)
                  .drop_duplicates(subset=["date"])
                  .sort_values("date")
            )
        else:
            combined = df.copy()
        combined.to_parquet(cache_path, index=False)

    return df



def attach_fy_ohlcv_to_panel(
    panel_path: Path,
    out_path: Path,
    cache_dir: Optional[Path] = Path("data/market/yf_monthly_cache"),
    lookback_months: int = 12,
) -> Path:
    """
    Enhance the Gold financials panel with fiscal-year OHLCV features per (ticker, period).

    For each row (ticker, period):
      - look at the last `lookback_months` of monthly bars ending at `period`
      - compute:

        px_close_fy_end   : last close in that window (period-end price)
        px_close_fy_start : first close in that window
        px_return_fy      : px_close_fy_end / px_close_fy_start - 1
        px_high_fy        : max High in window
        px_low_fy         : min Low in window
        px_vol_fy         : std of monthly log-returns in window

    Writes the enriched panel to `out_path` and returns that path.
    """
    panel = pd.read_parquet(panel_path)
    if panel.empty:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        panel.to_parquet(out_path, index=False)
        return out_path

    if not {"ticker", "period"}.issubset(panel.columns):
        raise ValueError("Panel must contain 'ticker' and 'period' columns.")

    df = panel.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["period"] = pd.to_datetime(df["period"])

    # We will build features per ticker to reuse downloaded data
    unique_tickers = df["ticker"].dropna().unique().tolist()
    print(f"[INFO] Building OHLCV features for {len(unique_tickers)} tickers")

    # Prepare empty columns for features
    df["px_close_fy_end"] = np.nan
    df["px_close_fy_start"] = np.nan
    df["px_return_fy"] = np.nan
    df["px_high_fy"] = np.nan
    df["px_low_fy"] = np.nan
    df["px_vol_fy"] = np.nan

    for tkr in unique_tickers:
        mask_t = df["ticker"] == tkr
        df_t = df.loc[mask_t].copy()
        if df_t.empty:
            continue

        # For this ticker, we only need prices between
        # (min(period) - lookback) and (max(period))
        min_period = df_t["period"].min()
        max_period = df_t["period"].max()

        start_global = min_period - pd.DateOffset(months=lookback_months + 1)
        end_global = max_period

        px = _fetch_monthly_ohlcv_for_ticker(
            ticker=tkr,
            start=start_global,
            end=end_global,
            cache_dir=cache_dir,
        )
        if px.empty:
            print(f"[WARN] No price data for {tkr}")
            continue

        px = px.sort_values("date")

        # Ensure numeric close/high/low
        px["close"] = pd.to_numeric(px["close"], errors="coerce")
        px["high"] = pd.to_numeric(px["high"], errors="coerce")
        px["low"] = pd.to_numeric(px["low"], errors="coerce")


        # Pre-compute monthly log returns for volatility
        px["log_ret"] = np.log(px["close"]).diff()

        # For each filing row of this ticker, compute features from window
        for idx_row, row in df_t.iterrows():
            period_date = row["period"]
            window_start = period_date - pd.DateOffset(months=lookback_months)

            win = px[(px["date"] > window_start) & (px["date"] <= period_date)]
            if win.empty:
                continue

            # Make sure close is numeric
            win_close = pd.to_numeric(win["close"], errors="coerce")

            # End and start close as scalars
            close_end = float(win_close.iloc[-1])
            close_start = float(win_close.iloc[0])

            # Guard against zero / NaN at start
            if not np.isfinite(close_start) or close_start == 0:
                px_close_fy_start = np.nan
                px_close_fy_end = np.nan
                px_return_fy = np.nan
            else:
                px_close_fy_start = close_start
                px_close_fy_end = close_end
                px_return_fy = (px_close_fy_end / px_close_fy_start) - 1.0

            px_high_fy = win["high"].max()
            px_low_fy = win["low"].min()
            # Vol of monthly log-returns in window
            win_log_ret = win["log_ret"].dropna()
            px_vol_fy = win_log_ret.std() if not win_log_ret.empty else np.nan

            # Assign back into main df
            df.at[idx_row, "px_close_fy_end"] = px_close_fy_end
            df.at[idx_row, "px_close_fy_start"] = px_close_fy_start
            df.at[idx_row, "px_return_fy"] = px_return_fy
            df.at[idx_row, "px_high_fy"] = px_high_fy
            df.at[idx_row, "px_low_fy"] = px_low_fy
            df.at[idx_row, "px_vol_fy"] = px_vol_fy

    # Save enriched panel
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"[INFO] Saved financials panel with OHLCV features to {out_path}")
    return out_path
