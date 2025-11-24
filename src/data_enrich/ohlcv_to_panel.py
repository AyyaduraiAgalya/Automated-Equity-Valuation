import pandas as pd
import numpy as np
import yfinance as yf

# =========================================================
# 0. QUICK DIAGNOSTICS: TICKERS + YFINANCE CONNECTIVITY
# =========================================================

def debug_yfinance_and_tickers(input_parquet: str, ticker_col: str = "ticker", n_sample: int = 5):
    df_debug = pd.read_parquet(input_parquet).copy()
    df_debug = df_debug[df_debug[ticker_col].notna()].copy()
    df_debug = df_debug[df_debug[ticker_col] != "DELISTED"].copy()
    df_debug[ticker_col] = df_debug[ticker_col].astype(str).str.strip()

    print("Total rows (non-null, non-DELISTED tickers):", df_debug.shape[0])
    print("Number of unique tickers:", df_debug[ticker_col].nunique())
    print("\nSample tickers from file:")
    print(df_debug[ticker_col].drop_duplicates().head(20).tolist())

    print("\nTesting yfinance with AAPL (connectivity check)...")
    test_aapl = yf.download("AAPL", period="5d", progress=False, group_by="column")
    print("AAPL data shape:", getattr(test_aapl, "shape", None))
    if test_aapl is None or test_aapl.empty:
        print("❌ AAPL returned no data. This strongly suggests a connectivity / Yahoo blocking issue.")
    else:
        print("✅ AAPL returned data. yfinance connectivity seems OK.")

    # Test a few of YOUR tickers
    sample_tickers = df_debug[ticker_col].drop_duplicates().head(n_sample).tolist()
    print(f"\nTesting yfinance with {n_sample} tickers from your file:", sample_tickers)

    for t in sample_tickers:
        try:
            df_t = yf.download(t, period="5d", progress=False, group_by="column")
            print(f"Ticker {t}: shape {getattr(df_t, 'shape', None)}")
        except Exception as e:
            print(f"Ticker {t}: ERROR {e}")

    return df_debug


# =========================================================
# 1. MAIN FUNCTION: ATTACH OHLCV TO FUNDAMENTALS
# =========================================================

def attach_ohlcv_to_fundamentals(
    input_parquet: str,
    output_parquet: str,
    ticker_col: str = "ticker",
    period_col: str = "period",
    filed_col: str = "filed",
    max_filing_delay_days: int = 150,
    fallback_days_after_period: int = 90,
    price_window_before_anchor: int = 5,
    price_window_after_anchor: int = 10,
    max_price_lag_days: int = 10,
) -> pd.DataFrame:
    """
    Read fundamentals parquet, clean tickers, compute anchor date based on filing/period logic,
    fetch OHLCV from yfinance, attach the first trading-day price ON OR AFTER anchor_date,
    and save an enriched parquet with OHLCV columns.

    Returns the enriched DataFrame.
    """

    # ---------------------------------------------------------
    # 1. Load fundamentals and clean tickers
    # ---------------------------------------------------------
    df = pd.read_parquet(input_parquet).copy()

    # Drop missing / DELISTED tickers
    df = df[df[ticker_col].notna()].copy()
    df = df[df[ticker_col] != "DELISTED"].copy()

    # Ensure ticker is clean string and stripped
    df[ticker_col] = df[ticker_col].astype(str).str.strip()

    print("Fundamentals rows after ticker cleaning:", df.shape)
    print("Unique tickers:", df[ticker_col].nunique())

    # Ensure datetime
    df[period_col] = pd.to_datetime(df[period_col], errors="coerce")
    df[filed_col]  = pd.to_datetime(df[filed_col],  errors="coerce")

    # Drop rows with missing period
    df = df[df[period_col].notna()].copy()

    # ---------------------------------------------------------
    # 2. Drop any impossible filing < period cases
    # ---------------------------------------------------------
    invalid_mask = (df[filed_col].notna()) & (df[filed_col] < df[period_col])
    if invalid_mask.any():
        print(f"Warning: found {invalid_mask.sum()} rows with filed < period. Dropping them.")
        df = df[~invalid_mask].copy()

    # ---------------------------------------------------------
    # 3. Compute anchor_date based on your rule
    # ---------------------------------------------------------
    df["days_diff"] = (df[filed_col] - df[period_col]).dt.days
    df.loc[df["days_diff"].isna(), "days_diff"] = 9999  # force to "late" branch if filed is NaN

    timely_mask = (df["days_diff"] >= 0) & (df["days_diff"] <= max_filing_delay_days)

    df["anchor_date"] = np.where(
        timely_mask,
        df[filed_col] + pd.Timedelta(days=1),                     # timely filing → filed + 1 day
        df[period_col] + pd.Timedelta(days=fallback_days_after_period),  # very late → period + 90 days
    )
    df["anchor_date"] = pd.to_datetime(df["anchor_date"])

    print(
        "Anchor date range:",
        df["anchor_date"].min(),
        "→",
        df["anchor_date"].max(),
    )

    # ---------------------------------------------------------
    # 4. Connectivity sanity check with AAPL
    # ---------------------------------------------------------
    print("\nConnectivity check: downloading AAPL last 5 days...")
    test_aapl = yf.download("AAPL", period="5d", progress=False, group_by="column")
    if test_aapl is None or test_aapl.empty:
        raise RuntimeError(
            "yfinance could not download data for AAPL. "
            "This likely means no internet access or Yahoo Finance is blocked. "
            "Fix connectivity before running this pipeline."
        )
    else:
        print("✅ AAPL download OK. Proceeding with your tickers.")

    # ---------------------------------------------------------
    # 5. Fetch OHLCV from yfinance for all tickers
    # ---------------------------------------------------------
    unique_tickers = sorted(df[ticker_col].unique())
    print("Number of unique tickers to download:", len(unique_tickers))

    price_frames = []
    failed_tickers = []

    for t in unique_tickers:
        df_t = df[df[ticker_col] == t]
        if df_t["anchor_date"].isna().all():
            continue

        start = (df_t["anchor_date"].min() - pd.Timedelta(days=price_window_before_anchor)).strftime("%Y-%m-%d")
        end   = (df_t["anchor_date"].max() + pd.Timedelta(days=price_window_after_anchor)).strftime("%Y-%m-%d")

        try:
            data = yf.download(t, start=start, end=end, progress=False, group_by="column")
        except Exception as e:
            print(f"[ERROR] Downloading {t}: {e}")
            failed_tickers.append(t)
            continue

        if data is None or data.empty:
            print(f"[WARN] No price data for {t}, skipping.")
            failed_tickers.append(t)
            continue

        data = data.reset_index()
        cols = list(data.columns)

        # Choose price column: Adj Close > Close
        if "Adj Close" in cols:
            px_col = "Adj Close"
        elif "Adj_Close" in cols:
            px_col = "Adj_Close"
        elif "Close" in cols:
            px_col = "Close"
        else:
            print(f"[WARN] {t}: No Adj Close/Close column. Columns: {cols}")
            failed_tickers.append(t)
            continue

        frame_cols = {
            "Date": "px_date",
            "Open": "px_open",
            "High": "px_high",
            "Low": "px_low",
            "Close": "px_close",
            px_col: "px_adj_close",
            "Volume": "px_volume",
        }

        available_cols = [c for c in frame_cols.keys() if c in data.columns]
        frame = data[available_cols].copy()
        rename_map = {c: frame_cols[c] for c in available_cols}
        frame = frame.rename(columns=rename_map)

        frame["px_ticker"] = t
        price_frames.append(frame)

    print("\nTickers with failed / missing downloads:", len(failed_tickers))
    if failed_tickers:
        print(failed_tickers[:30], "..." if len(failed_tickers) > 30 else "")

    if not price_frames:
        raise RuntimeError(
            "No price data downloaded for ANY ticker, even though AAPL worked. "
            "This suggests that the tickers in your parquet are not valid Yahoo symbols. "
            "Inspect sample tickers with debug_yfinance_and_tickers() to see the format."
        )

    px_all = pd.concat(price_frames, ignore_index=True)
    px_all = px_all.sort_values(["px_ticker", "px_date"])

    print("Price data shape:", px_all.shape)

    # ---------------------------------------------------------
    # 6. Merge fundamentals with prices (first trading day ON OR AFTER anchor_date)
    # ---------------------------------------------------------
    df = df.sort_values([ticker_col, "anchor_date"])

    merged = pd.merge_asof(
        df,
        px_all,
        left_on="anchor_date",
        right_on="px_date",
        left_by=ticker_col,
        right_by="px_ticker",
        direction="forward",  # skips weekends/holidays → next trading day
    )

    merged = merged.drop(columns=["px_ticker"])

    merged["px_lag_days"] = (merged["px_date"] - merged["anchor_date"]).dt.days

    lag_mask = (merged["px_lag_days"] >= 0) & (merged["px_lag_days"] <= max_price_lag_days)
    before_filter_shape = merged.shape
    merged = merged[lag_mask].copy()
    print(
        f"After enforcing price within 0–{max_price_lag_days} days of anchor:",
        before_filter_shape,
        "→",
        merged.shape,
    )

    # ---------------------------------------------------------
    # 7. Save to parquet and return
    # ---------------------------------------------------------
    merged.to_parquet(output_parquet, index=False)
    print(f"Saved enriched dataset with OHLCV to: {output_parquet}")

    return merged
