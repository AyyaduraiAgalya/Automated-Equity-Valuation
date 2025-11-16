# data_extract/gold/extract_ohlc.py

from typing import Union
import pandas as pd
from pathlib import Path
from typing import Optional
import yfinance as yf



def _ensure_datetime_and_sort(
    df: pd.DataFrame,
    ticker_col: str = "ticker",
    period_col: str = "period",
) -> pd.DataFrame:
    """
    Internal helper:
    - converts period column to datetime
    - sorts by ticker, period
    - returns a copy
    """
    df = df.copy()
    df[period_col] = pd.to_datetime(df[period_col])
    df = df.sort_values([ticker_col, period_col])
    return df

def make_ticker_period_ranges(
    df_funda: pd.DataFrame,
    ticker_col: str = "ticker",
    period_col: str = "period",
) -> pd.DataFrame:
    """
    From a fundamentals dataframe with at least [ticker_col, period_col],
    create a small dataframe with one row per ticker:

        [ticker_col, start_period, end_period]

    where:
    - start_period = earliest period per ticker
    - end_period   = latest  period per ticker
    """
    df = _ensure_datetime_and_sort(df_funda, ticker_col, period_col)

    ranges = (
        df.groupby(ticker_col)[period_col]
        .agg(start_period="min", end_period="max")
        .reset_index()
    )

    return ranges


def fetch_daily_prices_for_ticker(
    ticker: str,
    start,
    end,
    auto_adjust: bool = False,
) -> pd.DataFrame:
    """
    Download daily OHLCV for a single ticker between [start, end] (inclusive)
    using yfinance.

    Returns a DataFrame with columns:
        ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'ticker', 'date']
    or an empty DataFrame if no data is found.
    """
    # yfinance's 'end' is exclusive, so add one day
    df = yf.download(
        ticker,
        start=start,
        end=end + pd.Timedelta(days=1),
        auto_adjust=auto_adjust,
        progress=False,
    )

    if df.empty:
        return df  # empty

    df = df.sort_index()
    df["ticker"] = ticker
    df["date"] = df.index

    # optional: reset index so we keep it all explicit
    df = df.reset_index(drop=True)

    return df


def download_daily_prices_for_ranges(
    ticker_ranges: pd.DataFrame,
    out_path: str,
    ticker_col: str = "ticker",
    start_col: str = "start_period",
    end_col: str = "end_period",
    auto_adjust: bool = False,
    overwrite: bool = True,
) -> pd.DataFrame:
    """
    Given a dataframe with one row per ticker and columns:
        [ticker_col, start_col, end_col]

    Download daily OHLCV data for all tickers using yfinance, concatenate into
    a single DataFrame, save it as a parquet file at `out_path`, and return it.

    Parameters
    ----------
    ticker_ranges : pd.DataFrame
        DataFrame with at least [ticker_col, start_col, end_col].
    out_path : str
        File path where the combined daily prices will be saved as parquet.
    ticker_col : str, default 'ticker'
        Name of the column containing the ticker symbols.
    start_col : str, default 'start_period'
        Name of the column containing the start date (earliest fiscal period).
    end_col : str, default 'end_period'
        Name of the column containing the end date (latest fiscal period).
    auto_adjust : bool, default False
        If True, yfinance returns prices adjusted for splits/dividends.
    overwrite : bool, default True
        If False and out_path already exists, the file is not overwritten
        and is simply loaded and returned.

    Returns
    -------
    pd.DataFrame
        Combined daily prices with a MultiIndex [ticker, date] or simple index
        depending on how you choose to work with it.
    """
    out_path = Path(out_path)

    # If we don't want to overwrite and the file exists, just load and return it
    if not overwrite and out_path.exists():
        return pd.read_parquet(out_path)

    price_frames = []

    # Ensure dates are datetime
    df = ticker_ranges.copy()
    df[start_col] = pd.to_datetime(df[start_col])
    df[end_col] = pd.to_datetime(df[end_col])

    for _, row in df.iterrows():
        ticker = row[ticker_col]
        start = row[start_col]
        end = row[end_col]

        daily = fetch_daily_prices_for_ticker(
            ticker=ticker,
            start=start,
            end=end,
            auto_adjust=auto_adjust,
        )

        if daily.empty:
            # Optional: you can log this if you want
            # print(f"No price data for {ticker} between {start} and {end}")
            continue

        price_frames.append(daily)

    if not price_frames:
        # No data at all
        combined = pd.DataFrame()
    else:
        combined = pd.concat(price_frames, ignore_index=True)

        # Set a nice MultiIndex for later work
        combined = combined.set_index(["ticker", "date"]).sort_index()

    # Save to parquet so you don't have to refetch
    combined.to_parquet(out_path)

    return combined
