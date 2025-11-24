from pathlib import Path
import json
import pandas as pd
import yfinance as yf


# Very slow function - run only for required CIKs that need ticker mapped
def attach_tickers_to_fundamentals(
    fund_df: pd.DataFrame,
    parquet_out: str | Path,
    mapping_json_path: str | Path = "data/company_tickers.json",
    cik_col_fund: str = "cik",
    ticker_col_map: str = "ticker",
    instance_col: str = "instance",
    date_col: str | None = None,    # e.g. "period" if you have it
    date_window_days: int = 2,      # +/- days around min/max date
    yf_test_period: str = "5d",     # used if date_col is missing
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Attach tickers to fundamentals using:
      1) Unique CIK -> ticker mappings from company_tickers.json
      2) Duplicate CIK mappings stored as ticker_duplicates (list per CIK)
      3) Tickers extracted from instance strings stored as ticker_instance (list per CIK)
      4) For CIKs without a unique ticker but with candidate lists, choose a final ticker by:
           - Filtering for US-looking tickers first
           - Checking if yfinance returns prices in the relevant window
         If none of the candidates have price data, mark ticker as 'DELISTED'.

    Parameters
    ----------
    fund_df : pd.DataFrame
        Fundamentals dataframe (must contain CIK and instance_col).
    parquet_out : str or Path
        Path where the enriched dataframe (with tickers) will be saved as Parquet.
    mapping_json_path : str or Path, optional
        Path to company_tickers.json with dict-of-dicts:
          {"0": {"cik_str": ..., "ticker": ..., "title": ...}, ...}
    cik_col_fund : str, optional
        CIK column name in fund_df (default 'cik').
    ticker_col_map : str, optional
        Ticker key name in the JSON mapping (usually 'ticker').
    instance_col : str, optional
        Column in fund_df containing instance strings like 'AAPL-20231231'.
    date_col : str or None, optional
        Column in fund_df with period/end date for each row (for yfinance window).
        If None or missing, yfinance will use `yf_test_period` instead.
    date_window_days : int, optional
        +/- days around min and max dates for each CIK when querying yfinance.
    yf_test_period : str, optional
        Fallback yfinance period (e.g. '1y', '5d') if date_col is not provided.
    verbose : bool, optional
        If True, prints diagnostic info.

    Returns
    -------
    pd.DataFrame
        Copy of fund_df with extra columns:
          - 'ticker'
          - 'ticker_duplicates'
          - 'ticker_instance'
        Also saves this dataframe as a Parquet file at parquet_out.
    """

    # --- 0) Setup paths and load mapping JSON ---
    parquet_out = Path(parquet_out)
    parquet_out.parent.mkdir(parents=True, exist_ok=True)

    mapping_json_path = Path(mapping_json_path)
    if not mapping_json_path.exists():
        raise FileNotFoundError(f"Mapping JSON not found: {mapping_json_path}")

    with open(mapping_json_path, "r") as f:
        raw = json.load(f)

    # JSON is dict-of-dicts like {"0": {"cik_str": ..., "ticker": ..., "title": ...}, ...}
    map_df = pd.DataFrame(raw).T.reset_index(drop=True)

    # Standardise mapping columns
    if "cik_str" not in map_df.columns:
        raise ValueError("Expected 'cik_str' in mapping JSON, but it was not found.")

    map_df = map_df.rename(columns={
        "cik_str": "cik",
        "title": "sec_name",        # optional convenience
        ticker_col_map: "ticker",   # ensure we use 'ticker' internally
    })

    # --- 1) Work on copies ---
    df = fund_df.copy()
    mapping = map_df.copy()

    # --- 2) Normalise CIK types ---
    df[cik_col_fund] = df[cik_col_fund].astype(int)
    mapping["cik"] = mapping["cik"].astype(int)

    # --- 3) Restrict mapping to CIKs present in fundamentals ---
    ciks_in_fund = df[cik_col_fund].unique()
    mapping = mapping[mapping["cik"].isin(ciks_in_fund)].copy()

    if verbose:
        print(f"CIKs in fundamentals: {len(ciks_in_fund)}")
        print(f"Rows in mapping (filtered to those CIKs): {len(mapping)}")

    # --- 4) Compute unique vs duplicate CIKs in the mapping ---
    vc = mapping["cik"].value_counts()
    unique_ciks = vc[vc == 1].index
    dup_ciks = vc[vc > 1].index

    # 4.1 Unique mapping: one ticker per CIK -> main ticker column
    unique_map = mapping[mapping["cik"].isin(unique_ciks)][
        ["cik", "ticker"]
    ].rename(columns={"cik": "cik_map_unique", "ticker": "ticker_unique"})

    # 4.2 Duplicate mapping: group tickers into list per CIK -> ticker_duplicates
    dup_map = (
        mapping[mapping["cik"].isin(dup_ciks)]
        .groupby("cik")["ticker"]
        .apply(lambda x: sorted(set(x.dropna())))
        .reset_index(name="ticker_duplicates_tmp")
        .rename(columns={"cik": "cik_map_dup"})
    )

    # --- 5) Init columns in fundamentals ---
    df["ticker"] = pd.NA
    df["ticker_duplicates"] = pd.NA
    df["ticker_instance"] = pd.NA

    # --- 6) Attach unique tickers directly ---
    df = df.merge(
        unique_map,
        left_on=cik_col_fund,
        right_on="cik_map_unique",
        how="left",
    )
    df["ticker"] = df["ticker"].fillna(df["ticker_unique"])
    df = df.drop(columns=["cik_map_unique", "ticker_unique"])

    # --- 7) Attach duplicate ticker lists ---
    df = df.merge(
        dup_map,
        left_on=cik_col_fund,
        right_on="cik_map_dup",
        how="left",
    )
    df["ticker_duplicates"] = df["ticker_duplicates"].where(
        df["ticker_duplicates"].notna(),
        df["ticker_duplicates_tmp"],
    )
    df = df.drop(columns=["cik_map_dup", "ticker_duplicates_tmp"])

    # --- 8) Helper: US-ticker heuristic ---
    def is_us_ticker(ticker: str) -> bool:
        if not isinstance(ticker, str):
            return False
        if "." not in ticker:
            return True
        # Allow US share-class style suffixes like .A, .B, .C, .U
        allowed_suffixes = {".A", ".B", ".C", ".U"}
        return any(ticker.endswith(s) for s in allowed_suffixes)

    # --- 9) Build ticker_instance lists for CIKs still without any mapping ---
    def _extract_from_instance(val: str):
        if not isinstance(val, str):
            return None
        return val.split("-")[0].upper().strip()

    mask_no_map = df["ticker"].isna() & df["ticker_duplicates"].isna()

    if instance_col in df.columns:
        inst_df = df.loc[mask_no_map, [cik_col_fund, instance_col]].dropna()
        if len(inst_df) > 0:
            inst_map = (
                inst_df.groupby(cik_col_fund)[instance_col]
                .apply(
                    lambda vals: sorted({
                        _extract_from_instance(v)
                        for v in vals
                        if _extract_from_instance(v) is not None
                    })
                )
                .reset_index(name="ticker_instance_tmp")
            )
            df = df.merge(inst_map, on=cik_col_fund, how="left")
            df["ticker_instance"] = df["ticker_instance"].where(
                df["ticker_instance"].notna(),
                df["ticker_instance_tmp"],
            )
            df = df.drop(columns=["ticker_instance_tmp"])
    else:
        if verbose:
            print(f"Column '{instance_col}' not found; ticker_instance will stay NaN.")

    # --- 10) Prepare date ranges per CIK for yfinance window checks ---
    cik_date_ranges = {}
    if date_col is not None and date_col in df.columns:
        date_tmp = df.dropna(subset=[date_col])[[cik_col_fund, date_col]].copy()
        if not date_tmp.empty:
            date_tmp[date_col] = pd.to_datetime(date_tmp[date_col])
            grouped = date_tmp.groupby(cik_col_fund)[date_col].agg(["min", "max"])
            cik_date_ranges = grouped.to_dict("index")

    def _has_price_data(ticker: str, cik: int) -> bool:
        """Check if yfinance has any prices for this ticker in relevant window."""
        try:
            if cik in cik_date_ranges:
                dmin = cik_date_ranges[cik]["min"]
                dmax = cik_date_ranges[cik]["max"]
                if pd.isna(dmin) or pd.isna(dmax):
                    hist = yf.Ticker(ticker).history(period=yf_test_period)
                else:
                    start = dmin - pd.Timedelta(days=date_window_days)
                    end = dmax + pd.Timedelta(days=date_window_days)
                    hist = yf.Ticker(ticker).history(start=start, end=end)
            else:
                hist = yf.Ticker(ticker).history(period=yf_test_period)

            return hist is not None and len(hist) > 0
        except Exception:
            return False

    # --- 11) For CIKs without a unique ticker but with candidate lists, choose final ticker ---
    ciks_needing_choice = df.loc[
        df["ticker"].isna() & (df["ticker_duplicates"].notna() | df["ticker_instance"].notna()),
        cik_col_fund,
    ].unique()

    if verbose:
        print(f"CIKs needing candidate selection: {len(ciks_needing_choice)}")

    chosen_tickers: dict[int, str] = {}

    for cik in ciks_needing_choice:
        sub = df[df[cik_col_fund] == cik]

        candidates = set()

        # From ticker_duplicates
        dup_lists = [x for x in sub["ticker_duplicates"].dropna().tolist()]
        for lst in dup_lists:
            if isinstance(lst, (list, tuple, set)):
                candidates.update(lst)

        # From ticker_instance
        inst_lists = [x for x in sub["ticker_instance"].dropna().tolist()]
        for lst in inst_lists:
            if isinstance(lst, (list, tuple, set)):
                candidates.update(lst)

        candidates = [c for c in candidates if c]

        if not candidates:
            continue

        if verbose:
            print(f"\nCIK {cik} candidates: {candidates}")

        # 1) filter to US-looking tickers with prices
        valid_us = []
        for t in candidates:
            if is_us_ticker(t) and _has_price_data(t, cik):
                valid_us.append(t)
                if verbose:
                    print(f"  ✅ US ticker {t} has price data")

        if valid_us:
            chosen = valid_us[0]
            chosen_tickers[cik] = chosen
            if verbose:
                print(f"  -> Chosen ticker for CIK {cik}: {chosen}")
            continue

        # 2) otherwise: any ticker with prices
        valid_any = []
        for t in candidates:
            if _has_price_data(t, cik):
                valid_any.append(t)
                if verbose:
                    print(f"  ✅ Non-US ticker {t} has price data")

        if valid_any:
            chosen = valid_any[0]
            chosen_tickers[cik] = chosen
            if verbose:
                print(f"  -> Chosen ticker for CIK {cik}: {chosen}")
            continue

        # 3) If no candidates have price data: mark as 'DELISTED'
        chosen_tickers[cik] = "DELISTED"
        if verbose:
            print(f"  ⚠️ No price data for any candidate. Marking CIK {cik} as DELISTED.")

    # Apply chosen tickers to df
    for cik, ticker_final in chosen_tickers.items():
        df.loc[df[cik_col_fund] == cik, "ticker"] = ticker_final

    # --- 12) Save to parquet and return df ---
    df.to_parquet(parquet_out, index=False)
    if verbose:
        print(f"[INFO] Saved enriched fundamentals with tickers to: {parquet_out}")

    return df
