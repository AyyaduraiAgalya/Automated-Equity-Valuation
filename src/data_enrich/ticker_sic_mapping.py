# src/data_enrich/add_ticker_to_panel.py

from pathlib import Path
import pandas as pd


def ticker_from_instance(instance: str) -> str | None:
    """
    Infer a ticker-like string from the 'instance' filename prefix
    (before the first hyphen), e.g. 'dg-20130201.xml' -> 'DG'.

    Rules:
    - Take substring before first '.' and then before first '-'
    - Strip whitespace and uppercase
    - Only accept 1–5 alphabetic characters, else return None
    """
    if not isinstance(instance, str):
        return None

    base = instance.split(".")[0]      # dg-20130201.xml -> dg-20130201
    prefix = base.split("-")[0]        # dg-20130201 -> dg
    cand = prefix.strip().upper()      # DG

    if 1 <= len(cand) <= 5 and cand.isalpha():
        return cand
    return None


def map_sic_to_office_industry(
    df: pd.DataFrame,
    sic_mapping_path: str | Path = "data/sic_office_industry.csv",
) -> pd.DataFrame:
    """
    Attach SEC SIC Office and Industry Title metadata to a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain a 'sic' column (int or string).
    sic_mapping_path : str or Path, optional
        Path to CSV file with columns: 'sic', 'office', 'industry_title'.
        Defaults to 'data/sic_office_industry.csv'.

    Returns
    -------
    pd.DataFrame
        Copy of df with extra columns:
        - 'office'
        - 'industry_title'

    Notes
    -----
    The SIC mapping is sourced from:
    https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list
    """
    if "sic" not in df.columns:
        # nothing to do
        return df

    sic_mapping_path = Path(sic_mapping_path)
    if not sic_mapping_path.exists():
        raise FileNotFoundError(f"SIC mapping CSV not found: {sic_mapping_path}")

    mapping = pd.read_csv(
        sic_mapping_path,
        dtype={"sic": "Int64"},
    )

    df_out = df.copy()
    df_out["sic"] = df_out["sic"].astype("Int64")

    df_out = df_out.merge(mapping, how="left", on="sic")

    return df_out


def build_financials_panel_ticker_sic(
    parquet_in: str | Path = "data/enriched/financials_annual.parquet",
    sic_mapping_path: str | Path = "data/sic_office_industry.csv",
    parquet_ticker_intermediate: str | Path = "data/enriched/financials_annual_ticker.parquet",
    parquet_out: str | Path = "data/enriched/financials_annual_ticker_sic.parquet",
    save_intermediate: bool = True,
) -> Path:
    """
    Full enrichment pipeline:
    1. Load consolidated fundamentals panel parquet (gold).
    2. Infer `ticker` from `instance`.
    3. Join SIC mapping to add `office` and `industry_title`.
    4. Save final parquet with ticker + SIC info.

    Parameters
    ----------
    parquet_in : str or Path, optional
        Input parquet path, e.g. 'data/gold/financials_panel.parquet'.
    sic_mapping_path : str or Path, optional
        CSV with columns: 'sic', 'office', 'industry_title'.
    parquet_ticker_intermediate : str or Path, optional
        Optional intermediate parquet with just the ticker added.
    parquet_out : str or Path, optional
        Final enriched parquet path, e.g.
        'data/enriched/financials_panel_ticker_sic.parquet'.
    save_intermediate : bool, optional
        If True, save the ticker-only intermediate parquet.

    Returns
    -------
    Path
        Path to the final enriched parquet.
    """
    parquet_in = Path(parquet_in)
    parquet_out = Path(parquet_out)
    parquet_ticker_intermediate = Path(parquet_ticker_intermediate)

    if not parquet_in.exists():
        raise FileNotFoundError(f"Input parquet not found: {parquet_in}")

    # 1) Load gold panel
    df = pd.read_parquet(parquet_in)

    if "instance" not in df.columns:
        raise ValueError(
            f"Expected column 'instance' not found in dataframe. "
            f"Columns present: {list(df.columns)}"
        )

    df = df.copy()

    # 2) Infer ticker from instance
    df["ticker"] = df["instance"].apply(ticker_from_instance)

    # Ensure data/enriched exists
    parquet_out.parent.mkdir(parents=True, exist_ok=True)
    parquet_ticker_intermediate.parent.mkdir(parents=True, exist_ok=True)

    if save_intermediate:
        df.to_parquet(parquet_ticker_intermediate, index=False)
        print(f"[INFO] Saved intermediate panel with ticker to: {parquet_ticker_intermediate}")

    # 3) Attach SIC → office / industry_title
    df = map_sic_to_office_industry(df, sic_mapping_path=sic_mapping_path)

    # 4) Save final enriched parquet
    df.to_parquet(parquet_out, index=False)
    print(f"[INFO] Saved enriched panel with ticker + SIC to: {parquet_out}")

    return parquet_out
