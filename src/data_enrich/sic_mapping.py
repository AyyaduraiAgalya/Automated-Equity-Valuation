# src/data_enrich/add_ticker_to_panel.py

from pathlib import Path
import pandas as pd


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


def build_financials_panel_with_sic(
    parquet_in: str | Path = "data/enriched/financials_annual.parquet",
    sic_mapping_path: str | Path = "data/sic_office_industry.csv",
    parquet_out: str | Path = "data/enriched/financials_annual_sic.parquet",
) -> Path:
    """
    Enrich a consolidated fundamentals panel with SEC SIC office and industry info.

    Pipeline:
    1. Load consolidated fundamentals panel parquet.
    2. Join SIC mapping to add `office` and `industry_title`.
    3. Save final parquet with SIC info.

    Parameters
    ----------
    parquet_in : str or Path, optional
        Input parquet path, e.g. 'data/enriched/financials_annual.parquet'.
    sic_mapping_path : str or Path, optional
        CSV with columns: 'sic', 'office', 'industry_title'.
    parquet_out : str or Path, optional
        Final enriched parquet path, e.g.
        'data/enriched/financials_annual_sic.parquet'.

    Returns
    -------
    Path
        Path to the final enriched parquet.
    """
    parquet_in = Path(parquet_in)
    parquet_out = Path(parquet_out)

    if not parquet_in.exists():
        raise FileNotFoundError(f"Input parquet not found: {parquet_in}")

    # 1) Load fundamentals panel
    df = pd.read_parquet(parquet_in)

    # 2) Attach SIC → office / industry_title
    df = map_sic_to_office_industry(df, sic_mapping_path=sic_mapping_path)

    # Ensure output directory exists
    parquet_out.parent.mkdir(parents=True, exist_ok=True)

    # 3) Save final enriched parquet
    df.to_parquet(parquet_out, index=False)
    print(f"[INFO] Saved enriched panel with SIC to: {parquet_out}")

    return parquet_out
