# src/01_data_prep/filings_index.py

from pathlib import Path
import pandas as pd
from .fsds_loader import load_fsds_from_zip

def extract_metadata(
    zip_path: Path,
    forms=("10-K", "10-K/A", "20-F", "40-F"),         
    fp="FY",                  # Annual only (no quarterlies)
    out_path: Path | None = None
) -> pd.DataFrame:
    """
    Build a per-ZIP 'filings index' for annual reports.
    
    Args:
        zip_path: Path to one FSDS ZIP (e.g., data/raw/2025q2.zip)
        forms: tuple of form types to include. Start with ("10-K",).
               (Later you can add "20-F","40-F" for foreign issuers if you want.)
        fp: fiscal period filter. "FY" keeps annual filings only.
        out_path: if provided, write the result as a Parquet file.

    Returns:
        DataFrame with one row per filing (submission), columns:
        ['adsh','cik','name','form','fy','fp','period','filed','sic']
    """

    # 1) Load the 4 FSDS tables (sub, pre, num, tag); we only need 'sub' here
    dfs = load_fsds_from_zip(zip_path)
    sub = dfs["sub"].copy()

    # 2) Normalise company name column across quarters:
    #    Some releases use 'conm' instead of 'name'.
    if "name" not in sub.columns and "conm" in sub.columns:
        sub = sub.rename(columns={"conm": "name"})

    # 3) Filter to the desired filings: e.g., only 10-K and only 'FY' (annual)
    mask = sub["form"].isin(forms) & (sub["fp"].str.upper() == fp)
    idx = sub.loc[
        mask, ["adsh","cik","name","form","fy","fp","period","filed","sic"]
    ].drop_duplicates()

    # 4) Sorting is optional but helpful for eyeballing results (latest first)
    idx = idx.sort_values(
        ["filed","cik","fy"],
        ascending=[False, True, True]
    ).reset_index(drop=True)

    # 5) (Optional) Persist to Bronze layer as partitioned metadata per quarter
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        idx.to_parquet(out_path, index=False)

    return idx