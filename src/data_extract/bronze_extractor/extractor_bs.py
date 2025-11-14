# src/data_prep/bs_extractor.py

from pathlib import Path
import pandas as pd
from .fsds_loader import load_fsds_from_zip

def extract_balance_sheets(zip_path: Path) -> pd.DataFrame:
    """
    Extract Balance Sheet (BS) facts for all annual filings in a given FSDS ZIP.

    INPUT:
        zip_path: Path to SEC FSDS quarterly ZIP (e.g., 'data/raw/2025q2.zip').

    OUTPUT (long format DataFrame):
        One row per BS fact (per filing), columns (typical):
          - adsh: filing id
          - tag: XBRL tag (e.g., 'Assets', 'LiabilitiesCurrent', ...)
          - ddate: fiscal date (YYYYMMDD)
          - qtrs: 0 for instant (balance sheet), but we keep all as-is for transparency
          - uom: unit of measure (e.g., 'USD', 'shares')
          - value: numeric value (float)
          - cik, name, form, fy, fp, period, filed, sic: filing/company metadata
          - source_zip: the ZIP filename (traceability)
    NOTE:
        - This returns a LONG (tidy) table. Pivot to WIDE (one row per adsh, columns=tags)
          is a separate, small step - can do after mapping tags to canonical names.
    """

    # 1) Load raw FSDS tables from the ZIP
    dfs = load_fsds_from_zip(zip_path)
    sub, pre, num = dfs["sub"], dfs["pre"], dfs["num"]

    # 2) Find all (adsh, tag) pairs that belong to the Balance Sheet (stmt == 'BS')
    #    We uppercase to be safe against minor casing differences.
    bs_tags = pre.loc[pre["stmt"].str.upper() == "BS", ["adsh", "tag"]].drop_duplicates()

    # Edge case: if no BS tags found, return empty quickly
    if bs_tags.empty:
        return pd.DataFrame(columns=[
            "adsh","tag","ddate","qtrs","uom","value",
            "cik","name","form","fy","fp","period","filed","sic","source_zip"
        ])

    # 3) Keep only numeric facts for those BS (adsh, tag) pairs
    #    Inner join drops anything not in the BS presentation for that filing.
    num_bs = num.merge(bs_tags, on=["adsh", "tag"], how="inner")

    # 4) Keep only annual report forms (adjust as you like)
    valid_forms = {"10-K", "10-K/A"}  # you can reduce to {"10-K"} if desired
    sub_filtered = sub[sub["form"].isin(valid_forms)].copy()

    # 5) Attach filing/company metadata to each numeric fact row
    meta_cols = ["adsh","cik","name","form","fy","fp","period","filed","sic", "instance"]
    bs_full = num_bs.merge(sub_filtered[meta_cols], on="adsh", how="left")

    # 6) Clean numeric values; drop non-numeric or empty values
    bs_full["value"] = pd.to_numeric(bs_full["value"], errors="coerce")
    bs_full = bs_full.dropna(subset=["value"]).reset_index(drop=True)

    # 7) Add lineage (which zip produced this row)
    bs_full["source_zip"] = zip_path.name

    # Optional (debug/info):
    print(f"Extracted {len(bs_full):,} BS facts from {zip_path.name} | filings={bs_full['adsh'].nunique():,}")

    return bs_full
