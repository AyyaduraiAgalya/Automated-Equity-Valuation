from pathlib import Path
import pandas as pd
from .fsds_loader import load_fsds_from_zip

def extract_income_statements(zip_path: Path) -> pd.DataFrame:
    """
    Extract Income Statement (IS) facts for all annual filings in a given FSDS ZIP.
    Bronze-layer output (LONG format, raw-ish): we do not filter by qtrs/ddate here.
    Filtering to annual duration (qtrs='4') and ddate==period will be done in Silver.

    INPUT:
        zip_path: Path to SEC FSDS quarterly ZIP (e.g., 'data/raw/2025q2.zip').

    RETURNS (long DataFrame; one row per IS fact per filing):
        Columns typically include:
          - adsh: unique filing id
          - tag: XBRL tag (e.g., Revenues, NetIncomeLoss, CostOfRevenue, etc.)
          - version: taxonomy/version (if present)
          - ddate: fiscal date (period end, YYYYMMDD)
          - qtrs: number of quarters in the duration (IS is annual -> '4' in Silver)
          - uom: unit of measure (USD, shares, perShare)
          - coreg: co-registrant (rare; usually NaN)
          - value: numeric value (float)
          - cik, name, form, fy, fp, period, filed, sic: filing metadata (from sub)
          - source_zip: the zip filename for lineage

    NOTES:
        - We keep all rows here (Bronze). In Silver, you will:
            * filter to annual IS facts: qtrs == '4' and ddate == period
            * map tags to canonical names (tag_map.py)
            * pivot to wide per filing (one row per adsh)
    """

    # 1) Load raw FSDS tables from the ZIP
    dfs = load_fsds_from_zip(zip_path)
    sub, pre, num = dfs["sub"], dfs["pre"], dfs["num"]

    # 2) Identify Income Statement tags from the presentation table
    #    Most quarters use stmt == 'IS' for income statement.
    #    (If you later see variants like 'CI' or 'INC', you can broaden this filter.)
    is_tags = pre.loc[pre["stmt"].str.upper() == "IS", ["adsh", "tag"]].drop_duplicates()

    # Edge case: if nothing found, return an empty, well-formed DataFrame
    if is_tags.empty:
        return pd.DataFrame(columns=[
            "adsh","tag","version","ddate","qtrs","uom","coreg","value",
            "cik","name","form","fy","fp","period","filed","sic","source_zip"
        ])

    # 3) Keep only numeric facts for those (adsh, tag) pairs on the Income Statement
    #    Inner join ensures we only get tags presented on IS for each filing.
    num_is = num.merge(is_tags, on=["adsh", "tag"], how="inner")

    # 4) Restrict to annual report forms (adjust to {"10-K"} if you want only US domestic)
    valid_forms = {"10-K", "10-K/A", "20-F", "40-F"}
    sub_filtered = sub[sub["form"].isin(valid_forms)].copy()

    # 5) Attach filing/company metadata to each numeric IS row
    meta_cols = ["adsh","cik","name","form","fy","fp","period","filed","sic"]
    is_full = num_is.merge(sub_filtered[meta_cols], on="adsh", how="left")

    # 6) Numeric coercion; drop rows with non-numeric or missing values
    is_full["value"] = pd.to_numeric(is_full["value"], errors="coerce")
    is_full = is_full.dropna(subset=["value"]).reset_index(drop=True)

    # 7) Add lineage (which ZIP produced this)
    is_full["source_zip"] = zip_path.name

    # Optional: quick info
    print(f"Extracted {len(is_full):,} IS facts from {zip_path.name} | filings={is_full['adsh'].nunique():,}")

    return is_full
