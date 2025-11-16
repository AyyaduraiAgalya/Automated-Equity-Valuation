from pathlib import Path
import pandas as pd
from .fsds_loader import load_fsds_from_zip

def extract_cash_flows(zip_path: Path) -> pd.DataFrame:
    """
    Extract Cash Flow (CF) facts for all annual filings in a given FSDS ZIP.
    Bronze-layer output (LONG format, raw-ish): we do not filter by qtrs/ddate here.
    In Silver you'll filter to annual duration (qtrs == '4') and ddate == period,
    map to canonical tags, and pivot to wide.

    INPUT:
        zip_path: Path to SEC FSDS quarterly ZIP (e.g., 'data/raw/2025q2.zip').

    RETURNS (long DataFrame; one row per CF fact per filing), typical columns:
        - adsh: filing id
        - tag: XBRL tag (e.g., NetCashProvidedByUsedInOperatingActivities)
        - version: taxonomy/version (if present)
        - ddate: fiscal date (period end, YYYYMMDD)
        - qtrs: number of quarters in the duration (annual = '4' in Silver)
        - uom: unit of measure (e.g., 'USD')
        - coreg: co-registrant (rare; often NaN)
        - value: numeric value (float)
        - cik, name, form, fy, fp, period, filed, sic: filing metadata
        - source_zip: the ZIP filename (lineage)
    """

    # 1) Load raw FSDS tables from the ZIP
    dfs = load_fsds_from_zip(zip_path)
    sub, pre, num = dfs["sub"], dfs["pre"], dfs["num"]

    # 2) Identify Cash Flow tags from the presentation table
    #    Most commonly 'CF'. Some quarters use 'SCF' or variants.
    #    We'll accept stmt that equals 'CF' OR contains 'CF' to be robust.
    stmt_series = pre["stmt"].astype(str).str.upper()
    cf_mask = (stmt_series == "CF") | stmt_series.str.contains("CF", na=False)
    cf_tags = pre.loc[cf_mask, ["adsh", "tag"]].drop_duplicates()

    # Edge case: nothing found -> return an empty, well-formed DataFrame
    if cf_tags.empty:
        return pd.DataFrame(columns=[
            "adsh","tag","version","ddate","qtrs","uom","coreg","value",
            "cik","name","form","fy","fp","period","filed","sic","instance",
            "fye","accepted","countryba","stprba","source_zip",
        ])


    # 3) Keep only numeric facts for those (adsh, tag) pairs on the Cash Flow statement
    num_cf = num.merge(cf_tags, on=["adsh", "tag"], how="inner")

    # 4) Restrict to annual and quarterly report forms (adjust to {"10-K"} if you want only US domestic)
    valid_forms = {"10-K", "10-K/A", "10-Q", "10-Q/A"}
    sub_filtered = sub[sub["form"].isin(valid_forms)].copy()

    # 5) Attach filing/company metadata to each numeric CF row
    meta_cols = ["adsh","cik","name","form","fy","fp","period","filed","sic","instance",
    "fye","accepted","countryba","stprba"]
    meta_cols = [c for c in meta_cols if c in sub_filtered.columns]
    cf_full = num_cf.merge(sub_filtered[meta_cols], on="adsh", how="left")

    # 6) Numeric coercion; drop rows with non-numeric or missing values
    cf_full["value"] = pd.to_numeric(cf_full["value"], errors="coerce")
    cf_full = cf_full.dropna(subset=["value"]).reset_index(drop=True)

    # 7) Add lineage (which ZIP produced this)
    cf_full["source_zip"] = zip_path.name

    # Optional: quick info
    print(f"Extracted {len(cf_full):,} CF facts from {zip_path.name} | filings={cf_full['adsh'].nunique():,}")

    return cf_full
