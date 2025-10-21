# src/data_prep/silver_shares.py

from pathlib import Path
import pandas as pd
from .fsds_loader import load_fsds_from_zip
# from .tag_map_shares import SHARE_TAG_TO_CANON
import importlib
from . import tag_map_shares as tm



def build_silver_shares(zip_path: Path, out_path: Path | None = None) -> pd.DataFrame:
    """
    Silver layer: build a wide table of share counts per filing (one row per adsh).
    - BS shares: instant @ FY end (qtrs == '0', ddate == period)
    - IS shares: annual duration @ FY end (qtrs == '4', ddate == period)
    - uom == 'shares' only
    - Map tags to canonical names and pivot wide
    """
    dfs = load_fsds_from_zip(zip_path)
    sub, pre, num = dfs["sub"], dfs["pre"], dfs["num"]

    # ---- helper to filter & pivot for one statement type ----
    def _pivot_shares(stmt_key: str, qtrs_needed: str) -> pd.DataFrame:
        # 1) which (adsh, tag) belong to this statement?
        stmt = pre.loc[pre["stmt"].str.upper() == stmt_key.upper(), ["adsh", "tag"]].drop_duplicates()
        if stmt.empty:
            return pd.DataFrame()

        # 2) numeric facts joined to those tags
        df = num.merge(stmt, on=["adsh","tag"], how="inner")

        # 3) keep only shares unit, desired qtrs, and align on FY period
        #    sub.period is the official FY end date; we keep facts at that date
        df = df[(df["uom"].str.lower() == "shares") & (df["qtrs"] == qtrs_needed)]
        df = df.merge(sub[["adsh","period"]], on="adsh", how="left")
        df = df[df["ddate"] == df["period"]].copy()

        if df.empty:
            return pd.DataFrame()

        # 4) map tags → canonical; drop unknowns (log later if you like)
        importlib.reload(tm)
        MAP = tm.SHARE_TAG_TO_CANON
        df["tag"] = df["tag"].astype(str).str.strip()
        df["canon"] = df["tag"].map(MAP)
        log_unmapped_tags(df, stmt_key, zip_path.name)
        df = df.dropna(subset=["canon"])

        # 5) numeric cast and pivot to wide per filing
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"]).drop_duplicates(subset=["adsh","canon"], keep="first")

        wide = df.pivot_table(index="adsh", columns="canon", values="value", aggfunc="first").reset_index()
        return wide

    # Build BS (instant) and IS (duration) shares
    bs_w = _pivot_shares(stmt_key="BS", qtrs_needed="0")
    is_w = _pivot_shares(stmt_key="IS", qtrs_needed="4")

    # Merge them (left join is fine; a filing might have only one side populated)
    if bs_w.empty and is_w.empty:
        shares_wide = pd.DataFrame()
    elif bs_w.empty:
        shares_wide = is_w.copy()
    elif is_w.empty:
        shares_wide = bs_w.copy()
    else:
        shares_wide = bs_w.merge(is_w, on="adsh", how="outer")

    if shares_wide.empty:
        return shares_wide

    # Attach simple metadata for convenience
    meta_cols = ["adsh","cik","name","form","fy","fp","period","filed","sic"]
    shares_wide = shares_wide.merge(sub[meta_cols].drop_duplicates(), on="adsh", how="left")

    # Sorting columns: ids first, then canon shares
    id_cols = [c for c in ["adsh","cik","name","fy","period","filed","form","sic"] if c in shares_wide.columns]
    measure_cols = [c for c in shares_wide.columns if c not in id_cols]
    shares_wide = shares_wide[id_cols + measure_cols]

    # Optional save
    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        shares_wide.to_parquet(out_path, index=False)

    return shares_wide

# src/data_prep/silver_shares.py


def log_unmapped_tags(df: pd.DataFrame, stmt_key: str, zip_name: str):
    """
    Log unmapped share tags into logs/unmapped_shares.csv for later inspection.
    Appends (tag, stmt, count, uom, zip_name) if tag not in SHARE_TAG_TO_CANON.
    """
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / "unmapped_shares.csv"

    # Count how many times each unmapped tag appeared
    unmapped = (
        df.loc[~df["tag"].isin(SHARE_TAG_TO_CANON.keys()), ["tag", "uom"]]
        .value_counts()
        .reset_index(name="count")
    )
    if unmapped.empty:
        return

    unmapped["stmt"] = stmt_key
    unmapped["source_zip"] = zip_name

    # Append or create the CSV
    if log_path.exists():
        old = pd.read_csv(log_path)
        combined = pd.concat([old, unmapped], ignore_index=True)
        combined.drop_duplicates(subset=["tag", "stmt", "uom", "source_zip"], inplace=True)
        combined.to_csv(log_path, index=False)
    else:
        unmapped.to_csv(log_path, index=False)
