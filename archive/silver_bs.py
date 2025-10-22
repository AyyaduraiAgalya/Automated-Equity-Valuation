# src/silver/bs_silver.py

from __future__ import annotations
from pathlib import Path
import pandas as pd

# --- Bronze utilities ---
from filings_index import build_filings_index
from extractor_bs import extract_balance_sheets

# --- your tag map (the one we built together) ---
from src.config.tag_map_min import BS as BS_CANON
from src.config.tag_map_min import UOM_MULTIPLIERS, MONETARY_UOMS

FORMS = {"10-K", "10-K/A", "20-F", "40-F"}

def _reverse_map(forward: dict[str, list[str]]) -> dict[str, str]:
    """tag -> canon (include the canon name itself as a synonym)."""
    rev = {}
    for canon, syns in forward.items():
        syns = list(dict.fromkeys([*syns, canon]))  # include canon, drop dupes keep order
        for t in syns:
            rev[t] = canon
    return rev

def _uom_multiplier(u: str) -> float:
    if pd.isna(u):
        return 1.0
    # case-insensitive lookup with graceful fallback to 1.0
    u_lower = str(u).lower()
    for k, v in UOM_MULTIPLIERS.items():
        if u_lower == k.lower():
            return v
    return 1.0

def _filter_fy_at_period(df_long: pd.DataFrame, filings: pd.DataFrame) -> pd.DataFrame:
    """Keep FY-at-period BS facts: qtrs=='0' and ddate == period, annual forms only."""
    df = df_long.merge(
        filings[["adsh", "period", "form", "fp"]].drop_duplicates(),
        on="adsh", how="left"
    )
    df = df[df["form"].isin(FORMS) & (df["fp"].str.upper() == "FY")]
    return df[(df["qtrs"] == "0") & (df["ddate"] == df["period"])].copy()

def _filter_monetary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    keep = {u.lower() for u in MONETARY_UOMS}
    return df[df["uom"].str.lower().isin(keep)].copy()

def _normalize_values(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["value"] = df["value"] * df["uom"].map(_uom_multiplier)
    return df.dropna(subset=["value"])

def _resolve_collisions(mapped: pd.DataFrame, forward_map: dict[str, list[str]]) -> pd.DataFrame:
    """
    If a filing has multiple source tags mapping to the same canon,
    pick a single row using a stable preference:
      1) Prefer the FIRST synonym listed for that canon in your tag map
      2) Otherwise keep the row with the largest absolute value
    """
    if mapped.empty:
        return mapped

    # build a per-canon ranking for synonyms (lower rank = better)
    pref_rank = {}
    for canon, syns in forward_map.items():
        ordered = list(dict.fromkeys([*syns, canon]))  # include canon itself
        pref_rank.update({(canon, t): i for i, t in enumerate(ordered)})

    mapped = mapped.copy()
    mapped["__rank"] = mapped.apply(
        lambda r: pref_rank.get((r["canon"], r["tag"]), 10_000), axis=1
    )
    mapped["__abs"] = mapped["value"].abs()

    # sort so the preferred choice comes first, then drop duplicates
    mapped = mapped.sort_values(["adsh", "canon", "__rank", "__abs"], ascending=[True, True, True, False])
    mapped = mapped.drop_duplicates(subset=["adsh", "canon"], keep="first")

    return mapped.drop(columns=["__rank", "__abs"])

def build_bs_silver_for_zip(
    zip_path: Path,
    year_quarter: str | None = None,
    project_root: Path = Path("."),
    write_outputs: bool = True,
) -> pd.DataFrame:
    """
    Build Silver (wide, canonical) Balance Sheet for ONE FSDS ZIP.
    - Uses your Bronze extractors & filings index.
    - Filters FY-at-period + monetary.
    - Maps tags to canonical BS fields.
    - Resolves collisions (one value per canon per filing).
    - Pivots to wide with companies (filings) in rows, canon tags as columns.

    Returns the wide DataFrame. Optionally writes to:
      data/silver/bs/year_quarter=YYYYQ#/bs_wide_canonical.parquet
      logs/coverage/bs_YYYYQ#_coverage.csv
      logs/coverage/bs_YYYYQ#_unknown_tags.csv
    """
    project_root = Path(project_root)
    if year_quarter is None:
        # infer 2025Q2 from filename like ".../2025q2.zip"
        stem = zip_path.stem.lower()
        year_quarter = stem.replace("fsd", "").replace("_", "").upper()

    # --- Bronze stage (ensure inputs exist or build on the fly) ---
    bronze_root = project_root / "data" / "bronze"
    silver_root = project_root / "data" / "silver" / "bs" / f"year_quarter={year_quarter}"
    logs_root = project_root / "logs" / "coverage"
    silver_root.mkdir(parents=True, exist_ok=True)
    logs_root.mkdir(parents=True, exist_ok=True)

    filings_path = bronze_root / "filings" / f"year_quarter={year_quarter}" / "filings.parquet"
    if not filings_path.exists():
        filings_path.parent.mkdir(parents=True, exist_ok=True)
        filings = build_filings_index(zip_path, out_path=filings_path)
    else:
        filings = pd.read_parquet(filings_path)

    bs_long_path = bronze_root / "bs" / f"year_quarter={year_quarter}" / "bs_long.parquet"
    if not bs_long_path.exists():
        bs_long_path.parent.mkdir(parents=True, exist_ok=True)
        extract_balance_sheets(zip_path).to_parquet(bs_long_path, index=False)

    bs_long = pd.read_parquet(bs_long_path)

    # --- Silver transforms ---
    # 1) FY-at-period + monetary only
    bs = _filter_fy_at_period(bs_long, filings)
    bs = _filter_monetary(bs)
    bs = _normalize_values(bs)

    # 2) Map tags -> canon
    forward = BS_CANON
    reverse = _reverse_map(forward)
    mapped = bs.copy()
    mapped["canon"] = mapped["tag"].map(reverse)

    unknown = mapped[mapped["canon"].isna()].copy()
    mapped = mapped[~mapped["canon"].isna()].copy()

    # 3) Attach filing metadata (for the final wide table)
    meta_cols = ["adsh", "cik", "name", "fy", "filed", "period", "sic"]
    filings_meta = filings.rename(columns={"conm": "name"}) if "name" not in filings.columns and "conm" in filings.columns else filings
    mapped = mapped.merge(filings_meta[meta_cols].drop_duplicates(), on="adsh", how="left")

    # 4) Resolve collisions: one value per (adsh, canon)
    mapped = _resolve_collisions(mapped, forward)

    # 5) Coverage & unknown-tag logs
    # per-filing coverage after all filters
    cov_mapped = mapped.groupby("adsh")["tag"].count().rename("mapped_count")
    considered = pd.concat([mapped[["adsh", "tag"]], unknown[["adsh", "tag"]]], axis=0)
    cov_total = considered.groupby("adsh")["tag"].count().rename("total_considered")
    coverage = (
        pd.concat([cov_mapped, cov_total], axis=1)
        .fillna(0)
        .reset_index()
    )
    coverage["coverage_pct"] = coverage["mapped_count"].div(coverage["total_considered"]).where(coverage["total_considered"] > 0, 0.0)
    coverage = filings_meta.merge(coverage, on="adsh", how="left").fillna({"mapped_count": 0, "total_considered": 0, "coverage_pct": 0.0})

    unk_freq = (
        unknown.groupby("tag")["adsh"].nunique()
        .sort_values(ascending=False)
        .rename("filings_with_unknown")
        .reset_index()
    )

    # 6) Pivot wide (companies/filings in rows)
    index_cols = ["adsh", "cik", "name", "fy", "filed", "period", "sic"]
    wide = (
        mapped.pivot_table(index=index_cols, columns="canon", values="value", aggfunc="first")
        .reset_index()
    )
    wide.columns.name = None

    # OPTIONAL: de-dup to one row per (cik, fy) keeping latest filed
    if not wide.empty and {"cik", "fy", "filed"}.issubset(wide.columns):
        wide = (
            wide.sort_values(["cik", "fy", "filed"])
                .drop_duplicates(subset=["cik", "fy"], keep="last")
        )

    # --- Persist ---
    if write_outputs:
        wide_out = silver_root / "bs_wide_canonical.parquet"
        cov_out = (project_root / "logs" / "coverage" / f"bs_{year_quarter}_coverage.csv")
        unk_out = (project_root / "logs" / "coverage" / f"bs_{year_quarter}_unknown_tags.csv")
        wide.to_parquet(wide_out, index=False)
        coverage.to_csv(cov_out, index=False)
        unk_freq.to_csv(unk_out, index=False)

    return wide
