from __future__ import annotations
from pathlib import Path
import pandas as pd
from src.data_prep.bronze_extractor.fsds_loader import load_fsds_from_zip
from src.data_prep.config.tag_map_min import SHARES as SHARES_TAGMAP

FORMS = {"10-K", "10-K/A"}  # keep it US annual

def _reverse_map(forward: dict[str, list[str]]) -> dict[str, str]:
    """canon->synonyms  ==>  tag->canon (include canon itself)."""
    rev = {}
    for canon, syns in forward.items():
        for t in set([canon, *(syns or [])]):
            rev[t] = canon
    return rev

def _prep_num_with_meta(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    sub = dfs["sub"][["adsh","cik","name","form","fy","fp","period","filed","sic"]].copy()
    num = dfs["num"][["adsh","tag","ddate","qtrs","uom","value"]].copy()
    df = num.merge(sub, on="adsh", how="left")
    # Normalize a bit
    for c in ("uom","qtrs","fp","form"): df[c] = df[c].astype(str)
    return df

def transform_shares_to_wide(
    zip_path: Path,
    tag_map: dict[str, list[str]] = SHARES_TAGMAP,   # forward shares map
    out_path: Path | None = None,
    return_unknown: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
    """
    Silver transformer for Share counts (both point-in-time and weighted-average) for one FSDS ZIP.

    Keeps FY-only rows at the fiscal period date:
      - Point-in-time shares (BS-style): qtrs == '0' and ddate == period
      - Weighted-average shares (IS-style): qtrs == '4' and ddate == period
    Units: uom == 'shares' (case-insensitive)

    Returns:
      wide_df  (and unknown_df if return_unknown=True)
    """
    dfs = load_fsds_from_zip(zip_path)
    df = _prep_num_with_meta(dfs)

    if df.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # FY at period
    df = df[
        df["form"].isin(FORMS)
        & (df["fp"].str.upper() == "FY")
        & (df["ddate"] == df["period"])
    ].copy()
    if df.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # Shares only
    df = df[df["uom"].str.lower() == "shares"].copy()

    # Keep both styles: qtrs==0 (point-in-time) and qtrs==4 (weighted-average)
    df = df[df["qtrs"].isin(["0","4"])].copy()

    # Numeric
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    if df.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # Map tag -> canon
    reverse = _reverse_map(tag_map)
    df["canon"] = df["tag"].map(reverse)
    unknown = df[df["canon"].isna()].copy()
    mapped  = df[df["canon"].notna()].copy()
    if mapped.empty:
        return (pd.DataFrame(), unknown) if return_unknown else pd.DataFrame()

    # Prefer the first-listed synonym in your forward map; tie-break by |value|
    pref_rank = {}
    for canon, syns in tag_map.items():
        order = [canon, *(syns or [])]
        for i, t in enumerate(order):
            pref_rank[(canon, t)] = i
    mapped["__rank"] = mapped.apply(lambda r: pref_rank.get((r["canon"], r["tag"]), 10_000), axis=1)
    mapped["__abs"]  = mapped["value"].abs()
    mapped = (
        mapped.sort_values(["adsh","canon","__rank","__abs"], ascending=[True,True,True,False])
              .drop_duplicates(["adsh","canon"], keep="first")
              .drop(columns=["__rank","__abs"])
    )

    # Pivot wide (one row per filing)
    index_cols = ["adsh","cik","name","fy","filed","period","sic"]
    wide = mapped.pivot_table(index=index_cols, columns="canon", values="value", aggfunc="first").reset_index()
    wide.columns.name = None

    # Dedup: keep latest filed per (cik, fy)
    if not wide.empty and {"cik","fy","filed"}.issubset(wide.columns):
        wide = (
            wide.sort_values(["cik","fy","filed"])
                .drop_duplicates(["cik","fy"], keep="last")
        )

    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        wide.to_parquet(out_path, index=False)

    return (wide, unknown) if return_unknown else wide
