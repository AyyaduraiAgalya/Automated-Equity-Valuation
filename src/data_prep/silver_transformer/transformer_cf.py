from __future__ import annotations
from pathlib import Path
import pandas as pd
from src.data_prep.bronze_extractor.extractor_cf import extract_cash_flows
from src.data_prep.config.tag_map_min import UOM_MULTIPLIERS, MONETARY_UOMS
from src.data_prep.config.tag_map_min import CF as CF_TAGMAP  # forward: canon -> [synonyms]

FORMS = {"10-K", "10-K/A"}  

def _reverse_map(forward: dict[str, list[str]]) -> dict[str, str]:
    """canon->synonyms  ==>  tag->canon (include canon itself)."""
    rev = {}
    for canon, syns in forward.items():
        for t in set([canon, *(syns or [])]):
            rev[t] = canon
    return rev

def _uom_mult(u) -> float:
    if pd.isna(u): return 1.0
    ul = str(u).lower()
    for k, v in UOM_MULTIPLIERS.items():
        if ul == k.lower(): return v
    return 1.0

def transform_cash_flow_to_wide(
    zip_path: Path,
    tag_map: dict[str, list[str]] = CF_TAGMAP,   # forward map
    out_path: Path | None = None,
    return_unknown: bool = False,
):
    """
    Silver transformer for Cash Flow (CF), one FSDS ZIP (e.g., 2025Q2).

    Steps:
      1) Bronze: extract CF long
      2) Filter to FY-at-period, qtrs == '4' (annual duration)
      3) Monetary-only, normalize units
      4) Map raw tags -> canonical (reverse map)
      5) Resolve collisions per (adsh, canon)
      6) Pivot long -> wide (one row per filing), then keep latest per (cik, fy)
      7) Optionally write parquet and/or return unknown tags for map growth

    Returns:
      wide_df  (and unknown_df if return_unknown=True)
    """
    # 1) Bronze
    cf_long = extract_cash_flows(zip_path)
    if cf_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 2) FY-at-period, annual duration (cash flow is duration-based)
    cf_long = cf_long[
        (cf_long["form"].isin(FORMS)) &
        (cf_long["fp"].str.upper() == "FY") &
        (cf_long["qtrs"] == "4") &
        (cf_long["ddate"] == cf_long["period"])
    ].copy()
    if cf_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 3) Monetary-only + normalization
    monetary_uoms = {u.lower() for u in MONETARY_UOMS}
    cf_long = cf_long[cf_long["uom"].str.lower().isin(monetary_uoms)].copy()

    cf_long["value"] = pd.to_numeric(cf_long["value"], errors="coerce")
    cf_long["value"] = cf_long["value"] * cf_long["uom"].map(_uom_mult)
    cf_long = cf_long.dropna(subset=["value"])
    if cf_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 4) Mapping
    reverse = _reverse_map(tag_map)        # tag -> canon
    cf_long["canon"] = cf_long["tag"].map(reverse)

    unknown = cf_long[cf_long["canon"].isna()].copy()
    mapped  = cf_long[cf_long["canon"].notna()].copy()
    if mapped.empty:
        return (pd.DataFrame(), unknown) if return_unknown else pd.DataFrame()

    # 5) Resolve collisions (prefer first-listed synonym, else choose by |value|)
    pref_rank = {}
    for canon, syns in tag_map.items():
        order = [canon, *(syns or [])]
        for i, t in enumerate(order):
            pref_rank[(canon, t)] = i
    mapped["__rank"] = mapped.apply(lambda r: pref_rank.get((r["canon"], r["tag"]), 10_000), axis=1)
    mapped["__abs"]  = mapped["value"].abs()
    mapped = (mapped.sort_values(["adsh","canon","__rank","__abs"], ascending=[True,True,True,False])
                    .drop_duplicates(["adsh","canon"], keep="first")
                    .drop(columns=["__rank","__abs"]))

    # 6) Pivot wide and keep latest per (cik, fy)
    index_cols = ["adsh","cik","name","fy","filed","period","sic"]
    wide = (mapped.pivot_table(index=index_cols, columns="canon", values="value", aggfunc="first")
                 .reset_index())
    wide.columns.name = None

    if not wide.empty and {"cik","fy","filed"}.issubset(wide.columns):
        wide = (wide.sort_values(["cik","fy","filed"])
                     .drop_duplicates(["cik","fy"], keep="last"))

    # 7) Persist
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        wide.to_parquet(out_path, index=False)

    return (wide, unknown) if return_unknown else wide
