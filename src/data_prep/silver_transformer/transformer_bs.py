# src/data_prep/silver_transformer/transformer_bs.py

from pathlib import Path
import pandas as pd
from src.data_prep.bronze_extractor.extractor_bs import extract_balance_sheets
from src.data_prep.config.tag_map_min import UOM_MULTIPLIERS, MONETARY_UOMS

FORMS = {"10-K", "10-K/A"}

def _reverse_map(forward: dict[str, list[str]]) -> dict[str, str]:
    """canon->synonyms  ==>  tag->canon (includes canon itself as synonym)."""
    rev = {}
    for canon, syns in forward.items():
        for t in set([canon, *(syns or [])]):
            rev[t] = canon
    return rev

def _uom_mult(u):
    if pd.isna(u): return 1.0
    ul = str(u).lower()
    for k, v in UOM_MULTIPLIERS.items():
        if ul == k.lower(): return v
    return 1.0

def transform_balance_sheet_to_wide(
    zip_path: Path,
    tag_map: dict[str, list[str]],     # forward map: canon -> [synonyms]
    out_path: Path | None = None,
) -> pd.DataFrame:
    # 1) Bronze: extract BS long
    bs_long = extract_balance_sheets(zip_path)
    if bs_long.empty:
        return pd.DataFrame()

    # 2) Keep only annual FY-at-period BS facts (qtrs == '0' and ddate == period)
    #    (sub fields are already attached by your extractor)
    bs_long = bs_long[
        (bs_long["form"].isin(FORMS)) &
        (bs_long["fp"].str.upper() == "FY") &
        (bs_long["qtrs"] == "0") &
        (bs_long["ddate"] == bs_long["period"])
    ].copy()

    # 3) Units: allow monetary
    allowed_uoms = {u.lower() for u in MONETARY_UOMS}
    bs_long = bs_long[bs_long["uom"].str.lower().isin(allowed_uoms)].copy()

    # normalize monetary values
    bs_long["value"] = pd.to_numeric(bs_long["value"], errors="coerce")
    mult = bs_long["uom"].map(_uom_mult)
    bs_long.loc[bs_long["uom"].str.lower().isin({u.lower() for u in MONETARY_UOMS}), "value"] *= mult
    bs_long = bs_long.dropna(subset=["value"])

    # 4) Mapping: build reverse map tag->canon, then map
    reverse = _reverse_map(tag_map)                 # tag -> canon
    keep_canons = set(tag_map.keys())               # canonical column names
    bs_long["canon"] = bs_long["tag"].map(reverse)

    unknown = bs_long[bs_long["canon"].isna()].copy()
    mapped  = bs_long[bs_long["canon"].notna()].copy()

    # 5) Resolve collisions: if multiple source tags map to same canon for a filing, pick one
    # preference order = order in your forward map (first synonym wins), else largest abs(value)
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

    # 6) Pivot wide (companies in rows, canons as columns)
    index_cols = ["adsh","cik","name","fy","filed","period","sic"]
    wide = (mapped.pivot_table(index=index_cols, columns="canon", values="value", aggfunc="first")
                 .reset_index())
    wide.columns.name = None

    # Optional: keep one row per (cik, fy) — latest filed
    if not wide.empty and {"cik","fy","filed"}.issubset(wide.columns):
        wide = (wide.sort_values(["cik","fy","filed"])
                     .drop_duplicates(["cik","fy"], keep="last"))

    # 7) Coverage logs (optional but useful while you iterate)
    # (write just if out_path provided, adjust paths to your layout as needed)
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        wide.to_parquet(out_path, index=False)

    return wide, unknown
