# src/data_extract/silver_transformer/transformer_bs.py

from __future__ import annotations
from pathlib import Path
import pandas as pd

from src.data_extract.bronze_extractor.extractor_bs import extract_balance_sheets
from src.data_extract.config.tag_map_min import UOM_MULTIPLIERS, MONETARY_UOMS
from src.data_extract.config.tag_map_min import BS as BS_TAGMAP
from src.data_extract.config.forms import ALL_FORMS


def _reverse_map(forward: dict[str, list[str]]) -> dict[str, str]:
    """canon->synonyms  ==>  tag->canon (include canon itself)."""
    rev: dict[str, str] = {}
    for canon, syns in forward.items():
        for t in set([canon, *(syns or [])]):
            rev[t] = canon
    return rev


def _uom_mult(u) -> float:
    if pd.isna(u):
        return 1.0
    ul = str(u).lower()
    for k, v in UOM_MULTIPLIERS.items():
        if ul == k.lower():
            return v
    return 1.0


def transform_balance_sheet_to_wide(
    zip_path: Path,
    tag_map: dict[str, list[str]] = BS_TAGMAP,
    out_path: Path | None = None,
    return_unknown: bool = False,
    forms: set[str] | None = None,
    fp: str | list[str] | tuple[str, ...] | None = None,
):
    """
    Silver transformer for Balance Sheet (BS) for one FSDS ZIP (e.g., 2025q2).

    By default:
      - includes BOTH annual (10-K/10-K/A) and quarterly (10-Q/10-Q/A) filings
      - keeps instant rows where:
           * qtrs == '0' (balance sheet style)
           * ddate == period (date aligned to fiscal period end)

    Output:
      - one row per filing (adsh), wide form: columns = canonical BS items
    """
    # 1) Bronze: long BS
    bs_long = extract_balance_sheets(zip_path)
    if bs_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 2) Filter by form + fp
    if forms is None:
        forms = ALL_FORMS

    mask = bs_long["form"].isin(forms)

    if fp is not None:
        fp_upper = bs_long["fp"].astype(str).str.upper()
        if isinstance(fp, (list, tuple, set)):
            fp_vals = {str(x).upper() for x in fp}
            mask = mask & fp_upper.isin(fp_vals)
        else:
            mask = mask & (fp_upper == str(fp).upper())

    bs_long = bs_long[mask].copy()
    if bs_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 3) Instant logic: qtrs == '0' and ddate == period
    q = bs_long["qtrs"].fillna("0").astype(str)
    mask_instant = (q == "0") & (bs_long["ddate"] == bs_long["period"])
    bs_long = bs_long[mask_instant].copy()
    if bs_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 4) Monetary-only + unit normalisation
    monetary_uoms = {u.lower() for u in MONETARY_UOMS}
    bs_long = bs_long[bs_long["uom"].astype(str).str.lower().isin(monetary_uoms)].copy()

    bs_long["value"] = pd.to_numeric(bs_long["value"], errors="coerce")
    bs_long["value"] = bs_long["value"] * bs_long["uom"].map(_uom_mult)
    bs_long = bs_long.dropna(subset=["value"])
    if bs_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 5) Map XBRL tag -> canonical BS item
    reverse = _reverse_map(tag_map)
    bs_long["canon"] = bs_long["tag"].map(reverse)

    unknown = bs_long[bs_long["canon"].isna()].copy()
    mapped = bs_long[bs_long["canon"].notna()].copy()
    if mapped.empty:
        return (pd.DataFrame(), unknown) if return_unknown else pd.DataFrame()

    # 6) Resolve collisions (prefer first-listed synonym; tie-break |value|)
    pref_rank: dict[tuple[str, str], int] = {}
    for canon, syns in tag_map.items():
        order = [canon, *(syns or [])]
        for i, t in enumerate(order):
            pref_rank[(canon, t)] = i

    mapped["__rank"] = mapped.apply(
        lambda r: pref_rank.get((r["canon"], r["tag"]), 10_000), axis=1
    )
    mapped["__abs"] = mapped["value"].abs()

    mapped = (
        mapped.sort_values(["adsh", "canon", "__rank", "__abs"],
                           ascending=[True, True, True, False])
              .drop_duplicates(["adsh", "canon"], keep="first")
              .drop(columns=["__rank", "__abs"])
    )

    # 7) Pivot to wide: one row per filing
    index_cols = ["adsh", "cik", "name", "form", "fy", "fp", "filed", "period", "sic"]
    index_cols = [c for c in index_cols if c in mapped.columns]

    wide = (
        mapped.pivot_table(index=index_cols, columns="canon", values="value", aggfunc="first")
              .reset_index()
    )
    wide.columns.name = None

    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        wide.to_parquet(out_path, index=False)

    return (wide, unknown) if return_unknown else wide
