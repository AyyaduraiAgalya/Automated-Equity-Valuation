# src/data_extract/silver_transformer/transformer_cf.py

from __future__ import annotations
from pathlib import Path
import pandas as pd

from src.data_extract.bronze_extractor.extractor_cf import extract_cash_flows
from src.data_extract.config.tag_map_min import UOM_MULTIPLIERS, MONETARY_UOMS
from src.data_extract.config.tag_map_min import CF as CF_TAGMAP
from src.data_extract.config.forms import ANNUAL_FORMS, QUARTERLY_FORMS, ALL_FORMS


def _reverse_map(forward: dict[str, list[str]]) -> dict[str, str]:
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


def transform_cash_flow_to_wide(
    zip_path: Path,
    tag_map: dict[str, list[str]] = CF_TAGMAP,
    out_path: Path | None = None,
    return_unknown: bool = False,
    forms: set[str] | None = None,
    fp: str | list[str] | tuple[str, ...] | None = None,
):
    """
    Silver transformer for Cash Flow (CF) for one FSDS ZIP.

    Default behaviour:
      - includes annual (10-K/10-K/A) AND quarterly (10-Q/10-Q/A)
      - duration logic:
          * 10-K / 10-K/A: qtrs == '4' and ddate == period
          * 10-Q / 10-Q/A: qtrs in {'1','2','3'} and ddate == period
    """
    # 1) Bronze
    cf_long = extract_cash_flows(zip_path)
    if cf_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 2) Filter by form + fp
    if forms is None:
        forms = ALL_FORMS

    mask = cf_long["form"].isin(forms)

    if fp is not None:
        fp_upper = cf_long["fp"].astype(str).str.upper()
        if isinstance(fp, (list, tuple, set)):
            fp_vals = {str(x).upper() for x in fp}
            mask = mask & fp_upper.isin(fp_vals)
        else:
            mask = mask & (fp_upper == str(fp).upper())

    cf_long = cf_long[mask].copy()
    if cf_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 3) Duration logic
    is_10k = cf_long["form"].isin(ANNUAL_FORMS)
    is_10q = cf_long["form"].isin(QUARTERLY_FORMS)

    mask_dur = (
        (is_10k & (cf_long["qtrs"] == "4")) |
        (is_10q & cf_long["qtrs"].isin(["1", "2", "3"]))
    )
    mask_dur = mask_dur & (cf_long["ddate"] == cf_long["period"])
    cf_long = cf_long[mask_dur].copy()
    if cf_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 4) Monetary-only + normalisation
    monetary_uoms = {u.lower() for u in MONETARY_UOMS}
    cf_long = cf_long[cf_long["uom"].astype(str).str.lower().isin(monetary_uoms)].copy()

    cf_long["value"] = pd.to_numeric(cf_long["value"], errors="coerce")
    cf_long["value"] = cf_long["value"] * cf_long["uom"].map(_uom_mult)
    cf_long = cf_long.dropna(subset=["value"])
    if cf_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 5) Map tag -> canonical
    reverse = _reverse_map(tag_map)
    cf_long["canon"] = cf_long["tag"].map(reverse)

    unknown = cf_long[cf_long["canon"].isna()].copy()
    mapped = cf_long[cf_long["canon"].notna()].copy()
    if mapped.empty:
        return (pd.DataFrame(), unknown) if return_unknown else pd.DataFrame()

    # 6) Resolve collisions
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

    # 7) Pivot to wide (one row per filing)
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
