from __future__ import annotations
from pathlib import Path
import pandas as pd

from src.data_extract.bronze_extractor.extractor_is import extract_income_statements
from src.data_extract.config.tag_map_min import UOM_MULTIPLIERS, MONETARY_UOMS
from src.data_extract.config.tag_map_min import IS as IS_TAGMAP
from src.data_extract.config.forms import ANNUAL_FORMS, QUARTERLY_FORMS, ALL_FORMS


def _reverse_map(forward: dict[str, list[str]]) -> dict[str, str]:
    rev = {}
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


def transform_income_statement_to_wide(
    zip_path: Path,
    tag_map: dict[str, list[str]] = IS_TAGMAP,
    out_path: Path | None = None,
    return_unknown: bool = False,
    forms: set[str] | None = None,
    fp: str | list[str] | tuple[str, ...] | None = None,
):
    """
    Silver transformer for Income Statement (IS), one FSDS ZIP (e.g., 2025Q2).

    By default:
      - includes BOTH annual (10-K/10-KA) and quarterly (10-Q/10-QA)
      - keeps duration rows where:
          * 10-K / 10-K/A: qtrs == '4' and ddate == period
          * 10-Q / 10-Q/A: qtrs in {'1','2','3'} and ddate == period

    You can later restrict to annual-only by calling with:
        forms=ANNUAL_FORMS, fp="FY"
    """

    # 1) Bronze long
    is_long = extract_income_statements(zip_path)
    if is_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 2) Choose which forms to keep
    if forms is None:
        forms = ALL_FORMS

    mask = is_long["form"].isin(forms)

    # Optional fiscal period filter (fp=None means keep all FP)
    if fp is not None:
        fp_upper = is_long["fp"].astype(str).str.upper()
        if isinstance(fp, (list, tuple, set)):
            fp_vals = {str(x).upper() for x in fp}
            mask = mask & fp_upper.isin(fp_vals)
        else:
            mask = mask & (fp_upper == str(fp).upper())

    is_long = is_long[mask].copy()
    if is_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 3) Duration logic:
    #    - annual 10-Ks: qtrs == '4'
    #    - quarterly 10-Qs: qtrs in {'1','2','3'}
    is_10k = is_long["form"].isin(ANNUAL_FORMS)
    is_10q = is_long["form"].isin(QUARTERLY_FORMS)

    mask_dur = (
        (is_10k & (is_long["qtrs"] == "4")) |
        (is_10q & is_long["qtrs"].isin(["1", "2", "3"]))
    )

    # align duration end date with period end
    mask_dur = mask_dur & (is_long["ddate"] == is_long["period"])
    is_long = is_long[mask_dur].copy()

    if is_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 4) Monetary-only + normalization
    monetary_uoms = {u.lower() for u in MONETARY_UOMS}
    is_long = is_long[is_long["uom"].astype(str).str.lower().isin(monetary_uoms)].copy()

    is_long["value"] = pd.to_numeric(is_long["value"], errors="coerce")
    is_long["value"] = is_long["value"] * is_long["uom"].map(_uom_mult)
    is_long = is_long.dropna(subset=["value"])
    if is_long.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # 5) Map raw tags -> canonical
    reverse = _reverse_map(tag_map)
    is_long["canon"] = is_long["tag"].map(reverse)

    unknown = is_long[is_long["canon"].isna()].copy()
    mapped = is_long[is_long["canon"].notna()].copy()
    if mapped.empty:
        return (pd.DataFrame(), unknown) if return_unknown else pd.DataFrame()

    # 6) Resolve collisions per (adsh, canon)
    pref_rank = {}
    for canon, syns in tag_map.items():
        order = [canon, *(syns or [])]
        for i, t in enumerate(order):
            pref_rank[(canon, t)] = i

    mapped["__rank"] = mapped.apply(lambda r: pref_rank.get((r["canon"], r["tag"]), 10_000), axis=1)
    mapped["__abs"] = mapped["value"].abs()

    mapped = (
        mapped.sort_values(["adsh", "canon", "__rank", "__abs"],
                           ascending=[True, True, True, False])
              .drop_duplicates(["adsh", "canon"], keep="first")
              .drop(columns=["__rank", "__abs"])
    )

    # 7) Pivot long -> wide (one row per filing)
    index_cols = ["adsh", "cik", "name", "form", "fy", "fp", "filed", "period", "sic"]
    wide = (
        mapped.pivot_table(index=index_cols, columns="canon", values="value", aggfunc="first")
              .reset_index()
    )
    wide.columns.name = None

    # (Optional) you can still dedupe per (cik, fy, fp) or leave filing-level
    # For now, keep one row per filing (adsh)

    # 8) Persist
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        wide.to_parquet(out_path, index=False)

    return (wide, unknown) if return_unknown else wide
