# src/data_extract/silver_transformer/transformer_shares.py

from __future__ import annotations
from pathlib import Path
import pandas as pd

from src.data_extract.bronze_extractor.fsds_loader import load_fsds_from_zip
from src.data_extract.config.tag_map_min import SHARES as SHARES_TAGMAP
from src.data_extract.config.forms import ALL_FORMS


def _reverse_map(forward: dict[str, list[str]]) -> dict[str, str]:
    rev: dict[str, str] = {}
    for canon, syns in forward.items():
        for t in set([canon, *(syns or [])]):
            rev[t] = canon
    return rev


def _prep_num_with_meta(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    sub = dfs["sub"][["adsh", "cik", "name", "form", "fy", "fp", "period", "filed", "sic"]].copy()
    num = dfs["num"][["adsh", "tag", "ddate", "qtrs", "uom", "value"]].copy()
    df = num.merge(sub, on="adsh", how="left")
    for c in ("uom", "qtrs", "fp", "form"):
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df


def transform_shares_to_wide(
    zip_path: Path,
    tag_map: dict[str, list[str]] = SHARES_TAGMAP,
    out_path: Path | None = None,
    return_unknown: bool = False,
    forms: set[str] | None = None,
    fp: str | list[str] | tuple[str, ...] | None = None,
):
    """
    Silver transformer for share counts (point-in-time + weighted-average).

    Default behaviour:
      - includes 10-K/10-K/A/10-Q/10-Q/A
      - keeps rows where:
          * ddate == period (end of fiscal period)
          * uom == 'shares'
          * qtrs in {'0','1','2','3','4'}  (instant + duration styles)
    """
    dfs = load_fsds_from_zip(zip_path)
    df = _prep_num_with_meta(dfs)
    if df.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    if forms is None:
        forms = ALL_FORMS

    mask = df["form"].isin(forms)

    if fp is not None:
        fp_upper = df["fp"].astype(str).str.upper()
        if isinstance(fp, (list, tuple, set)):
            fp_vals = {str(x).upper() for x in fp}
            mask = mask & fp_upper.isin(fp_vals)
        else:
            mask = mask & (fp_upper == str(fp).upper())

    # at-period date
    mask = mask & (df["ddate"] == df["period"])
    df = df[mask].copy()
    if df.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # shares only
    df = df[df["uom"].astype(str).str.lower() == "shares"].copy()
    if df.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # keep both instant (0) and period (1–4)
    df = df[df["qtrs"].isin(["0", "1", "2", "3", "4"])].copy()
    if df.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # numeric
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    if df.empty:
        return (pd.DataFrame(), pd.DataFrame()) if return_unknown else pd.DataFrame()

    # map tag -> canon
    reverse = _reverse_map(tag_map)
    df["canon"] = df["tag"].map(reverse)

    unknown = df[df["canon"].isna()].copy()
    mapped = df[df["canon"].notna()].copy()
    if mapped.empty:
        return (pd.DataFrame(), unknown) if return_unknown else pd.DataFrame()

    # resolve collisions
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

    # pivot wide (one row per filing)
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
