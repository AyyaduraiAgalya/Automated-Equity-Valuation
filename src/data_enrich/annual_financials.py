from __future__ import annotations
from pathlib import Path
import pandas as pd

def build_annual_10k_panel(
    panel_path: str | Path = "data/gold/financials_panel.parquet",
    out_path: str | Path | None = "data/enriched/financials_annual.parquet",
) -> pd.DataFrame:
    """
    Build an annual-only panel of 10-K / 10-K/A filings.

    Rules:
      - Start from the full financials panel (annual + quarterly, all forms).
      - Keep only forms in {"10-K", "10-K/A"}.
      - Prefer fiscal period fp == "FY" where available.
      - One row per (cik, fy): keep the latest filed.
      - If multiple filings on the same date, prefer 10-K/A over 10-K.

    Parameters
    ----------
    panel_path : str | Path
        Path to the master panel, e.g. 'data/gold/financials_panel.parquet'.
    out_path : str | Path | None
        If provided, write the annual panel there as Parquet.

    Returns
    -------
    pd.DataFrame
        Annual-only panel, one row per (cik, fy).
    """
    panel_path = Path(panel_path)
    df = pd.read_parquet(panel_path)

    if df.empty:
        if out_path is not None:
            out_path = Path(out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(out_path, index=False)
        return df

    # --- Basic filtering: 10-K + 10-K/A only ---
    if "form" not in df.columns:
        raise ValueError("Expected column 'form' in financials_panel, but it is missing.")

    forms_keep = {"10-K", "10-K/A"}
    df = df[df["form"].isin(forms_keep)].copy()
    if df.empty:
        if out_path is not None:
            out_path = Path(out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(out_path, index=False)
        return df

    # --- Ensure fy and cik are usable ---
    for col in ["cik", "fy"]:
        if col not in df.columns:
            raise ValueError(f"Expected column '{col}' in financials_panel, but it is missing.")

    df["cik"] = pd.to_numeric(df["cik"], errors="coerce").astype("Int64")
    df["fy"]  = pd.to_numeric(df["fy"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["cik", "fy"]).copy()

    # --- Prefer true annual fp == 'FY' when fp exists ---
    if "fp" in df.columns:
        # Mark which rows are clearly annual
        df["__is_fy"] = df["fp"].astype(str).str.upper().eq("FY")
        # We *keep* rows even if __is_fy == False, but use this flag in sorting
    else:
        df["__is_fy"] = True  # if fp absent, treat as annual

    # --- Ensure 'filed' is datetime ---
    if "filed" in df.columns:
        df["filed"] = pd.to_datetime(df["filed"], errors="coerce")
    else:
        df["filed"] = pd.NaT

    # --- Form priority: prefer 10-K/A over 10-K when same date ---
    form_rank_map = {"10-K": 0, "10-K/A": 1}
    df["__form_rank"] = df["form"].map(form_rank_map).fillna(0)

    # --- Sort so that the "best" row per (cik, fy) is last ---
    # Priority:
    #   1. __is_fy (True after False)  -> annual over non-annual / weird fp
    #   2. filed date (latest)
    #   3. form_rank (10-K/A > 10-K)
    df = df.sort_values(
        ["cik", "fy", "__is_fy", "filed", "__form_rank"],
        ascending=[True, True, True, True, True]  # we keep 'last' below
    )

    # --- Collapse to one row per (cik, fy) ---
    df_annual = df.drop_duplicates(subset=["cik", "fy"], keep="last").copy()

    # Clean up helper columns
    df_annual = df_annual.drop(columns=[c for c in ["__is_fy", "__form_rank"] if c in df_annual.columns])

    # --- Optional: save ---
    if out_path is not None:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df_annual.to_parquet(out_path, index=False)

    return df_annual
