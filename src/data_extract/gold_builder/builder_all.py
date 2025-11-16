from __future__ import annotations
from pathlib import Path
import pandas as pd
from .builder_per_zip import _qc_flags  # no need for _latest_per_cik_fy now

def build_gold_all(
    gold_zip_dir: Path,          # e.g., Path("data/gold")
    out_path: Path,              # e.g., Path("data/gold/financials_panel.parquet")
) -> Path:
    """
    Concatenate all per-ZIP Gold files into a single panel.

    This version keeps **all filings (annual + quarterly)** — i.e. one row per adsh.
    No dedup by (cik, fy);  can later filter/deduplicate in analysis.
    """
    files = sorted(gold_zip_dir.glob("*_financials.parquet"))
    dfs = [pd.read_parquet(f) for f in files if f.exists()]
    if not dfs:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_parquet(out_path, index=False)
        return out_path

    panel = pd.concat(dfs, axis=0, ignore_index=True)

    # Recompute QC flags (safe even if some columns missing)
    panel = _qc_flags(panel)

    # Stable column order: IDs first
    id_cols = [c for c in [
        "adsh","cik","name","form","fy","fp","period","filed",
        "sic","industry","source_zip"
    ] if c in panel.columns]
    other_cols = [c for c in panel.columns if c not in id_cols]
    panel = panel[id_cols + other_cols]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(out_path, index=False)
    return out_path
