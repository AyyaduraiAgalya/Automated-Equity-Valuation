from __future__ import annotations
from pathlib import Path
import pandas as pd
from .builder_per_zip import _latest_per_cik_fy, _qc_flags

def build_gold_all(
    gold_zip_dir: Path,          # e.g., Path("data/gold")
    out_path: Path,              # e.g., Path("data/gold/financials_panel.parquet")
) -> Path:
    """
    Concatenate all per-ZIP Gold files into a single annual panel, deduplicate by (cik, fy),
    keep latest filed, recompute QC flags, and write to out_path.
    """
    files = sorted(gold_zip_dir.glob("*_financials.parquet"))
    dfs = [pd.read_parquet(f) for f in files if f.exists()]
    if not dfs:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_parquet(out_path, index=False)
        return out_path

    panel = pd.concat(dfs, axis=0, ignore_index=True)

    # Dedup again across quarters/years: latest per (cik, fy)
    panel = _latest_per_cik_fy(panel)

    # Recompute QC flags (columns might differ across zips)
    panel = _qc_flags(panel)

    # Stable column order: IDs first
    id_cols = [c for c in ["adsh","cik","name","fy","filed","period","sic","industry","source_zip"] if c in panel.columns]
    other_cols = [c for c in panel.columns if c not in id_cols]
    panel = panel[id_cols + other_cols]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(out_path, index=False)
    return out_path
