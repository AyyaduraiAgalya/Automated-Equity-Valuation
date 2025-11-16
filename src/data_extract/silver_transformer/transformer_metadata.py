# src/data_extract/silver_transformer/transformer_metadata.py

from __future__ import annotations
from pathlib import Path
import pandas as pd

from src.data_extract.bronze_extractor.extractor_metadata import extract_metadata
from src.data_extract.config.forms import ALL_FORMS


def transform_metadata_to_wide(
    zip_path: Path,
    out_path: Path | None = None,
    forms=ALL_FORMS,
    fp: str | list[str] | tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """
    Silver transformer for filing-level metadata.

    Default behaviour:
      - 10-K, 10-K/A, 10-Q, 10-Q/A
      - all fiscal periods (fp=None)

    """
    idx = extract_metadata(zip_path, forms=forms, fp=fp)
    if idx.empty:
        return pd.DataFrame()

    # Already one row per filing (adsh) from bronze; just sort for sanity
    sort_cols = [c for c in ["cik", "fy", "fp", "filed"] if c in idx.columns]
    if sort_cols:
        idx["filed"] = pd.to_datetime(idx["filed"], errors="coerce")
        idx = idx.sort_values(sort_cols, na_position="last")

    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        idx.to_parquet(out_path, index=False)

    return idx
