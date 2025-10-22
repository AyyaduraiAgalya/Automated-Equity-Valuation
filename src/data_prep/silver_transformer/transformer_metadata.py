from __future__ import annotations
from pathlib import Path
import pandas as pd
from src.data_prep.bronze_extractor.extractor_metadata import extract_metadata

# Optional: static mapping of SIC → Industry description
SIC_LOOKUP = {
    1311: "Crude Petroleum & Natural Gas",
    1389: "Oil & Gas Field Services",
    2834: "Pharmaceutical Preparations",
    3571: "Electronic Computers",
    3572: "Computer Storage Devices",
    3663: "Radio & TV Broadcasting Equipment",
    3674: "Semiconductors & Related Devices",
    3845: "Electromedical Apparatus",
    4813: "Telephone Communications",
    6021: "National Commercial Banks",
    7372: "Prepackaged Software",
    7389: "Business Services, NEC",
    8731: "Commercial Physical Research",
    8742: "Management Consulting Services",
    9999: "Unknown / Other",
    # … (you can expand later with full SEC SIC list)
}

def transform_metadata_to_wide(
    zip_path: Path,
    out_path: Path | None = None,
    forms=("10-K", "10-K/A", "20-F", "40-F"),
    fp="FY",
) -> pd.DataFrame:
    """
    Silver transformer for filing-level metadata.

    Builds on Bronze filings_index output, enriches with industry names,
    and filters to one record per (cik, fy) — latest filed.
    """
    idx = extract_metadata(zip_path, forms=forms, fp=fp)
    if idx.empty:
        return pd.DataFrame()

    # Add industry name from SIC
    idx["industry"] = idx["sic"].map(SIC_LOOKUP).fillna("Unknown")

    # Ensure proper types
    idx["sic"] = pd.to_numeric(idx["sic"], errors="coerce").astype("Int64")

    # Deduplicate: keep the latest filed per (cik, fy)
    if {"cik","fy","filed"}.issubset(idx.columns):
        idx = (
            idx.sort_values(["cik","fy","filed"])
               .drop_duplicates(["cik","fy"], keep="last")
        )

    # Optional: save to Silver layer
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        idx.to_parquet(out_path, index=False)

    return idx
