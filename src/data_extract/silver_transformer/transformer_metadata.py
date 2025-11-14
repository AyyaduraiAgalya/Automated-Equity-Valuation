from __future__ import annotations
from pathlib import Path
import pandas as pd
from src.data_extract.bronze_extractor.extractor_metadata import extract_metadata
from src.data_prep.cik_ticker import ticker_from_instance, load_sec_cik_ticker_exchange

SEC_TICKER_PATH = Path("data/company_tickers_exchange.json")

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
}

def transform_metadata_to_wide(
    zip_path: Path,
    out_path: Path | None = None,
    forms=("10-K", "10-K/A"),
    fp="FY",
    sec_ticker_path: Path | None = SEC_TICKER_PATH,
) -> pd.DataFrame:
    """
    Silver transformer for filing-level metadata.

    Builds on Bronze filings_index output, enriches with industry names,
    infers ticker from XBRL instance filename, overlays SEC official mapping,
    and filters to one record per (cik, fy) — latest filed.
    """
    idx = extract_metadata(zip_path, forms=forms, fp=fp)
    if idx.empty:
        return pd.DataFrame()

    # --- Industry mapping ---
    if "sic" in idx.columns:
        idx["industry"] = idx["sic"].map(SIC_LOOKUP).fillna("Unknown")
        idx["sic"] = pd.to_numeric(idx["sic"], errors="coerce").astype("Int64")
    else:
        idx["industry"] = "Unknown"

    # --- Ticker inference from instance (if present) ---
    if "instance" in idx.columns:
        idx["ticker_inferred"] = idx["instance"].apply(ticker_from_instance)
    else:
        idx["ticker_inferred"] = pd.NA

    # --- SEC official mapping (optional but recommended) ---
    if sec_ticker_path is not None and sec_ticker_path.exists():
        sec_map = load_sec_cik_ticker_exchange(sec_ticker_path)
        sec_map["cik"] = pd.to_numeric(sec_map["cik"], errors="coerce").astype("Int64")

        idx["cik"] = pd.to_numeric(idx["cik"], errors="coerce").astype("Int64")
        idx = idx.merge(
            sec_map[["cik", "ticker_sec"]],
            on="cik",
            how="left"
        )
        # Final ticker: prefer SEC mapping, fallback to instance inference
        idx["ticker"] = idx["ticker_sec"].fillna(idx["ticker_inferred"])
        idx = idx.drop(columns=[c for c in ["ticker_sec", "ticker_inferred"] if c in idx.columns])
    else:
        # No SEC mapping available, just use inferred ticker
        idx = idx.rename(columns={"ticker_inferred": "ticker"})

    # --- Deduplicate: keep latest filed per (cik, fy) ---
    if {"cik","fy","filed"}.issubset(idx.columns):
        idx = (
            idx.sort_values(["cik","fy","filed"])
               .drop_duplicates(["cik","fy"], keep="last")
        )

    # --- Optional: save to Silver layer ---
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        idx.to_parquet(out_path, index=False)

    return idx
