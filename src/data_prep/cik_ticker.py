# src/market_data/cik_ticker.py

import pandas as pd
import requests
import json 
import os
from collections import Counter
from typing import Callable

SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"


def load_cik_ticker_map(from_web: bool = False, local_path: str | None = None) -> pd.DataFrame:
    """
    Load CIK -> ticker mapping from SEC or from a local JSON file.
    Returns DataFrame with columns: cik, ticker, name_sec
    """
    # Load from SEC website
    if from_web:
        headers = {"User-Agent": "your_name your_email for academic research"}
        resp = requests.get(SEC_TICKER_URL, headers=headers)
        resp.raise_for_status()
        raw = resp.json()

    else:
        if local_path is None:
            raise ValueError("Must provide local_path when from_web=False")
        
        raw = pd.read_json(local_path, typ="series").to_dict()  # SEC JSON shape

    # Parse JSON structure -> records
    records = []
    for v in raw.values():
        try:
            cik_int = int(v["cik_str"])
            ticker = v["ticker"].upper()
            name_sec = v["title"]
        except Exception:
            continue
        
        records.append({
            "cik": cik_int,
            "ticker": ticker,
            "name_sec": name_sec,
        })

    df_map = pd.DataFrame(records)

    # Make sure CIK is numeric and unique
    df_map["cik"] = pd.to_numeric(df_map["cik"], errors="coerce").astype("Int64")
    df_map = df_map.drop_duplicates(subset=["cik"]).reset_index(drop=True)

    return df_map


def attach_tickers(df_fundamentals: pd.DataFrame, cik_map: pd.DataFrame) -> pd.DataFrame:
    """
    Merge ticker symbols onto your cleaned fundamentals dataframe using CIK.
    """
    df = df_fundamentals.copy()

    df["cik"] = pd.to_numeric(df["cik"], errors="coerce").astype("Int64")
    cik_map["cik"] = pd.to_numeric(cik_map["cik"], errors="coerce").astype("Int64")

    df = df.merge(cik_map[["cik", "ticker"]], on="cik", how="left")

    matched = df["ticker"].notna().mean() * 100
    print(f"[INFO] Matched tickers for {matched:.2f}% of rows.")

    return df

import json
import pandas as pd

def load_sec_cik_ticker_exchange(local_path: str) -> pd.DataFrame:
    """
    Load SEC company_tickers_exchange.json (table-style) and return cik -> ticker mapping.

    Expects JSON like:
    {
      "fields": ["cik","name","ticker","exchange"],
      "data": [
        [1045810,"NVIDIA CORP","NVDA","Nasdaq"],
        ...
      ]
    }
    """
    with open(local_path, "r") as f:
        raw = json.load(f)

    fields = raw["fields"]
    data = raw["data"]

    df = pd.DataFrame(data, columns=fields)

    # Normalise column names
    df.columns = [c.lower() for c in df.columns]

    # Rename to our internal names
    df = df.rename(columns={
        "ticker": "ticker_sec",
        "name": "name_sec",
    })

    # Type + cleanup
    df["cik"] = pd.to_numeric(df["cik"], errors="coerce").astype("Int64")
    df["ticker_sec"] = df["ticker_sec"].str.upper()
    df["exchange"] = df["exchange"].astype(str)

    df = df.dropna(subset=["cik", "ticker_sec"])
    df = df.drop_duplicates(subset=["cik"]).reset_index(drop=True)

    return df

def infer_cik_tickers_from_fsds_zips(
    zips_root: str,
    load_fsds_from_zip: Callable[[str], dict],
) -> pd.DataFrame:
    """
    Go through all FSDS zip files under `zips_root`, extract the `sub` table,
    and infer a ticker-like string from the 'instance' filename prefix
    (before the first hyphen), e.g. 'nflx-20121231.xml' -> 'NFLX'.

    Returns a DataFrame with columns: cik, ticker_inferred
    where ticker_inferred is the most common inferred ticker per cik.
    """

    records = []

    for fname in os.listdir(zips_root):
        if not fname.lower().endswith(".zip"):
            continue

        zip_path = os.path.join(zips_root, fname)
        print(f"[INFO] Processing zip: {zip_path}")

        try:
            dfs = load_fsds_from_zip(zip_path)
        except Exception as e:
            print(f"[WARN] Failed to load {zip_path}: {e}")
            continue

        if "sub" not in dfs:
            continue

        sub = dfs["sub"]

        # We now EXPECT 'instance' because we added it to sub_cols.
        if not {"cik", "instance"}.issubset(sub.columns):
            print(f"[WARN] 'cik' or 'instance' missing in sub from {zip_path}")
            continue

        sub = sub[["cik", "instance"]].dropna()
        sub["cik"] = pd.to_numeric(sub["cik"], errors="coerce").astype("Int64")

        def infer_from_instance(inst: str) -> str | None:
            inst = str(inst)
            base = inst.split(".")[0]      # dg-20130201.xml -> dg-20130201
            prefix = base.split("-")[0]    # dg-20130201 -> dg
            cand = prefix.strip().upper()  # DG

            # Heuristic: tickers usually 1–5 letters, all alphabetic
            if 1 <= len(cand) <= 5 and cand.isalpha():
                return cand
            return None

        sub["ticker_inferred"] = sub["instance"].apply(infer_from_instance)
        sub = sub.dropna(subset=["ticker_inferred"])

        for _, row in sub.iterrows():
            records.append({
                "cik": row["cik"],
                "ticker_inferred": row["ticker_inferred"],
            })

    if not records:
        print("[WARN] No tickers inferred from FSDS zips.")
        return pd.DataFrame(columns=["cik", "ticker_inferred"])

    df_all = pd.DataFrame(records)
    df_all["cik"] = pd.to_numeric(df_all["cik"], errors="coerce").astype("Int64")

    # For each cik, choose the most common inferred ticker
    cik_best = []
    for cik, group in df_all.groupby("cik"):
        counts = Counter(group["ticker_inferred"])
        best_ticker, _ = counts.most_common(1)[0]
        cik_best.append({"cik": cik, "ticker_inferred": best_ticker})

    df_best = pd.DataFrame(cik_best)
    return df_best

def ticker_from_instance(instance: str) -> str | None:
    if not isinstance(instance, str):
        return None
    base = instance.split(".")[0]      # dg-20130201.xml -> dg-20130201
    prefix = base.split("-")[0]        # dg-20130201 -> dg
    cand = prefix.strip().upper()      # DG
    if 1 <= len(cand) <= 5 and cand.isalpha():
        return cand
    return None
