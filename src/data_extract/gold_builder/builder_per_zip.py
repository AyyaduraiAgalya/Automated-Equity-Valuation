from __future__ import annotations
from pathlib import Path
import pandas as pd

def _dedup_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.loc[:, ~df.columns.duplicated()].copy()

def _outer_join(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    right = _dedup_cols(right)
    if right.empty:
        return left
    # since we aggressively drop duplicate ID/meta cols on the right,
    # suffixes can safely be blank
    return pd.merge(left, right, on="adsh", how="outer", suffixes=("", ""))

def _safe_read(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()

def _latest_per_cik_fy(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    keep_cols = [c for c in ["cik","fy","filed"] if c in df.columns]
    if len(keep_cols) < 3:
        return df
    return (
        df.sort_values(["cik","fy","filed"])
          .drop_duplicates(["cik","fy"], keep="last")
    )

def _qc_flags(df: pd.DataFrame) -> pd.DataFrame:
    has = df.columns

    # BS balance (optionally include TemporaryEquity if present)
    tl = "TotalLiabilities" in has
    se = "ShareholdersEquity" in has
    ta = "TotalAssets" in has
    te = "TemporaryEquity" in has
    if ta and tl and se:
        rhs = df["TotalLiabilities"].fillna(0) + df["ShareholdersEquity"].fillna(0)
        if te:
            rhs = rhs + df["TemporaryEquity"].fillna(0)
        df["bs_diff"] = (df["TotalAssets"] - rhs).abs()
        df["bs_balanced_flag"] = df["bs_diff"] <= 1_000  # $1k tolerance
    else:
        df["bs_balanced_flag"] = pd.NA

    # CF identity CFO+CFI+CFF ~ 0
    for col in ["CFO","CFI","CFF"]:
        if col not in has:
            df["cf_balanced_flag"] = pd.NA
            break
    else:
        delta = df[["CFO","CFI","CFF"]].sum(axis=1, skipna=True)
        df["cf_delta_abs"] = delta.abs()
        df["cf_balanced_flag"] = df["cf_delta_abs"] <= 1_000

    # Coverage score over key set (using canonical names)
    key_cols = [c for c in [
        "TotalAssets","TotalLiabilities","ShareholdersEquity",
        "Revenue","OperatingIncome","NetIncome",
        "CFO","CFI","CFF",
        "CommonSharesOutstanding","WASOBasic","WASODiluted",
    ] if c in has]
    if key_cols:
        df["coverage_score"] = df[key_cols].notna().mean(axis=1)
    else:
        df["coverage_score"] = pd.NA

    return df

def build_gold_zip(
    yq: str,                   # e.g., "2025q2"
    silver_dir: Path,          # e.g., Path("data/silver")
    out_dir: Path,             # e.g., Path("data/gold")
) -> Path:
    """
    Join BS + IS + CF + Shares + Metadata for a single ZIP (YYYYq#) into
    gold/{YYYYq#}_financials.parquet.

    For this version we keep **one row per filing (adsh)**, including both
    annual and quarterly forms. No collapsing by (cik, fy).
    """
    base = silver_dir / f"bs/year_quarter={yq}"
    bs_path   = base / "bs.parquet"
    is_path   = silver_dir / f"is/year_quarter={yq}/is.parquet"
    cf_path   = silver_dir / f"cf/year_quarter={yq}/cf.parquet"
    sh_path   = silver_dir / f"shares/year_quarter={yq}/shares.parquet"
    meta_path = silver_dir / f"meta/year_quarter={yq}/meta.parquet"

    bs   = _safe_read(bs_path)
    is_  = _safe_read(is_path)
    cf   = _safe_read(cf_path)
    sh   = _safe_read(sh_path)
    meta = _safe_read(meta_path)

    # Start with metadata (preferred ID grid)
    df = _dedup_cols(meta.copy())
    if df.empty:
        # Fallback: reconstruct IDs from whatever exists
        dfs = [d for d in [bs, is_, cf, sh] if not d.empty]
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{yq}_financials.parquet"
        if not dfs:
            pd.DataFrame().to_parquet(out_path, index=False)
            return out_path

        df = dfs[0]
        for d in dfs[1:]:
            core = d[["adsh","cik","fy","filed","period","name","sic"]].drop_duplicates("adsh")
            df = pd.merge(df, core, on="adsh", how="outer", suffixes=("","_dup"))
        for col in ["cik","fy","filed","period","name","sic"]:
            dup = f"{col}_dup"
            if dup in df.columns:
                df[col] = df[col].fillna(df[dup])
                df = df.drop(columns=[dup])
    # 🔑 List of metadata/ID-like columns we only want from META
    META_ID_COLS = [
        "cik","fy","filed","period","name","sic",
        "form","fp",               # these caused the error
        "instance","fye","accepted",
        "countryba","stprba",
        "ticker","source_zip",
    ]

    # Outer-join BS/IS/CF (drop all meta-ish cols from right)
    for part in [bs, is_, cf]:
        if not part.empty:
            part = _dedup_cols(part)
            drop_cols = [c for c in META_ID_COLS if c in part.columns]
            non_id_cols = [c for c in part.columns if c not in drop_cols and c != "adsh"]
            keep = ["adsh"] + non_id_cols
            df = _outer_join(df, part[keep])

    # Left-join Shares similarly (no extra IDs from right)
    if not sh.empty:
        sh = _dedup_cols(sh)
        drop_cols = [c for c in META_ID_COLS if c in sh.columns]
        non_id_cols = [c for c in sh.columns if c not in drop_cols and c != "adsh"]
        keep = ["adsh"] + non_id_cols
        df = pd.merge(df, sh[keep], on="adsh", how="left")

    # Deduplicate per (cik, fy) keep latest filed
    df = _latest_per_cik_fy(df)

    # Add source_zip (quarter marker)
    df["source_zip"] = yq

    df = _dedup_cols(df)
    df = _qc_flags(df)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{yq}_financials.parquet"
    df.to_parquet(out_path, index=False)
    return out_path