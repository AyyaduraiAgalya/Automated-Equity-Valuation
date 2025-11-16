from src.data_prep.prep_config import PREP_CONFIG
import pandas as pd
import numpy as np
from pathlib import Path
# -----------------------------
# STEP 1: Check columns
# -----------------------------
def check_columns(df, config=PREP_CONFIG):
    expected = set(config["relevant_cols"])
    actual = set(df.columns)

    missing = expected - actual
    extra   = actual - expected

    if missing:
        print(f"[WARN] Missing expected columns: {sorted(missing)}")
    if extra:
        print(f"[INFO] Extra columns present (will be ignored by pipeline): {sorted(extra)}")

    # keep only relevant columns (drop extras)
    df = df[[c for c in config["relevant_cols"] if c in df.columns]]
    return df


# -----------------------------
# STEP 2: Drop duplicates
# -----------------------------
def drop_duplicates(df):
    # duplicate rule: same cik + fy + form -> keep latest 'filed'
    if not {"cik", "fy", "form", "filed"}.issubset(df.columns):
        return df

    df = df.sort_values("filed")  # older first
    df = df.drop_duplicates(subset=["cik", "fy", "form"], keep="last") # keeps the last (which is the latest) as it was sorted older first
    return df


# -----------------------------
# STEP 3: Fix dtypes
# -----------------------------
def fix_dtypes(df, config=PREP_CONFIG):
    # dates
    for col in config["date_cols"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # numerics
    for col in config["numeric_cols"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# -----------------------------
# STEP 4: Report missingness
# -----------------------------
def report_missingness(df):
    missing_pct = df.isna().mean().sort_values(ascending=False)
    print("\n[INFO] Missingness (% of rows with NaN) - top 20:")
    print((missing_pct * 100).round(2).head(20))
    return missing_pct


# -----------------------------
# STEP 5: Drop unusable rows
# -----------------------------
def drop_unusable_rows(df):
    essential = ["Revenue", "TotalAssets", "TotalLiabilities", "NetIncome", "CFO"]
    present_essential = [c for c in essential if c in df.columns]

    # drop rows where ALL essential fields are missing
    df = df.dropna(subset=present_essential, how="all")

    # drop weird rows where TotalAssets <= 0 (bad filings)
    if "TotalAssets" in df.columns:
        df = df[df["TotalAssets"] > 0]

    return df


# -----------------------------
# STEP 6: Fill zero-logic cols
# -----------------------------
def fill_zero_logic_columns(df, config=PREP_CONFIG):
    for col in config["zero_logic_cols"]:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)
    return df


# -----------------------------
# STEP 7: Winsorise numeric tails
# -----------------------------
def winsorize_numeric(df, config=PREP_CONFIG):
    lower = config["winsorize_lower"]
    upper = config["winsorize_upper"]

    for col in config["numeric_cols"]:
        if col not in df.columns:
            continue
        series = df[col]
        if series.notna().sum() == 0:
            continue
        lo = series.quantile(lower)
        hi = series.quantile(upper)
        df[col] = series.clip(lo, hi)
    return df


# -----------------------------
# STEP 8: Compute core ratios
# -----------------------------
def compute_core_ratios(df, config=PREP_CONFIG):
    for ratio_name, spec in config["core_ratio_defs"].items():
        num_spec, denom_col = spec

        if isinstance(num_spec, list):
            # sum of multiple numerator columns
            num = sum(df.get(c, 0) for c in num_spec)
        else:
            num = df.get(num_spec, np.nan)

        denom = df.get(denom_col, np.nan)

        with np.errstate(divide="ignore", invalid="ignore"):
            df[ratio_name] = num / denom

    return df


# -----------------------------
# STEP 9: Log-transform size features
# -----------------------------
def log_transform_size_features(df, config=PREP_CONFIG):
    for col in config["size_cols_log"]:
        if col in df.columns:
            # log1p to handle low values; set non-positive to NaN
            x = df[col].copy()
            x[x <= 0] = np.nan
            df[f"log_{col}"] = np.log1p(x)
    return df


# -----------------------------
# STEP 10: Standardise features
# -----------------------------
def standardise_features(df, config=PREP_CONFIG, scalers=None):
    """
    scalers: optional dict {col: (mean, std)}.
    If None, fit new; otherwise reuse (for new quarters).
    """
    if scalers is None:
        scalers = {}

    for col in config["scaled_cols"]:
        # use log-version if available, else raw
        base_col = f"log_{col}" if f"log_{col}" in df.columns else col
        if base_col not in df.columns:
            continue

        vals = df[base_col].astype(float)

        if col in scalers:
            mu, sigma = scalers[col]
        else:
            mu, sigma = vals.mean(), vals.std(ddof=0)
            scalers[col] = (mu, sigma)

        if sigma == 0 or np.isnan(sigma):
            df[f"z_{col}"] = np.nan
        else:
            df[f"z_{col}"] = (vals - mu) / sigma

    return df, scalers


# -----------------------------
# STEP 11: Map SIC → industry buckets
# -----------------------------

def map_sic_to_industry(df: pd.DataFrame,
                     sic_mapping_path: str | Path = "data/sic_office_industry.csv"
                    ) -> pd.DataFrame:
    """
    Attach SEC SIC Office and Industry Title metadata to a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame that must contain a 'sic' column (int or string).
    sic_mapping_path : str or Path, optional
        Path to CSV file with columns: 'sic', 'office', 'industry_title'.
        Defaults to 'data/sic_office_industry.csv'.

    Returns
    -------
    pd.DataFrame
        Copy of df with three new columns:
        - 'office'
        - 'industry_title'
        - 'office_industry_title'

    Notes
    -----
    The SIC mapping is sourced from:
    https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list
    """
    if "sic" not in df.columns:
    # nothing to do
        return df

    sic_mapping_path = Path(sic_mapping_path)

    # Load mapping
    mapping = pd.read_csv(
        sic_mapping_path,
        dtype={"sic": "Int64"}  # keep SIC as nullable integer
    )

    # Ensure consistent type for merge
    df_out = df.copy()
    df_out["sic"] = df_out["sic"].astype("Int64")

    # Merge on 'sic'
    df_out = df_out.merge(mapping, how="left", on="sic")

    # # Optional: combined column
    # df_out["office_industry_title"] = (
    #     df_out["office"].fillna("") + " - " + df_out["industry_title"].fillna("")
    # ).str.strip(" -")  # clean up if one side is missing

    return df_out


# -----------------------------
# MASTER PIPELINE
# -----------------------------
def process_fundamentals(df_raw, config=PREP_CONFIG, scalers=None, verbose=True):
    """
    Run the full cleaning pipeline on a raw fundamentals dataframe.
    Returns:
        df_clean, scalers
    """
    df = df_raw.copy()

    # 1. Column checks and subsetting
    df = check_columns(df, config=config)

    # 2. Drop duplicate filings
    df = drop_duplicates(df)

    # 3. Fix dtypes
    df = fix_dtypes(df, config=config)

    # 4. Report missingness (for your info)
    if verbose:
        report_missingness(df)

    # 5. Drop unusable rows
    df = drop_unusable_rows(df)

    # 6. Fill zero-logic columns
    df = fill_zero_logic_columns(df, config=config)

    # 7. Winsorise numeric columns
    df = winsorize_numeric(df, config=config)

    # 8. Compute core ratios
    df = compute_core_ratios(df, config=config)

    # 9. Log-transform size features
    df = log_transform_size_features(df, config=config)

    # 10. Standardise selected features
    df, scalers = standardise_features(df, config=config, scalers=scalers)

    # 11. Map SIC → industry buckets (stub)
    df = map_sic_to_industry(df)

    return df, scalers