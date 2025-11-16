from pathlib import Path
import pandas as pd
from .fsds_loader import load_fsds_from_zip

def extract_metadata(
    zip_path: Path,
    forms=("10-K", "10-K/A", "10-Q", "10-Q/A"),
    fp=None,               # e.g., "FY" for annual only; None = keep all
    out_path: Path | None = None,
) -> pd.DataFrame:
    """
    Build a per-ZIP 'filings index'.

    Args:
        zip_path: Path to one FSDS ZIP (e.g., data/raw/2025q2.zip)
        forms: tuple of form types to include.
        fp: fiscal period filter.
            - None       -> keep all fiscal periods (FY, Q1, Q2, ...)
            - "FY"       -> keep only annual
            - ("Q1","Q2") -> keep specific periods
        out_path: if provided, write the result as a Parquet file.
    """

    dfs = load_fsds_from_zip(zip_path)
    sub = dfs["sub"].copy()

    # Normalise company name column across quarters
    if "name" not in sub.columns and "conm" in sub.columns:
        sub = sub.rename(columns={"conm": "name"})

    # Base form filter
    mask = sub["form"].isin(forms)

    # Optional fiscal period filter
    if fp is not None:
        fp_upper = sub["fp"].astype(str).str.upper()
        if isinstance(fp, (list, tuple, set)):
            fp_vals = {str(x).upper() for x in fp}
            mask = mask & fp_upper.isin(fp_vals)
        else:
            mask = mask & (fp_upper == str(fp).upper())

    # Metadata columns we want to keep
    cols = [
        "adsh", "cik", "name", "form", "fy", "fp",
        "period", "filed", "sic", "instance",
        "fye", "accepted", "countryba", "stprba",
    ]
    cols = [c for c in cols if c in sub.columns]

    idx = sub.loc[mask, cols].drop_duplicates()

    idx = idx.sort_values(
        ["filed", "cik", "fy"],
        ascending=[False, True, True],
    ).reset_index(drop=True)

    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        idx.to_parquet(out_path, index=False)

    return idx
