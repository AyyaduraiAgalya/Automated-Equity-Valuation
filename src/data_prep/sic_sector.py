import pandas as pd
from pathlib import Path

def add_sic_metadata(df: pd.DataFrame,
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
