# fsds_io.py
from pathlib import Path
import zipfile
import io
import pandas as pd

# ---------- Config: canonical BS tags (synonyms → standard names) ----------
BS_CANON_MAP = {
    # core totals
    "Assets": ["Assets"],
    "Liabilities": ["Liabilities"],
    "StockholdersEquity": [
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "StockholdersEquity"
    ],
    # liquidity
    "AssetsCurrent": ["AssetsCurrent"],
    "LiabilitiesCurrent": ["LiabilitiesCurrent"],
    "CashAndCashEquivalents": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsAndShortTermInvestments"
    ],
    # working capital components (optional but useful)
    "AccountsReceivableCurrent": ["AccountsReceivableNetCurrent","AccountsReceivableCurrent"],
    "AccountsPayableCurrent": ["AccountsPayableCurrent"],
    "Inventory": ["InventoryNet","Inventory"],
    # debt
    "LongTermDebtNoncurrent": ["LongTermDebtNoncurrent","LongTermDebtAndCapitalLeaseObligations"],
    # equity details (optional)
    "RetainedEarningsAccumulatedDeficit": ["RetainedEarningsAccumulatedDeficit"],
}

# Flatten reverse lookup for quick mapping
def build_reverse_map(canon_map):
    rev = {}
    for canon, alts in canon_map.items():
        for t in alts:
            rev[t] = canon
        # also allow the canonical key itself if it appears
        rev[canon] = canon
    return rev

BS_REVERSE_TAG_MAP = build_reverse_map(BS_CANON_MAP)

# ---------- Helper: read a TXT inside a ZIP as a DataFrame (tab-delimited) ----------
def _read_tab_from_zip(zip_path: Path, member_name: str, usecols=None) -> pd.DataFrame:
    """
    Read a tab-delimited FSDS .txt file from within a ZIP into a pandas DataFrame.
    - Forces dtype=str (safe), marks \\N as NaN, low_memory=False (consistent types).
    - Tolerates missing columns by intersecting requested columns with available ones.
    """
    import io
    import pandas as pd
    import zipfile

    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(member_name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")

            # First read only header to discover actual columns
            header_df = pd.read_csv(
                text, sep="\t", na_values="\\N", dtype=str,
                low_memory=False, nrows=0
            )
            actual_cols = set(header_df.columns)

    # Reopen to read full content (we consumed the stream above)
    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(member_name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")

            if usecols is None:
                wanted = None
            else:
                # intersect requested with actual
                wanted = [c for c in usecols if c in actual_cols]
                if not wanted:
                    # if none match, just read all to avoid empty frames
                    wanted = None

            df = pd.read_csv(
                text,
                sep="\t",
                na_values="\\N",
                dtype=str,
                low_memory=False,
                usecols=wanted
            )
    return df

def load_fsds_tables(zip_path: Path):
    """
    Load only the 3 needed tables: sub, pre, num (minimal columns for BS).
    Returns: sub, pre, num (DataFrames)
    """
    # Find the folder inside the zip (e.g., '2025q2/2025q2/')
    with zipfile.ZipFile(zip_path, "r") as z:
        names = z.namelist()
        # We expect .../sub.txt etc.
        sub_name = next(n for n in names if n.lower().endswith("sub.txt"))
        pre_name = next(n for n in names if n.lower().endswith("pre.txt"))
        num_name = next(n for n in names if n.lower().endswith("num.txt"))

    # Minimal columns we need now (keeps memory light)
    sub_cols = ["adsh","cik","name","form","fy","fp","period","filed"]
    pre_cols = ["adsh","tag","stmt","report","line"]
    num_cols = ["adsh","tag","ddate","qtrs","uom","value","coreg","dimh"]

    sub = _read_tab_from_zip(zip_path, sub_name, usecols=sub_cols)
    pre = _read_tab_from_zip(zip_path, pre_name, usecols=pre_cols)
    num = _read_tab_from_zip(zip_path, num_name, usecols=num_cols)

    return sub, pre, num

# ---------- Helper: get list of 10-K adshs from one ZIP ----------
def list_annual_10k_adsh(sub: pd.DataFrame):
    """
    Return adsh list for 10-K annual filings (form in 10-K/20-F/40-F, fp='FY').
    """
    mask = (sub["form"].isin(["10-K","20-F","40-F"])) & (sub["fp"].str.upper() == "FY")
    adshs = sub.loc[mask, "adsh"].dropna().unique().tolist()
    return adshs

# from pathlib import Path
# from fsds_io import load_fsds_tables, list_annual_10k_adsh, BS_REVERSE_TAG_MAP

zip_path = "/Users/agalyaayyadurai/Documents/Dissertation/Automated-Equity-Valuation/data/sec/2025q2.zip"
sub, pre, num = load_fsds_tables(zip_path)
adshs = list_annual_10k_adsh(sub)
print(len(adshs), adshs[:5])
print("Sample of reverse map:", list(BS_REVERSE_TAG_MAP.items())[:5])
