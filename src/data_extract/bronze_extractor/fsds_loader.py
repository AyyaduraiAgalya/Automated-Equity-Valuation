from pathlib import Path
import zipfile
import io
import pandas as pd

def load_fsds_from_zip(zip_path: Path):
    """
    Load the four core tables (sub, pre, num, tag) from a single SEC FSDS ZIP file.

    Args:
        zip_path (Path): Path to the SEC FSDS ZIP file (e.g., 'data/2025q2.zip').

    Returns:
        dict[str, pd.DataFrame]: Dictionary with DataFrames for:
            - sub: Submission index (company + filing info)
            - pre: Presentation linkbase (tags grouped by statements)
            - num: Numeric facts (actual financial data)
            - tag: Tag metadata (names, labels, abstracts)
    """

    # Find paths to the four .txt files inside the ZIP
    with zipfile.ZipFile(zip_path, "r") as z:
        names = z.namelist()
        need = {}
        for key in ("sub", "pre", "num", "tag"):
            try:
                # SEC files look like "2025q2/sub.txt", "2025q2/num.txt", etc.
                # need[key] = next(n for n in names if n.lower() == (f"{key}.txt"))
                need[key] = next(
                    n for n in names
                    if n.lower().endswith(f"/{key}.txt") or n.lower() == f"{key}.txt"
                )
            except StopIteration:
                raise FileNotFoundError(f"{key}.txt not found in {zip_path.name}")

    # Helper to safely read any member file into a DataFrame
    def _read_member(member_name: str, wanted_cols=None):
        """Read one table from inside the ZIP, handling missing columns safely."""
        # Read only the header first to detect what columns exist
        with zipfile.ZipFile(zip_path, "r") as z:
            with z.open(member_name) as f:
                hdr = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")
                header_df = pd.read_csv(hdr, sep="\t", nrows=0, dtype=str)
                actual_cols = list(header_df.columns)

        # Pick only the wanted columns that actually exist
        usecols = [c for c in (wanted_cols or []) if c in actual_cols] or None

        # Reopen and load the full data file using those columns
        with zipfile.ZipFile(zip_path, "r") as z:
            with z.open(member_name) as f:
                text_stream = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")
                return pd.read_csv(
                    text_stream,
                    sep="\t",
                    na_values="\\N",  # SEC's null marker
                    dtype=str,        # safer for mixed data
                    low_memory=False,
                    usecols=usecols
                )

    # Define our target columns (these rarely change)
    sub_cols = ["adsh", "cik", "name", "form", "fy", "fp", "period", "filed", "sic",
    "instance", "fye", "accepted", "countryba", "stprba"]
    pre_cols = ["adsh", "tag", "stmt", "report", "line", "version"]
    num_cols = ["adsh", "tag", "ddate", "qtrs", "uom", "value", "coreg", "dimh", "iprx", "version"]
    tag_cols = ["tag", "tlabel", "version", "abstract"]

    # Read all four tables and return them
    return {
        "sub": _read_member(need["sub"], sub_cols),
        "pre": _read_member(need["pre"], pre_cols),
        "num": _read_member(need["num"], num_cols),
        "tag": _read_member(need["tag"], tag_cols),
    }
