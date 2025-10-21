from pathlib import Path
import pandas as pd
from collections import defaultdict
from src.data_prep.fsds_loader import load_fsds_from_zip

FORMS = {"10-K", "10-K/A", "20-F", "40-F"}

def _fy_at_period_filter(df_num: pd.DataFrame, df_sub: pd.DataFrame, stmt: str) -> pd.DataFrame:
    df = df_num.merge(df_sub[["adsh","period","form","fp"]], on="adsh", how="left")
    df = df[df["form"].isin(FORMS) & (df["fp"].str.upper() == "FY")]
    if stmt == "BS":
        df = df[(df["qtrs"] == "0") & (df["ddate"] == df["period"])]
    else:  # IS/CF
        df = df[(df["qtrs"] == "4") & (df["ddate"] == df["period"])]
    return df

def suggest_from_zip(zip_path: Path, top_n: int = 40) -> dict:
    dfs = load_fsds_from_zip(zip_path)
    sub, pre, num, tag = dfs["sub"], dfs["pre"], dfs["num"], dfs["tag"]

    # normalize name/conm
    if "name" not in sub.columns and "conm" in sub.columns:
        sub = sub.rename(columns={"conm": "name"})

    # map stmt tags from PRE
    pre["stmt"] = pre["stmt"].str.upper()
    stmt_tags = {
        "BS": pre.loc[pre["stmt"] == "BS", ["adsh","tag"]].drop_duplicates(),
        "IS": pre.loc[pre["stmt"] == "IS", ["adsh","tag"]].drop_duplicates(),
        "CF": pre.loc[pre["stmt"] == "CF", ["adsh","tag"]].drop_duplicates(),
    }

    # helper to compute top tags by # of distinct filings using them
    def top_tags_for_stmt(stmt):
        st = stmt_tags[stmt]
        df = num.merge(st, on=["adsh","tag"], how="inner")
        df = _fy_at_period_filter(df, sub, stmt)
        # monetary top
        mon = df[df["uom"].str.lower().str.startswith("usd")].copy()
        mon_top = (mon.groupby("tag")["adsh"].nunique()
                      .sort_values(ascending=False).head(top_n).reset_index()
                      .rename(columns={"adsh":"filings_using"}))
        # join labels
        mon_top = mon_top.merge(tag[["tag","tlabel"]], on="tag", how="left")
        # shares/per-share top
        sh = df[df["uom"].str.lower().eq("shares")]
        sh_top = (sh.groupby("tag")["adsh"].nunique()
                    .sort_values(ascending=False).head(top_n).reset_index()
                    .rename(columns={"adsh":"filings_using"}))
        sh_top = sh_top.merge(tag[["tag","tlabel"]], on="tag", how="left")
        return mon_top, sh_top

    out = {}
    for s in ("BS","IS","CF"):
        mon_top, sh_top = top_tags_for_stmt(s)
        out[s] = {
            "monetary_top": mon_top.sort_values("filings_using", ascending=False),
            "shares_top": sh_top.sort_values("filings_using", ascending=False),
        }
    return out

def save_suggestions(sug: dict, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    for s in ("BS","IS","CF"):
        sug[s]["monetary_top"].to_csv(out_dir / f"{s.lower()}_monetary_top.csv", index=False)
        sug[s]["shares_top"].to_csv(out_dir / f"{s.lower()}_shares_top.csv", index=False)
