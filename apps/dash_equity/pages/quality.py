import dash
from dash import html, dcc
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

dash.register_page(__name__, path="/quality", name="Data Quality")

PARQUET_PATH = os.path.join("data", "gold", "financials_panel.parquet")

# ---------- load ----------
try:
    df = pd.read_parquet(PARQUET_PATH)
except Exception as e:
    df = pd.DataFrame({"__error__": [str(e)]})

# ---------- figure 1: Missingness ----------
def build_missingness_fig(_df: pd.DataFrame):
    if _df.empty:
        fig = go.Figure()
        fig.update_layout(title="No data loaded")
        return fig

    miss = _df.isna().mean().sort_values(ascending=False).head(25)
    miss = miss.reset_index()
    miss.columns = ["column", "missing_share"]
    fig = px.bar(
        miss,
        x="column",
        y="missing_share",
        title="Top 25 Columns by Missingness",
        labels={"missing_share": "Missing (share)"},
    )
    fig.update_layout(xaxis_tickangle=-45, margin=dict(b=120))
    return fig

# ---------- figure 2: Balance sheet identity error ----------
def build_bs_identity_fig(_df: pd.DataFrame):
    needed = {"TotalAssets", "TotalLiabilities", "ShareholdersEquity"}
    if not needed.issubset(_df.columns):
        fig = go.Figure()
        fig.update_layout(
            title="Balance Sheet Identity",
            annotations=[dict(
                text="Need columns: TotalAssets, TotalLiabilities, ShareholdersEquity",
                x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False
            )]
        )
        return fig

    df2 = _df.copy()
    # parse period if present
    if "period" in df2.columns:
        df2["period"] = pd.to_datetime(df2["period"], errors="coerce")

    A = df2["TotalAssets"]
    L = df2["TotalLiabilities"]
    E = df2["ShareholdersEquity"]
    rhs_candidates = [("L+E", L + E)]

    if "NoncontrollingInterest" in df2.columns:
        rhs_candidates.append(("L+E+NCI", L + E + df2["NoncontrollingInterest"]))
    if "TemporaryEquity" in df2.columns:
        rhs_candidates.append(("L+E+TempEq", L + E + df2["TemporaryEquity"]))
    if ("NoncontrollingInterest" in df2.columns) and ("TemporaryEquity" in df2.columns):
        rhs_candidates.append(
            ("L+E+NCI+TempEq", L + E + df2["NoncontrollingInterest"] + df2["TemporaryEquity"])
        )

    # compute relative error for each candidate and pick the smallest per row
    rel_errors = []
    for label, rhs in rhs_candidates:
        err = (A - rhs).abs() / A.replace(0, pd.NA)
        rel_errors.append(err.rename(label))

    rel_df = pd.concat(rel_errors, axis=1)
    best_rel_err = rel_df.min(axis=1) * 100.0  # in %

    # histogram of best relative error
    fig = px.histogram(
        best_rel_err.dropna(),
        nbins=40,
        title="Assets vs (Liabilities + Equity) — Best Relative Error (%)",
        labels={"value": "Relative Error (%)"},
    )

    # annotate tail share
    try:
        share_over_1pct = (best_rel_err > 1.0).mean() * 100
        fig.add_annotation(
            text=f"Rows with error > 1%: {share_over_1pct:.1f}%",
            x=0.98, y=0.98, xref="paper", yref="paper", showarrow=False,
            align="right", bgcolor="rgba(0,0,0,0.05)"
        )
    except Exception:
        pass

    return fig

missing_fig = build_missingness_fig(df)
bs_fig = build_bs_identity_fig(df)

layout = html.Div(
    [
        html.H2("Data Quality"),
        html.P("Quick checks: column completeness and basic balance sheet consistency."),
        dcc.Graph(figure=missing_fig),
        html.Hr(),
        dcc.Graph(figure=bs_fig),
    ],
    style={"padding": "20px"},
)
