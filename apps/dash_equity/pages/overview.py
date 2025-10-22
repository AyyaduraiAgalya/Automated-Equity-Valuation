# apps/dash_equity/pages/overview.py

import dash
from dash import html, dcc
import pandas as pd
import plotly.express as px
import os

# Register this as a Dash page
dash.register_page(__name__, path="/", name="Overview")

# --- Load Data ---
PARQUET_PATH = os.path.join("data", "gold", "financials_panel.parquet")

try:
    df = pd.read_parquet(PARQUET_PATH)
except Exception as e:
    df = pd.DataFrame({"Error": [str(e)]})

# --- Use relevant columns ---
if all(col in df.columns for col in ["name", "Revenue", "period"]):
    df = df[["name", "Revenue", "period"]].dropna()
    df["period"] = pd.to_datetime(df["period"], errors="coerce")

    # Aggregate in case multiple entries exist per company-period
    df = df.groupby(["name", "period"], as_index=False)["Revenue"].sum()

    # Select top 10 companies by total revenue
    top10 = df.groupby("name")["Revenue"].sum().nlargest(10).index
    df_top = df[df["name"].isin(top10)]

    fig = px.line(
        df_top,
        x="period",
        y="Revenue",
        color="name",
        markers=True,
        title="Revenue Trend — Top 10 Companies by Total Revenue",
        labels={"period": "Period End", "Revenue": "Revenue", "name": "Company"},
    )
else:
    import plotly.graph_objects as go
    fig = go.Figure()
    fig.add_annotation(
        text="Columns 'name', 'Revenue', or 'period' missing in parquet.",
        x=0.5, y=0.5, showarrow=False, font=dict(size=14)
    )
    fig.update_layout(title="Data Error")

# --- Page Layout ---
layout = html.Div(
    [
        html.H2("Overview"),
        html.P("Shows Revenue trends for the top 10 companies in the dataset."),
        dcc.Graph(figure=fig),
    ],
    style={"padding": "20px"},
)
