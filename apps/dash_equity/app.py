from dash import Dash, html, dcc
import dash

app = Dash(__name__, use_pages=True, suppress_callback_exceptions=True)
app.title = "Equity Research"

app.layout = html.Div(
    [
        html.Nav(
            [dcc.Link(p["name"], href=p["path"], style={"marginRight": 12})
             for p in dash.page_registry.values()]
        ),
        dash.page_container,
    ],
    style={"padding": 12, "fontFamily": "system-ui, -apple-system, Segoe UI, Roboto, sans-serif"},
)

if __name__ == "__main__":
    app.run(debug=True)
