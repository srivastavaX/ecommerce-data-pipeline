import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc

from visualization.data_loader import (
    load_products_enriched,
    load_price_predictions,
    load_price_coefficients,
    load_rating_predictions,
    load_rating_coefficients,
)
from visualization.charts import overview_charts as ov
from visualization.charts import cluster_charts  as cl
from visualization.charts import price_charts    as pr
from visualization.charts import rating_charts   as ra


# APP INITIALIZATION
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.CYBORG],   # dark Bootstrap theme
    title="E-Commerce Pipeline Dashboard",
    suppress_callback_exceptions=True,
)


# HELPER FUNCTIONS
def _graph(fig_id: str, figure, height: int = 420) -> dcc.Graph:
    return dcc.Graph(
        id=fig_id,
        figure=figure,
        config={"displayModeBar": True, "scrollZoom": False},
        style={"height": f"{height}px"},
    )


def _section_title(text: str) -> html.H5:
    return html.H5(
        text,
        style={"marginTop": "24px", "marginBottom": "8px", "color": "#adb5bd"},
    )


def _kpi_card(label: str, value: str) -> dbc.Card:
    return dbc.Card(
        dbc.CardBody([
            html.P(label, className="text-muted mb-1", style={"fontSize": "0.75rem"}),
            html.H4(value, className="mb-0 text-white"),
        ]),
        className="text-center",
        style={"background": "#1e2130", "border": "1px solid #343a56"},
    )


# TAB: OVERVIEW
def build_overview_tab() -> dbc.Container:
    df = load_products_enriched()

    kpis = dbc.Row([
        dbc.Col(_kpi_card("Total Products",   str(len(df))), md=3),
        dbc.Col(_kpi_card("Categories",       str(df["category"].nunique())), md=3),
        dbc.Col(_kpi_card("Avg Price",        f"${df['price'].mean():.2f}"), md=3),
        dbc.Col(_kpi_card("Avg Rating",       f"{df['rating'].mean():.2f} ★"), md=3),
    ], className="g-3 mb-3")

    return dbc.Container([
        html.H4("Product Overview", className="mt-3 mb-1 text-white"),
        html.Hr(style={"borderColor": "#343a56"}),
        kpis,
        dbc.Row([
            dbc.Col(_graph("ov-price-dist",    ov.price_distribution(df)),        md=6),
            dbc.Col(_graph("ov-price-cat",     ov.avg_price_by_category(df)),     md=6),
        ], className="g-3"),
        dbc.Row([
            dbc.Col(_graph("ov-rating-cat",    ov.avg_rating_by_category(df)),    md=6),
            dbc.Col(_graph("ov-discount-scatter", ov.discount_vs_price_scatter(df)), md=6),
        ], className="g-3"),
        dbc.Row([
            dbc.Col(_graph("ov-price-band",    ov.price_band_donut(df)),          md=6),
            dbc.Col(_graph("ov-rating-band",   ov.rating_band_donut(df)),         md=6),
        ], className="g-3"),
    ], fluid=True)


# TAB: CLUSTERING
def build_cluster_tab() -> dbc.Container:
    df = load_products_enriched()

    return dbc.Container([
        html.H4("Product Clustering (KMeans, k=4)", className="mt-3 mb-1 text-white"),
        html.Hr(style={"borderColor": "#343a56"}),
        html.P(
            "Products are grouped by price, rating, discount % and stock level. "
            "Bubble size = stock quantity.",
            className="text-muted",
        ),
        dbc.Row([
            dbc.Col(_graph("cl-scatter",   cl.cluster_scatter(df),          height=480), md=8),
            dbc.Col(_graph("cl-dist-bar",  cl.cluster_distribution_bar(df), height=480), md=4),
        ], className="g-3"),
        dbc.Row([
            dbc.Col(_graph("cl-price-box", cl.cluster_price_box(df)),  md=6),
            dbc.Col(_graph("cl-rate-box",  cl.cluster_rating_box(df)), md=6),
        ], className="g-3"),
        dbc.Row([
            dbc.Col(_graph("cl-heatmap",   cl.cluster_profile_heatmap(df), height=360), md=12),
        ], className="g-3"),
    ], fluid=True)


# TAB: PRICE PREDICTION
def build_price_tab() -> dbc.Container:
    df_preds = load_price_predictions()
    df_coeff = load_price_coefficients()

    mse  = (df_preds["residual"] ** 2).mean()
    rmse = mse ** 0.5
    mae  = df_preds["residual"].abs().mean()

    kpis = dbc.Row([
        dbc.Col(_kpi_card("Test Samples",  str(len(df_preds))),       md=4),
        dbc.Col(_kpi_card("RMSE",          f"${rmse:.2f}"),           md=4),
        dbc.Col(_kpi_card("MAE",           f"${mae:.2f}"),            md=4),
    ], className="g-3 mb-3")

    return dbc.Container([
        html.H4("Price Prediction Model (Linear Regression)", className="mt-3 mb-1 text-white"),
        html.Hr(style={"borderColor": "#343a56"}),
        kpis,
        dbc.Row([
            dbc.Col(_graph("pr-avp",     pr.actual_vs_predicted_price(df_preds)),  md=6),
            dbc.Col(_graph("pr-resid",   pr.price_residual_histogram(df_preds)),   md=6),
        ], className="g-3"),
        dbc.Row([
            dbc.Col(_graph("pr-coeff",   pr.price_coefficients_bar(df_coeff)),     md=6),
            dbc.Col(_graph("pr-scatter", pr.price_error_scatter(df_preds)),        md=6),
        ], className="g-3"),
    ], fluid=True)


# TAB: RATING PREDICTION
def build_rating_tab() -> dbc.Container:
    df_preds = load_rating_predictions()
    df_coeff = load_rating_coefficients()

    mse  = (df_preds["residual"] ** 2).mean()
    rmse = mse ** 0.5
    mae  = df_preds["residual"].abs().mean()

    kpis = dbc.Row([
        dbc.Col(_kpi_card("Test Samples",  str(len(df_preds))),         md=4),
        dbc.Col(_kpi_card("RMSE",          f"{rmse:.4f}"),              md=4),
        dbc.Col(_kpi_card("MAE",           f"{mae:.4f}"),               md=4),
    ], className="g-3 mb-3")

    return dbc.Container([
        html.H4("Rating Prediction Model (Linear Regression)", className="mt-3 mb-1 text-white"),
        html.Hr(style={"borderColor": "#343a56"}),
        kpis,
        dbc.Row([
            dbc.Col(_graph("ra-avp",     ra.actual_vs_predicted_rating(df_preds)),  md=6),
            dbc.Col(_graph("ra-resid",   ra.rating_residual_histogram(df_preds)),   md=6),
        ], className="g-3"),
        dbc.Row([
            dbc.Col(_graph("ra-coeff",   ra.rating_coefficients_bar(df_coeff)),     md=6),
            dbc.Col(_graph("ra-scatter", ra.rating_residual_vs_actual(df_preds)),   md=6),
        ], className="g-3"),
    ], fluid=True)


# LAYOUT
_NAV_STYLE = {
    "backgroundColor": "#0d0f1a",
    "borderBottom": "1px solid #343a56",
    "padding": "0 24px",
}

_CONTENT_STYLE = {
    "backgroundColor": "#10121e",
    "minHeight": "100vh",
    "padding": "0 0 48px 0",
}

app.layout = html.Div(
    style={"backgroundColor": "#10121e", "fontFamily": "Inter, sans-serif"},
    children=[
        # ── Header ──────────────────────────────────────────
        html.Div(
            style=_NAV_STYLE,
            children=[
                html.H3(
                    "🛒 E-Commerce Pipeline Dashboard",
                    style={"color": "#e9ecef", "margin": "0", "padding": "16px 0 4px 0"},
                ),
                html.P(
                    "Live view of ML outputs — refresh the page after a new pipeline run.",
                    style={"color": "#6c757d", "margin": "0 0 12px 0", "fontSize": "0.85rem"},
                ),
            ],
        ),

        # ── Tabs ────────────────────────────────────────────
        dbc.Tabs(
            id="main-tabs",
            active_tab="tab-overview",
            style={"backgroundColor": "#0d0f1a", "paddingLeft": "24px"},
            children=[
                dbc.Tab(label="📊 Overview",      tab_id="tab-overview"),
                dbc.Tab(label="🔵 Clusters",      tab_id="tab-clusters"),
                dbc.Tab(label="💰 Price Model",   tab_id="tab-price"),
                dbc.Tab(label="⭐ Rating Model",  tab_id="tab-rating"),
            ],
        ),

        # ── Tab content (rendered on selection) ─────────────
        html.Div(id="tab-content", style=_CONTENT_STYLE),

        # ── Interval: auto-refresh every 5 min ──────────────
        dcc.Interval(id="refresh-interval", interval=5 * 60 * 1000, n_intervals=0),
    ],
)


# CALLBACKS
@app.callback(
    Output("tab-content", "children"),
    Input("main-tabs", "active_tab"),
    Input("refresh-interval", "n_intervals"),
)
def render_tab(active_tab: str, _n: int):
    """
    Rebuild the tab content every time the user switches tabs OR
    the 5-minute refresh interval fires.  Because all build_*_tab()
    functions call load_*() which reads from disk, the charts always
    reflect the latest CSV outputs from the pipeline.
    """
    if active_tab == "tab-overview":
        return build_overview_tab()
    if active_tab == "tab-clusters":
        return build_cluster_tab()
    if active_tab == "tab-price":
        return build_price_tab()
    if active_tab == "tab-rating":
        return build_rating_tab()
    return html.P("Select a tab above.", className="text-muted p-4")


# ENTRY POINT
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)