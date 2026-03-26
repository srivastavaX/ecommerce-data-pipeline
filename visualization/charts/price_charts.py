import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

_TEMPLATE = "plotly_dark"


def actual_vs_predicted_price(df_preds: pd.DataFrame) -> go.Figure:
    """
    Scatter: actual price vs predicted price.
    Perfect predictions lie on the diagonal.
    """
    lim_min = min(df_preds["actual_price"].min(), df_preds["predicted_price"].min()) * 0.95
    lim_max = max(df_preds["actual_price"].max(), df_preds["predicted_price"].max()) * 1.05

    fig = go.Figure()

    # Perfect-prediction reference line
    fig.add_trace(go.Scatter(
        x=[lim_min, lim_max], y=[lim_min, lim_max],
        mode="lines",
        line=dict(color="rgba(255,255,255,0.35)", dash="dash", width=1),
        name="Perfect Prediction",
        hoverinfo="skip",
    ))

    # Scatter points
    fig.add_trace(go.Scatter(
        x=df_preds["actual_price"],
        y=df_preds["predicted_price"],
        mode="markers",
        marker=dict(
            color=df_preds["residual"].abs(),
            colorscale="Plasma",
            colorbar=dict(title="|Residual|"),
            size=8,
            opacity=0.75,
        ),
        name="Products",
        hovertemplate=(
            "Actual: $%{x:.2f}<br>"
            "Predicted: $%{y:.2f}<br>"
            "Error: $%{customdata:.2f}<extra></extra>"
        ),
        customdata=df_preds["residual"],
    ))

    fig.update_layout(
        title="Actual vs Predicted Price",
        xaxis_title="Actual Price ($)",
        yaxis_title="Predicted Price ($)",
        template=_TEMPLATE,
    )
    return fig


def price_residual_histogram(df_preds: pd.DataFrame) -> go.Figure:
    """Histogram of prediction residuals (actual − predicted)."""
    fig = px.histogram(
        df_preds, x="residual",
        nbins=30,
        title="Price Prediction — Residual Distribution",
        labels={"residual": "Residual (Actual − Predicted, $)"},
        color_discrete_sequence=["#7B61FF"],
        template=_TEMPLATE,
        marginal="box",
        opacity=0.85,
    )
    fig.add_vline(x=0, line_dash="dash", line_color="white", opacity=0.5)
    return fig


def price_coefficients_bar(df_coeff: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar of LinearRegression coefficients.
    Positive = pushes price up, negative = pulls price down.
    """
    df_sorted = df_coeff.sort_values("coefficient")
    colors = ["#FF6B6B" if c < 0 else "#51CF66" for c in df_sorted["coefficient"]]

    fig = go.Figure(go.Bar(
        x=df_sorted["coefficient"],
        y=df_sorted["feature"],
        orientation="h",
        marker_color=colors,
        text=df_sorted["coefficient"].round(2),
        textposition="outside",
        hovertemplate="Feature: %{y}<br>Coefficient: %{x:.4f}<extra></extra>",
    ))
    fig.add_vline(x=0, line_color="white", opacity=0.4)
    fig.update_layout(
        title="Price Model — Feature Coefficients",
        xaxis_title="Coefficient",
        yaxis_title="",
        template=_TEMPLATE,
    )
    return fig


def price_error_scatter(df_preds: pd.DataFrame) -> go.Figure:
    """Residual vs Actual price — checks for heteroscedasticity."""
    fig = px.scatter(
        df_preds, x="actual_price", y="residual",
        title="Residuals vs Actual Price",
        labels={"actual_price": "Actual Price ($)", "residual": "Residual ($)"},
        color="residual",
        color_continuous_scale="RdBu_r",
        color_continuous_midpoint=0,
        template=_TEMPLATE,
        opacity=0.75,
    )
    fig.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.5)
    fig.update_coloraxes(showscale=False)
    return fig