import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

_TEMPLATE = "plotly_dark"


def actual_vs_predicted_rating(df_preds: pd.DataFrame) -> go.Figure:
    """
    Scatter: actual rating vs predicted rating.
    Perfect predictions lie on the diagonal.
    """
    lim_min = min(df_preds["actual_rating"].min(), df_preds["predicted_rating"].min()) * 0.97
    lim_max = max(df_preds["actual_rating"].max(), df_preds["predicted_rating"].max()) * 1.03

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=[lim_min, lim_max], y=[lim_min, lim_max],
        mode="lines",
        line=dict(color="rgba(255,255,255,0.35)", dash="dash", width=1),
        name="Perfect Prediction",
        hoverinfo="skip",
    ))

    fig.add_trace(go.Scatter(
        x=df_preds["actual_rating"],
        y=df_preds["predicted_rating"],
        mode="markers",
        marker=dict(
            color=df_preds["residual"].abs(),
            colorscale="Turbo",
            colorbar=dict(title="|Residual|"),
            size=8,
            opacity=0.75,
        ),
        name="Products",
        hovertemplate=(
            "Actual: %{x:.3f}<br>"
            "Predicted: %{y:.3f}<br>"
            "Error: %{customdata:.4f}<extra></extra>"
        ),
        customdata=df_preds["residual"],
    ))

    fig.update_layout(
        title="Actual vs Predicted Rating",
        xaxis_title="Actual Rating",
        yaxis_title="Predicted Rating",
        template=_TEMPLATE,
    )
    return fig


def rating_residual_histogram(df_preds: pd.DataFrame) -> go.Figure:
    """Histogram of rating prediction residuals."""
    fig = px.histogram(
        df_preds, x="residual",
        nbins=30,
        title="Rating Prediction — Residual Distribution",
        labels={"residual": "Residual (Actual − Predicted)"},
        color_discrete_sequence=["#20C997"],
        template=_TEMPLATE,
        marginal="box",
        opacity=0.85,
    )
    fig.add_vline(x=0, line_dash="dash", line_color="white", opacity=0.5)
    return fig


def rating_coefficients_bar(df_coeff: pd.DataFrame) -> go.Figure:
    """Horizontal bar of LinearRegression coefficients for rating model."""
    df_sorted = df_coeff.sort_values("coefficient")
    colors = ["#FF6B6B" if c < 0 else "#51CF66" for c in df_sorted["coefficient"]]

    fig = go.Figure(go.Bar(
        x=df_sorted["coefficient"],
        y=df_sorted["feature"],
        orientation="h",
        marker_color=colors,
        text=df_sorted["coefficient"].round(6),
        textposition="outside",
        hovertemplate="Feature: %{y}<br>Coefficient: %{x:.6f}<extra></extra>",
    ))
    fig.add_vline(x=0, line_color="white", opacity=0.4)
    fig.update_layout(
        title="Rating Model — Feature Coefficients",
        xaxis_title="Coefficient",
        yaxis_title="",
        template=_TEMPLATE,
    )
    return fig


def rating_residual_vs_actual(df_preds: pd.DataFrame) -> go.Figure:
    """Residuals vs actual rating — checks for systematic bias."""
    fig = px.scatter(
        df_preds, x="actual_rating", y="residual",
        title="Residuals vs Actual Rating",
        labels={"actual_rating": "Actual Rating", "residual": "Residual"},
        color="residual",
        color_continuous_scale="RdBu_r",
        color_continuous_midpoint=0,
        template=_TEMPLATE,
        opacity=0.75,
    )
    fig.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.5)
    fig.update_coloraxes(showscale=False)
    return fig