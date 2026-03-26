import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

_TEMPLATE = "plotly_dark"
_COLORS   = px.colors.qualitative.Vivid


def price_distribution(df: pd.DataFrame) -> go.Figure:
    """Histogram of product prices with KDE-style rug."""
    fig = px.histogram(
        df, x="price", nbins=40,
        title="Price Distribution",
        labels={"price": "Price ($)"},
        color_discrete_sequence=[_COLORS[0]],
        template=_TEMPLATE,
        marginal="rug",
        opacity=0.85,
    )
    fig.update_layout(bargap=0.05)
    return fig


def avg_price_by_category(df: pd.DataFrame) -> go.Figure:
    """Horizontal bar – average price per category, sorted descending."""
    agg = (
        df.groupby("category")["price"]
        .mean()
        .round(2)
        .reset_index()
        .sort_values("price", ascending=True)
    )
    fig = px.bar(
        agg, x="price", y="category",
        orientation="h",
        title="Average Price by Category",
        labels={"price": "Avg Price ($)", "category": ""},
        color="price",
        color_continuous_scale="Tealrose",
        template=_TEMPLATE,
        text="price",
    )
    fig.update_traces(texttemplate="$%{text:.0f}", textposition="outside")
    fig.update_coloraxes(showscale=False)
    return fig


def avg_rating_by_category(df: pd.DataFrame) -> go.Figure:
    """Bar chart – average rating per category."""
    agg = (
        df.groupby("category")["rating"]
        .mean()
        .round(2)
        .reset_index()
        .sort_values("rating", ascending=True)
    )
    fig = px.bar(
        agg, x="rating", y="category",
        orientation="h",
        title="Average Rating by Category",
        labels={"rating": "Avg Rating", "category": ""},
        color="rating",
        color_continuous_scale="Teal",
        range_color=[3.5, 5.0],
        template=_TEMPLATE,
        text="rating",
    )
    fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
    fig.update_coloraxes(showscale=False)
    return fig


def price_band_donut(df: pd.DataFrame) -> go.Figure:
    """Donut chart showing product count in each price band."""
    counts = df["price_band"].value_counts().reset_index()
    counts.columns = ["price_band", "count"]
    fig = px.pie(
        counts, names="price_band", values="count",
        title="Products by Price Band",
        hole=0.45,
        color_discrete_sequence=_COLORS,
        template=_TEMPLATE,
    )
    fig.update_traces(textinfo="label+percent", pull=[0.03] * 4)
    return fig


def rating_band_donut(df: pd.DataFrame) -> go.Figure:
    """Donut chart showing product count in each rating band."""
    counts = df["rating_band"].value_counts().reset_index()
    counts.columns = ["rating_band", "count"]
    fig = px.pie(
        counts, names="rating_band", values="count",
        title="Products by Rating Band",
        hole=0.45,
        color_discrete_sequence=px.colors.qualitative.Pastel,
        template=_TEMPLATE,
    )
    fig.update_traces(textinfo="label+percent", pull=[0.03] * 4)
    return fig


def discount_vs_price_scatter(df: pd.DataFrame) -> go.Figure:
    """Scatter: discount % vs price, coloured by category."""
    fig = px.scatter(
        df, x="discount_percentage", y="price",
        color="category",
        hover_data=["title", "brand", "rating"],
        title="Discount % vs Price (by Category)",
        labels={"discount_percentage": "Discount (%)", "price": "Price ($)"},
        template=_TEMPLATE,
        opacity=0.75,
    )
    return fig