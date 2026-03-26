import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

_TEMPLATE = "plotly_dark"
_CLUSTER_COLORS = px.colors.qualitative.Bold


def cluster_scatter(df: pd.DataFrame) -> go.Figure:
    """
    Price vs Rating scatter coloured by cluster.
    Bubble size = stock level.
    """
    fig = px.scatter(
        df.dropna(subset=["cluster"]),
        x="price", y="rating",
        color="cluster",
        size="stock",
        size_max=25,
        hover_data=["title", "brand", "category", "discount_percentage"],
        title="Product Clusters — Price vs Rating",
        labels={"price": "Price ($)", "rating": "Rating", "cluster": "Cluster"},
        color_discrete_sequence=_CLUSTER_COLORS,
        template=_TEMPLATE,
        opacity=0.80,
    )
    fig.update_layout(legend_title_text="Cluster")
    return fig


def cluster_distribution_bar(df: pd.DataFrame) -> go.Figure:
    """Bar chart: number of products per cluster."""
    counts = (
        df.dropna(subset=["cluster"])
        .groupby("cluster")
        .size()
        .reset_index(name="count")
        .sort_values("cluster")
    )
    fig = px.bar(
        counts, x="cluster", y="count",
        title="Product Count per Cluster",
        labels={"cluster": "Cluster", "count": "Number of Products"},
        color="cluster",
        color_discrete_sequence=_CLUSTER_COLORS,
        template=_TEMPLATE,
        text="count",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False)
    return fig


def cluster_price_box(df: pd.DataFrame) -> go.Figure:
    """Box plot: price distribution within each cluster."""
    fig = px.box(
        df.dropna(subset=["cluster"]),
        x="cluster", y="price",
        color="cluster",
        title="Price Distribution per Cluster",
        labels={"cluster": "Cluster", "price": "Price ($)"},
        color_discrete_sequence=_CLUSTER_COLORS,
        template=_TEMPLATE,
        points="outliers",
    )
    fig.update_layout(showlegend=False)
    return fig


def cluster_rating_box(df: pd.DataFrame) -> go.Figure:
    """Box plot: rating distribution within each cluster."""
    fig = px.box(
        df.dropna(subset=["cluster"]),
        x="cluster", y="rating",
        color="cluster",
        title="Rating Distribution per Cluster",
        labels={"cluster": "Cluster", "rating": "Rating"},
        color_discrete_sequence=_CLUSTER_COLORS,
        template=_TEMPLATE,
        points="outliers",
    )
    fig.update_layout(showlegend=False)
    return fig


def cluster_profile_heatmap(df: pd.DataFrame) -> go.Figure:
    """
    Heatmap of mean feature values per cluster (z-scored for comparability).
    """
    features = ["price", "rating", "discount_percentage", "stock"]
    df_clean = df.dropna(subset=["cluster"] + features)

    profile = df_clean.groupby("cluster")[features].mean().round(2)

    # Z-score across clusters so all features share the same colour scale
    z_scored = (profile - profile.mean()) / (profile.std() + 1e-9)

    fig = go.Figure(go.Heatmap(
        z=z_scored.values,
        x=features,
        y=[f"Cluster {c}" for c in z_scored.index],
        colorscale="RdBu_r",
        zmid=0,
        text=profile.values,
        texttemplate="%{text:.1f}",
        hovertemplate="Feature: %{x}<br>Cluster: %{y}<br>Raw mean: %{text}<extra></extra>",
        colorbar=dict(title="Z-score"),
    ))
    fig.update_layout(
        title="Cluster Profiles (Raw Means, Z-scored Colour Scale)",
        xaxis_title="Feature",
        template=_TEMPLATE,
    )
    return fig