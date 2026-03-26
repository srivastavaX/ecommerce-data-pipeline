import os
import pandas as pd

_BASE = os.path.join(os.path.dirname(__file__), "..", "ml", "outputs")


def _path(filename: str) -> str:
    return os.path.join(_BASE, filename)


def load_products_enriched() -> pd.DataFrame:
    df = pd.read_csv(_path("products_enriched.csv"))
    df["cluster"] = df["cluster"].astype("Int64").astype(str)   # treat as category for colour
    df["price_band"]  = pd.Categorical(df["price_band"],  categories=["budget", "mid", "premium", "luxury"], ordered=True)
    df["rating_band"] = pd.Categorical(df["rating_band"], categories=["low", "average", "good", "excellent"], ordered=True)
    return df


def load_price_predictions() -> pd.DataFrame:
    """Columns: actual_price, predicted_price, residual"""
    return pd.read_csv(_path("price_predictions.csv"))


def load_price_coefficients() -> pd.DataFrame:
    """Columns: feature, coefficient"""
    return pd.read_csv(_path("price_coefficients.csv"))


def load_rating_predictions() -> pd.DataFrame:
    """Columns: actual_rating, predicted_rating, residual"""
    return pd.read_csv(_path("rating_predictions.csv"))


def load_rating_coefficients() -> pd.DataFrame:
    """Columns: feature, coefficient"""
    return pd.read_csv(_path("rating_coefficients.csv"))