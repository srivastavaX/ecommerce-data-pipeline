import logging
import os
import pandas as pd
from sqlalchemy import text

from sklearn.linear_model import LinearRegression
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

from loading.db_engine import get_engine

logger = logging.getLogger(__name__)

# ML_OUTPUTS_DIR = os.path.join(os.path.dirname(__file__), "ml", "outputs")
ML_OUTPUTS_DIR = "ml/outputs"


# ─────────────────────────────────────────
# Load Data from PostgreSQL
# ─────────────────────────────────────────

def load_products_from_db() -> pd.DataFrame:
    logger.info("Loading products from PostgreSQL...")
    engine = get_engine()

    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM raw.products"), conn)

    logger.info(f"Loaded {len(df)} products from DB.")
    return df


# Feature Engineering
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Engineering features...")

    df = df.copy()

    df["discount_amount"]  = (df["price"] * df["discount_percentage"] / 100).round(2)
    df["effective_price"]  = (df["price"] - df["discount_amount"]).round(2)

    df["price_band"] = pd.cut(
        df["price"],
        bins=[0, 50, 200, 500, float("inf")],
        labels=["budget", "mid", "premium", "luxury"]
    )
    df["rating_band"] = pd.cut(
        df["rating"],
        bins=[0, 3, 4, 4.5, float("inf")],
        labels=["low", "average", "good", "excellent"]
    )

    df["category_encoded"] = df["category"].astype("category").cat.codes
    df["brand_encoded"]    = df["brand"].fillna("unknown").astype("category").cat.codes

    logger.info("Feature engineering complete.")
    return df


# Price Prediction
def predict_price(df: pd.DataFrame):
    logger.info("Training price prediction model...")

    features = ["discount_percentage", "rating", "stock", "category_encoded", "brand_encoded"]
    target   = "price"

    df_model = df[features + [target]].dropna()

    X = df_model[features]
    y = df_model[target]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mse    = mean_squared_error(y_test, y_pred)
    r2     = r2_score(y_test, y_pred)

    logger.info(f"Price Prediction | MSE: {mse:.2f} | R2: {r2:.4f}")

    df_preds = pd.DataFrame({
        "actual_price":    y_test.values,
        "predicted_price": y_pred.round(2),
    })
    df_preds["residual"] = (df_preds["actual_price"] - df_preds["predicted_price"]).round(2)

    df_coeff = pd.DataFrame({
        "feature":     features,
        "coefficient": model.coef_.round(4),
    }).sort_values("coefficient", ascending=False)

    return model, df_preds, df_coeff


# Rating Prediction
def predict_rating(df: pd.DataFrame):
    logger.info("Training rating prediction model...")

    features = ["price", "discount_percentage", "stock", "category_encoded", "brand_encoded"]
    target   = "rating"

    df_model = df[features + [target]].dropna()

    X = df_model[features]
    y = df_model[target]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mse    = mean_squared_error(y_test, y_pred)
    r2     = r2_score(y_test, y_pred)

    logger.info(f"Rating Prediction | MSE: {mse:.4f} | R2: {r2:.4f}")

    # Build predictions DataFrame for visualization
    df_preds = pd.DataFrame({
        "actual_rating":    y_test.values,
        "predicted_rating": y_pred.round(4),
    })
    df_preds["residual"] = (df_preds["actual_rating"] - df_preds["predicted_rating"]).round(4)

    # Feature coefficients for visualization
    df_coeff = pd.DataFrame({
        "feature":     features,
        "coefficient": model.coef_.round(6),
    }).sort_values("coefficient", ascending=False)

    return model, df_preds, df_coeff


# ─────────────────────────────────────────
# Product Clustering
# ─────────────────────────────────────────

def cluster_products(df: pd.DataFrame):
    logger.info("Training product clustering model...")

    features = ["price", "rating", "discount_percentage", "stock"]

    df_model = df[features].dropna()

    scaler   = StandardScaler()
    X_scaled = scaler.fit_transform(df_model)

    model = KMeans(n_clusters=4, random_state=42, n_init=10)
    model.fit(X_scaled)

    df.loc[df_model.index, "cluster"] = model.labels_
    df["cluster"] = df["cluster"].astype("Int64")

    logger.info(f"Clustering complete. Cluster distribution:\n{df['cluster'].value_counts().to_string()}")

    return model, scaler, df


# ─────────────────────────────────────────
# Save All ML Outputs to Disk
# ─────────────────────────────────────────

def save_outputs(df: pd.DataFrame, df_price_preds: pd.DataFrame, df_price_coeff: pd.DataFrame,
                 df_rating_preds: pd.DataFrame, df_rating_coeff: pd.DataFrame) -> None:
    os.makedirs(ML_OUTPUTS_DIR, exist_ok=True)

    # Columns to keep for the enriched product table
    product_cols = [
        "id", "title", "category", "brand", "price", "effective_price",
        "discount_percentage", "discount_amount", "stock",
        "rating", "price_band", "rating_band",
        "category_encoded", "brand_encoded", "cluster",
    ]
    existing_cols = [c for c in product_cols if c in df.columns]
    df[existing_cols].to_csv(os.path.join(ML_OUTPUTS_DIR, "products_enriched.csv"), index=False)

    df_price_preds.to_csv(os.path.join(ML_OUTPUTS_DIR, "price_predictions.csv"), index=False)
    df_price_coeff.to_csv(os.path.join(ML_OUTPUTS_DIR, "price_coefficients.csv"), index=False)

    df_rating_preds.to_csv(os.path.join(ML_OUTPUTS_DIR, "rating_predictions.csv"), index=False)
    df_rating_coeff.to_csv(os.path.join(ML_OUTPUTS_DIR, "rating_coefficients.csv"), index=False)

    logger.info(f"ML outputs saved to {ML_OUTPUTS_DIR}")


# ─────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────

def run_ml():
    logger.info("=" * 50)
    logger.info("ML LAYER: STARTING...")
    logger.info("=" * 50)

    df = load_products_from_db()
    df = engineer_features(df)

    price_model,  df_price_preds,  df_price_coeff  = predict_price(df)
    rating_model, df_rating_preds, df_rating_coeff = predict_rating(df)
    cluster_model, scaler, df = cluster_products(df)

    # ── Save all outputs for the visualization layer ──
    save_outputs(df, df_price_preds, df_price_coeff, df_rating_preds, df_rating_coeff)

    logger.info("=" * 50)
    logger.info("ML LAYER: COMPLETE.")
    logger.info("=" * 50)

    return {
        "df":            df,
        "price_model":   price_model,
        "rating_model":  rating_model,
        "cluster_model": cluster_model,
        "scaler":        scaler,
    }
