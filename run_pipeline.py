import logging
from ingestion.api_client import fetch_all
from storage.storage import save_all, load_all
from transformation.transformation import transform_all
from loading.loader import load_all
from loading.db_engine import dispose_engine
from ml.ml import run_ml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    try:
        # ── Step 1: Ingestion ──────────────────────────────────────
        raw_data = fetch_all()
        save_all(raw_data)

        # ── Step 2: Transformation ─────────────────────────────────
        transformed_data = transform_all(raw_data)

        # ── Step 3: Loading ───────────────────────────────────────
        load_all(
            products_df=transformed_data["products"],
            users_df=transformed_data["users"],
            carts_df=transformed_data["carts"],
            cart_items_df=transformed_data["cart_items"]
        )

        # ── Step 4: Machine Learning ──────────────────────────────
        ml_results = run_ml()

    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}")
        raise
    
    finally:
        dispose_engine()
        logger.info("PIPELINE RUN COMPLETE.")
