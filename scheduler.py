import logging
import schedule
import time
import os

from ingestion.api_client import fetch_all
from storage.storage import save_all, load_all
from transformation.transformation import transform_all
from loading.loader import load_all
from loading.db_engine import dispose_engine
from ml.ml import run_ml



logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger(__name__)


def pipeline():
    logger.info("="*50)
    logger.info("STARTING PIPELINE...")
    logger.info("="*50)

    try:
        # INGESTION ────────────────────────────────────────
        raw_data = fetch_all()
        save_all(raw_data)
    
        # TRANSFORMATION ─────────────────────────────────
        transformed_data = transform_all(raw_data)
    
        # LOADING ───────────────────────────────────────
        load_all(
            products_df=transformed_data["products"],
            users_df=transformed_data["users"],
            carts_df=transformed_data["carts"],
            cart_items_df=transformed_data["cart_items"]
        )

        # MACHINE LEARNING ──────────────────────────────

        # VISUALIZATION ────────────────────────────────

    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}")
        raise

    finally:
        dispose_engine()
        logger.info("PIPELINE RUN COMPLETE.")


if __name__ == "__main__":
    schedule.every(2).minutes.do(pipeline)

    # logger.info("SCHEDULER STARTED. PIPELINE WILL RUN DAILY AT 11:00 AM.")
    logger.info("SCHEDULER STARTED. PIPELINE WILL RUN EVERY 2 MINUTES.")

    while True:
        schedule.run_pending()
        time.sleep(60)