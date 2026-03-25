import logging
from ingestion import fetch_all
from storage import save_all, load_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":

    # ── Step 1: Ingestion ──────────────────────────────────────
    raw_data = fetch_all()
    save_all(raw_data)

    logger.info("Pipeline run complete.")