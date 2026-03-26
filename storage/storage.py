import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

RAW_DATA_DIR = Path("ingestion/raw_data")


def save_raw(name, data):
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    filepath = RAW_DATA_DIR / f"{name}_raw.json"

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Saved {len(data)} {name} records → {filepath}")


def load_raw(name):
    filepath = RAW_DATA_DIR / f"{name}_raw.json"

    if not filepath.exists():
        raise FileNotFoundError(
            f"{filepath} not found. Run ingestion first."
        )

    with open(filepath, "r") as f:
        data = json.load(f)

    logger.info(f"Loaded {len(data)} {name} records from {filepath}")
    return data


def save_all(raw_data):
    for name, records in raw_data.items():
        save_raw(name, records)
    logger.info("All raw data saved to disk.")


def load_all():
    return {
        "products": load_raw("products"),
        "users":    load_raw("users"),
        "carts":    load_raw("carts"),
    }