import logging
import pandas as pd
from sqlalchemy import text
import os

from loading.db_engine import get_engine
from loading.upsert import upsert_dataframe

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


PRODUCTS_COLUMNS = [
    "id", 
    "title", 
    "description", 
    "category", 
    "price", 
    "discount_percentage", 
    "rating", 
    "stock", 
    "brand", 
    "sku", 
    "availability_status", 
    "minimum_order_quantity"
]

USERS_COLUMNS = [
    "id",
    "first_name",
    "last_name",
    "maiden_name",
    "age",
    "gender",
    "email",
    "phone",
    "image",
    "city",
    "state",
    "country"
]

CARTS_COLUMNS = [
    "id",
    "user_id",
    "total",
    "discounted_total",
    "total_products",
    "total_quantity"
]

CART_ITEMS_COLUMNS = [
    "cart_id",
    "product_id",
    "title",
    "price",
    "quantity",
    "total",
    "discounted_total",
    "discounted_percentage"
]


def _validate(df: pd.DataFrame, required_columns: list, table_name: str, pk: str):
    if df.empty:
        raise ValueError(f"{table_name.upper()} DATAFRAME IS EMPTY.")

    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(
            f"{table_name.upper()} DATAFRAME IS MISSING REQUIRED COLUMNS: {missing}. "
            f"CHECK TRANSFORMATION LAYER OUTPUT FOR {table_name.upper()}."
        )

    if pk and pk in df.columns:
        null_pks = df[pk].isnull().sum()
        if null_pks > 0:
            raise ValueError(
                f"{table_name.upper()} DATAFRAME HAS {null_pks} NULL VALUES IN PRIMARY KEY COLUMN '{pk}'. "
                f"DEDUPLICATE IN TRANSFORMATION LAYER."
            )

    logger.info(
        f"[{table_name.upper()}] VALIDATION PASSED: {len(df)} RECORDS. "
        f"{len(df.columns)} COLUMNS"
    )


def load_products(df: pd.DataFrame):
    table    = "raw.products"
    pk       = "id"

    logger.info(f"STARTING LOAD FOR '{table}'...")
    df = df[[c for c in PRODUCTS_COLUMNS if c in df.columns]].copy()
    _validate(df, PRODUCTS_COLUMNS, table, pk)

    engine = get_engine()
    upsert_dataframe(df, table, pk, engine)
    logger.info(f"'{table.upper()}' LOAD COMPLETE.\n")


def load_users(df: pd.DataFrame):
    table    = "raw.users"
    pk       = "id"

    logger.info(f"STARTING LOAD FOR '{table}'...")
    df = df[[c for c in USERS_COLUMNS if c in df.columns]].copy()
    _validate(df, USERS_COLUMNS, table, pk)

    engine = get_engine()
    upsert_dataframe(df, table, pk, engine)
    logger.info(f"'{table.upper()}' LOAD COMPLETE.\n")


def load_carts(df: pd.DataFrame):
    table    = "raw.carts"
    pk       = "id"

    logger.info(f"STARTING LOAD FOR '{table}'...")
    df = df[[c for c in CARTS_COLUMNS if c in df.columns]].copy()
    _validate(df, CARTS_COLUMNS, table, pk)

    engine = get_engine()
    upsert_dataframe(df, table, pk, engine)
    logger.info(f"'{table.upper()}' LOAD COMPLETE.\n")


def load_cart_items(df: pd.DataFrame):
    table   = "raw.cart_items"
    base_name = table.split(".")[-1]
    staging = f"{base_name}_staging"

    logger.info(f"STARTING LOAD FOR '{table}'...")
    df = df[[c for c in CART_ITEMS_COLUMNS if c in df.columns]].copy()

    if df.empty:
        raise ValueError(f"[{table.upper()}] DATAFRAME IS EMPTY — NOTHING TO LOAD.")

    missing = set(CART_ITEMS_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"[{table.upper()}] MISSING COLUMNS: {missing}")

    logger.info(f"[{table.upper()}] VALIDATION PASSED: {len(df)} ROWS.")

    engine = get_engine()

    with engine.begin() as conn:
        df.to_sql(staging, conn, if_exists="replace", index=False)

        update_cols = [c for c in CART_ITEMS_COLUMNS if c not in ("cart_id", "product_id")]
        set_clause  = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

        conn.execute(text(f"""
            INSERT INTO {table} ({', '.join(CART_ITEMS_COLUMNS)})
            SELECT {', '.join(CART_ITEMS_COLUMNS)} FROM {staging}
            ON CONFLICT (cart_id, product_id) DO UPDATE
            SET {set_clause};
        """))

        conn.execute(text(f"DROP TABLE IF EXISTS {staging};"))

    logger.info(f"'{table.upper()}' LOAD COMPLETE.\n")


def load_all(
    products_df:   pd.DataFrame,
    users_df:      pd.DataFrame,
    carts_df:      pd.DataFrame,
    cart_items_df: pd.DataFrame,
):
    logger.info("=" * 50)
    logger.info("LOADING LAYER: STARTING FULL LOAD...")
    logger.info("=" * 50)

    load_users(users_df)
    load_products(products_df)
    load_carts(carts_df)
    load_cart_items(cart_items_df)

    logger.info("=" * 50)
    logger.info("LOADING LAYER: ALL TABLES LOADED SUCCESSFULLY.")
    logger.info("=" * 50)