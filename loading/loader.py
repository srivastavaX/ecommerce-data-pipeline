import logging
import pandas as pd
from sqlalchemy import text

from loading.db_engine import get_engine
from loading.upsert import upsert_dataframe

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
    "discounted_price",
    "discount_percentage"
]


def _validate(df: pd.DataFrame, required_columns: list, table_name: str, pk: str):
    if df.empty:
        raise ValueError(f"{table_name.upper()} DATAFRAME IS EMPTY.")

    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(
            f"{table_name.upper()} DATAFRAME IS MISSING REQUIRED COLUMNS: {missing}"
            f"CHECK TRANSFORMATION LAYER OUTPUT FOR {table_name.upper()}."
        )
    
    if pk and pk in df.columns:
        null_pks = df[pk].isnull().sum()
        if null_pks > 0:
            raise ValueError(
                f"{table_name.upper()} DATAFRAME HAS {null_pks} NULL VALUES IN PRIMARY KEY COLUMN '{pk}'."
                f"CHECK TRANSFORMATION LAYER OUTPUT FOR {table_name.upper()}."
                f"DEDUPLICATE IN TRANSFORMATION LAYER"
            )
        
    logger.info(
        f"[{table_name.upper()}] VALIDATION PASSED: {len(df)} RECORDS."
        f"{len(df.columns)} COLUMNS"
    )


# INDIVIDUAL TABLE LOADERS

def load_products(df: pd.DataFrame):
    table = "products"
    pk = "id"

    logger.info(f"STARTING LOAD FOR '{table}'...")
    df = df[[c for c in PRODUCTS_COLUMNS if c in df.columns]].copy()
    _validate(df, PRODUCTS_COLUMNS, table, pk)

    engine = get_engine()
    upsert_dataframe(df, table, pk, engine)
    logger.info(f"'{table.upper()}' LOAD COMPLETE.\n")


def load_users(df: pd.DataFrame):
    table = "users"
    pk = "id"

    logger.info(f"STARTING LOAD FOR '{table.upper()}'...")
    df = df[[c for c in USERS_COLUMNS if c in df.columns]].copy()
    _validate(df, USERS_COLUMNS, table, pk)

    engine = get_engine()
    upsert_dataframe(df, table, pk, engine)
    logger.info(f"'{table.upper()}' LOAD COMPLETe.\n")


def load_carts(df: pd.DataFrame):
    table = "carts"
    pk = "id"

    logger.info(f"STARTING LOAD FOR '{table.upper()}'...")
    df = df[[c for c in CARTS_COLUMNS if c in df.columns]].copy()
    _validate(df, CARTS_COLUMNS, table, pk)

    engine = get_engine()
    upsert_dataframe(df, table, pk, engine)
    logger.info(f"'{table.upper()}' LOAD COMPLETE.\n")


def load_cart_items(df: pd.DataFrame):
    """
    Load flattened cart items into the `cart_items` table.
    Note: cart_items has a composite PK (cart_id, product_id).
    """
    table = "cart_items"
    logger.info(f"STARTING LOAD FOR '{table.upper()}'...")

    df = df[[c for c in CART_ITEMS_COLUMNS if c in df.columns]].copy()

    if df.empty:
        raise ValueError(f"[{table.upper()}] DATAfRAME IS EMPTY — NOTHING TO LOAD.")

    missing = set(CART_ITEMS_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"[{table.upper()}] MISSING COLUMNS: {missing}")

    logger.info(f"[{table.upper()}] VALIDATION PASSED: {len(df)} ROWS.")

    engine = get_engine()
    staging = f"{table}_staging"

    with engine.begin() as conn:
        # Load to staging
        df.to_sql(staging, conn, if_exists="replace", index=False)

        # Upsert with composite PK
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


# ─── Orchestrator ─────────────────────────────────────────────────────────────

def load_all(
    products_df:   pd.DataFrame,
    users_df:      pd.DataFrame,
    carts_df:      pd.DataFrame,
    cart_items_df: pd.DataFrame,
):
    """
    Runs all table loaders in dependency order:
      users → products → carts → cart_items
    """
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