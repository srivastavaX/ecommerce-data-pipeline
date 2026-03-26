import logging
import os
from sqlalchemy import text

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def upsert_dataframe(df, table_name, pk_column, engine):
    if df.empty:
        logger.info("DATAFRAME IS EMPTY. NO RECORDS TO UPSERT.")
        return

    base_name     = table_name.split(".")[-1]  # "raw.users" → "users"
    staging_table = f"{base_name}_staging"     # → "users_staging" (in public schema)

    columns        = df.columns.tolist()
    update_columns = [c for c in columns if c != pk_column]

    set_clause = ", ".join(
        f"{col} = EXCLUDED.{col}" for col in update_columns
    )

    upsert_sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        SELECT {', '.join(columns)} FROM {staging_table}
        ON CONFLICT ({pk_column}) DO UPDATE
        SET {set_clause}
    """

    with engine.begin() as conn:
        logger.info(f"LOADING {len(df)} RECORDS INTO STAGING TABLE '{staging_table}'...")
        df.to_sql(staging_table, con=conn, if_exists="replace", index=False)

        logger.info(f"PERFORMING UPSERT INTO TARGET TABLE '{table_name}'...")
        conn.execute(text(upsert_sql))

        conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
        logger.info(f"UPSERT COMPLETE FOR '{table_name}'. STAGING TABLE DROPPED.")