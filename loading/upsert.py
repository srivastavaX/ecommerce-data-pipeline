import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


def upsert_dataframe(df, table_name, pk_column, engine):
    """
    Upsert a DataFrame into a SQL table

    Parameters:
    - df:
    - table_name:
    - pk_column:
    - engine:
    """
    if df.empty:
        logger.info("DATAFRAME IS EMPTY. NO RECORDS TO UPSERT.")
        return

    staging_table = f"{table_name}_staging"
    columns = df.columns.tolist()

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
        try:
            logger.info(f"LOADING {len(df)} RECORDS INTO STAGING TABLE '{staging_table}'...")
            df.to_sql(
                name=staging_table,
                con=conn,
                if_exists='replace',
                index=False,
            )
        except Exception as e:
            logger.error(f"FAILED TO LOAD DATA INTO STAGING TABLE '{staging_table}': {e}")
            raise
    
    # UPSERT FROM STAGING TO TARGET TABLE
    logger.info(f"PERFORMING UPSERT INTO TARGET TABLE '{table_name}'...")
    conn.execute(text(upsert_sql))

    # DROP STAGING TABLE
    conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
    logger.info(f"UPSERT COMPLETE FOR TABLE: '{table_name}'; STAGING TABLE DROPPED.")