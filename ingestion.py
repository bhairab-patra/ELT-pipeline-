from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import SCHEMAS
from logger import log


def extract_sales(engine: Engine) -> pd.DataFrame:
    query = text(f"""
        SELECT order_id, sales_rep, region, product, amount, order_date
        FROM {SCHEMAS['source']}.sales
        WHERE order_date::date = (CURRENT_DATE - INTERVAL '1 day')::date
    """)
    df = pd.read_sql(query, engine)
    log.info("Extracted %d rows from %s.sales", len(df), SCHEMAS['source'])
    return df


def load_to_staging(df: pd.DataFrame, engine: Engine) -> None:
    df = df.copy()
    df["_loaded_at"] = datetime.now(timezone.utc).isoformat()
    df.to_sql(
        "raw_sales",
        engine,
        schema=SCHEMAS["staging"],
        if_exists="append",
        index=False,
    )
    log.info("Loaded %d rows into %s.raw_sales", len(df), SCHEMAS['staging'])


def ingest(engine: Engine) -> pd.DataFrame:
    raw_df = extract_sales(engine)
    if not raw_df.empty:
        load_to_staging(raw_df, engine)
    else:
        log.warning("Ingestion produced no rows — nothing to stage today")
    return raw_df