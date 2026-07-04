import pandas as pd
from sqlalchemy.engine import Engine

from config import SCHEMAS
from logger import log


def load_to_warehouse(tables: dict[str, pd.DataFrame], engine: Engine) -> None:
    for name, table_df in tables.items():
        table_df.to_sql(
            name,
            engine,
            schema=SCHEMAS["warehouse"],
            if_exists="append",
            index=False,
        )
        log.info("Loaded %d rows into %s.%s", len(table_df), SCHEMAS['warehouse'], name)