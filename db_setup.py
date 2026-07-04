from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import SCHEMAS
from logger import log


def ensure_schemas(engine: Engine) -> None:
    with engine.begin() as conn:
        for schema_name in SCHEMAS.values():
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
    log.info("Schemas ready: %s", ", ".join(SCHEMAS.values()))