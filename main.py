from sqlalchemy import create_engine

from config import DB_URL
from db_setup import ensure_schemas
from ingestion import ingest
from logger import log
from quality_checks import quality_check
from reporting import generate_reports
from transform import transform
from warehouse_loader import load_to_warehouse


def run_pipeline() -> None:
    log.info("=== ELT pipeline started ===")

    engine = create_engine(DB_URL)
    ensure_schemas(engine)

    try:
        raw_df = ingest(engine)

        if not quality_check(raw_df):
            log.error("Pipeline halted: quality gate failed. Warehouse not updated.")
            return

        warehouse_tables = transform(raw_df)
        load_to_warehouse(warehouse_tables, engine)
        generate_reports(warehouse_tables)

        log.info("=== ELT pipeline completed successfully ===")

    except Exception:
        log.exception("Pipeline failed with an unhandled error")
        raise


if __name__ == "__main__":
    run_pipeline()