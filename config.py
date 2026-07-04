from pathlib import Path

BASE_DIR = Path(__file__).parent

PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "sales_pipeline"

DB_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

SCHEMAS = {
    "source": "source_sys",
    "staging": "staging",
    "warehouse": "warehouse",
}

REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

LOG_FILE = BASE_DIR / "pipeline.log"