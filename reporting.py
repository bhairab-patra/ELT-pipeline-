"""
REPORTING LAYER

Turns warehouse tables into the files each audience actually looks at.
If tomorrow you need to email these instead of saving CSVs, or push them
to a dashboard tool, this is the only file that changes.
"""

from datetime import datetime, timezone

import pandas as pd

from config import REPORTS_DIR
from logger import log


def generate_reports(tables: dict[str, pd.DataFrame]) -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    sales_path = REPORTS_DIR / f"sales_report_{today}.csv"
    tables["fact_sales_by_rep"].to_csv(sales_path, index=False)
    log.info("Sales report written -> %s", sales_path)

    ceo_path = REPORTS_DIR / f"ceo_dashboard_{today}.csv"
    tables["daily_kpis"].to_csv(ceo_path, index=False)
    log.info("CEO dashboard written -> %s", ceo_path)
