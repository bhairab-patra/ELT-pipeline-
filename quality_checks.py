"""
QUALITY GATE

Runs on ingested data before it's allowed to move into the warehouse.
This is a separate file on purpose: quality rules are something a data
analyst or business user should be able to read and update without ever
touching extraction or transformation code.
"""

import pandas as pd

from logger import log


def quality_check(df: pd.DataFrame) -> bool:
    """Returns True if the data is safe to load into the warehouse.

    Extend this with whatever checks matter for your data: referential
    checks, range checks, row-count deltas vs. the prior run, etc.
    Fail loudly rather than silently loading bad data.
    """
    if df.empty:
        log.warning("Quality check: no rows to validate — skipping load")
        return False

    problems = []
    if df["amount"].isnull().any():
        problems.append("null values in 'amount'")
    if (df["amount"] < 0).any():
        problems.append("negative values in 'amount'")
    if df["order_id"].duplicated().any():
        problems.append("duplicate order_id values")

    if problems:
        log.error("Quality check FAILED: %s", "; ".join(problems))
        return False

    log.info("Quality check passed (%d rows)", len(df))
    return True
