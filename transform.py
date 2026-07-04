"""
TRANSFORMATION LAYER

Reshapes raw ingested data into warehouse-ready, business-friendly tables.

Deliberately pure pandas here — no database connections, no file writes.
That means you can unit test this file by feeding it a small DataFrame and
asserting on the output, with no database required at all.
"""

import pandas as pd


def transform(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Builds the tables the warehouse layer will store."""
    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"])

    # Fact-style table: one row per rep per day (what Sales needs)
    fact_sales_by_rep = (
        df.groupby(["sales_rep", "region", "order_date"], as_index=False)
        .agg(total_amount=("amount", "sum"), order_count=("order_id", "count"))
        .sort_values("total_amount", ascending=False)
    )

    # Aggregate table: company-wide daily rollup (what the CEO needs)
    daily_kpis = pd.DataFrame({
        "report_date": [df["order_date"].max()],
        "total_revenue": [df["amount"].sum()],
        "total_orders": [len(df)],
        "avg_order_value": [df["amount"].mean()],
        "active_reps": [df["sales_rep"].nunique()],
        "top_region": [df.groupby("region")["amount"].sum().idxmax()],
    })

    return {
        "fact_sales_by_rep": fact_sales_by_rep,
        "daily_kpis": daily_kpis,
    }
