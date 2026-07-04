from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import create_engine

from config import DB_URL, SCHEMAS
from db_setup import ensure_schemas

engine = create_engine(DB_URL)
ensure_schemas(engine)

yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)

reps = ["Asha Rao", "Ben Cole", "Priya Iyer", "Sam Osei"]
regions = ["South", "North", "West", "East"]
products = ["Widget A", "Widget B", "Gadget X"]

rows = []
for i in range(20):
    rows.append({
        "order_id": 1000 + i,
        "sales_rep": reps[i % len(reps)],
        "region": regions[i % len(regions)],
        "product": products[i % len(products)],
        "amount": round(100 + 37.5 * i, 2),
        "order_date": str(yesterday),
    })

df = pd.DataFrame(rows)
df.to_sql("sales", engine, schema=SCHEMAS["source"], if_exists="replace", index=False)
print(f"Seeded {len(df)} sample orders dated {yesterday} into {SCHEMAS['source']}.sales")