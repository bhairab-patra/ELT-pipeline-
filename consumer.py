"""
REAL-TIME CONSUMER — replaces the "wait for nightly cron" model.

Debezium publishes one Kafka message per row change (insert/update/delete)
on source_sys.sales, to the topic "salesdb.source_sys.sales". This script
listens to that topic forever and processes each new order the moment it
happens — no more waiting for 2 AM.

It reuses the exact same quality_check / transform / load_to_warehouse
functions from the nightly pipeline — only the trigger changed (an event
instead of a schedule), the business logic stays identical.

Run this file and leave it running (like a background service):
    python consumer.py

Requires: pip install kafka-python
"""

import json

import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine

from config import DB_URL
from logger import log
from quality_checks import quality_check
from transform import transform
from warehouse_loader import load_to_warehouse
from ingestion import load_to_staging

TOPIC = "salesdb.source_sys.sales"
BOOTSTRAP_SERVERS = "localhost:9092"

engine = create_engine(DB_URL)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="sales-elt-consumer",
)


def handle_event(event: dict) -> None:
    """Debezium wraps each change in a payload with 'op' (c=create,
    u=update, d=delete, r=initial snapshot read) and 'after' (the new
    row's values). We only care about new orders being created.
    """
    payload = event.get("payload", event)
    op = payload.get("op")

    if op not in ("c", "r"):
        log.info("Skipping non-insert event (op=%s)", op)
        return

    after = payload.get("after")
    if not after:
        return

    order_id = after.get("order_id")
    log.info("New order detected: order_id=%s", order_id)

    df = pd.DataFrame([after])

    if not quality_check(df):
        log.error("Order %s failed quality check, skipping", order_id)
        return
    load_to_staging(df, engine)

    # NOTE: transform() currently builds a daily rollup per batch. For a
    # single streamed row this will produce a one-row rollup each time —
    # fine for the fact table (it's naturally append-only per event), but
    # daily_kpis will need incremental aggregation logic later instead of
    # being recomputed per event. Flagging this so it isn't a surprise.
    warehouse_tables = transform(df)
    load_to_warehouse(warehouse_tables, engine)
    log.info("Order %s loaded into warehouse", order_id)


def run_consumer() -> None:
    log.info("Listening for new orders on topic: %s", TOPIC)
    for message in consumer:
        if message.value is None:
            continue
        try:
            handle_event(message.value)
        except Exception:
            log.exception("Failed to process message, continuing to next one")


if __name__ == "__main__":
    run_consumer()