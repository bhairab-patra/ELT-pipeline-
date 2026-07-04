 # Sales ELT Pipeline — Batch + Real-Time Streaming

A data engineering pipeline that moves sales data from a source PostgreSQL
database into a central data warehouse, and generates reports for Sales
reps and the CEO. The pipeline supports two modes of operation:

1. **Batch (nightly ELT)** — a scheduled job pulls the previous day's data.
2. **Real-time streaming (CDC)** — using PostgreSQL's transaction log,
   Kafka, and Debezium, every new order is captured and processed the
   moment it's inserted, with no wait for a nightly schedule.

This document captures the full working setup for future reference.

---

## 1. High-Level Architecture

### 1.1 Batch mode (nightly)

```
 Source DB (source_sys.sales)
         |
         v
 Ingestion Layer (extract + load raw)
         |
         v
 Staging Layer (staging.raw_sales)  <-- raw, untouched copy
         |
         v
 Quality Gate (validates before warehouse load)
         |
         v
 Data Warehouse (warehouse.*)        <-- central, transformed layer
         |
         v
 Reporting Layer (CSV reports)
         |
         v
 Sales team  /  CEO dashboard
```

### 1.2 Real-time mode (CDC streaming)

```
 Postgres WAL (Write-Ahead Log)
         |
         v
 Debezium (Kafka Connect connector)
         |
         v
 Kafka topic: salesdb.source_sys.sales
         |
         v
 consumer.py (always running)
         |
         v
 Quality Gate --> Staging --> Transform --> Warehouse
         |
         v
 Reports (same warehouse tables Sales/CEO read from)
```

**Key idea:** both modes share the exact same business logic
(`quality_checks.py`, `transform.py`, `warehouse_loader.py`). Only the
*trigger* is different — a schedule (batch) vs. an event (streaming).

---

## 2. Core Concepts / Glossary

| Term | Meaning |
|---|---|
| **ELT** | Extract, Load, Transform — raw data is loaded before it's transformed, so a copy of the untouched data always exists (in staging). |
| **Ingestion layer** | The part of the pipeline responsible for bringing data *into* the system — extraction + writing the raw copy to staging. |
| **Central layer / Data warehouse** | The single source of truth for all reporting. Every report reads from here, never from staging or the source DB directly. |
| **Quality gate** | Automated checks (nulls, duplicates, negative values, etc.) that must pass before data is allowed into the warehouse. |
| **CDC (Change Data Capture)** | A technique for detecting and streaming database changes (inserts/updates/deletes) as they happen, instead of polling on a schedule. |
| **WAL (Write-Ahead Log)** | PostgreSQL's internal transaction log. Every change to the database is recorded here before being applied — Debezium reads this log directly. |
| **Debezium** | An open-source CDC tool that reads a database's transaction log and publishes each change as a Kafka event. |
| **Kafka** | A distributed message broker. Debezium publishes change events to a Kafka *topic*; `consumer.py` subscribes to that topic and processes events as they arrive. |
| **Kafka Connect** | The framework that runs Debezium as a plugin ("connector") and manages its lifecycle (start/stop/restart, offsets, etc.). |
| **Replication slot** | A PostgreSQL mechanism that reserves a position in the WAL for a specific consumer (Debezium), ensuring no changes are missed even if Debezium temporarily disconnects. |
| **Schema (Postgres)** | A namespace inside a single database. This project uses one database (`sales_pipeline`) with three schemas (`source_sys`, `staging`, `warehouse`) to represent the three pipeline layers — the standard way to separate layers within one Postgres instance. |

---

## 3. Folder & File Structure

```
ELT pipeline/                       <- project root
│
├── venv/                           <- Python virtual environment (not shared/committed)
│
├── config.py                       <- all settings: DB connection, schema names, paths
├── logger.py                       <- shared logging setup used by every module
├── db_setup.py                     <- creates the 3 Postgres schemas if they don't exist
│
├── ingestion.py                    <- DATA INGESTION LAYER: extract from source + load raw to staging
├── quality_checks.py               <- validates data before it's allowed into the warehouse
├── transform.py                    <- pure pandas logic: raw data -> business-ready tables
├── warehouse_loader.py             <- writes transformed tables into the warehouse schema
├── reporting.py                    <- generates the final CSV reports for Sales / CEO
│
├── main.py                         <- BATCH orchestrator (nightly run, one-shot script)
├── consumer.py                     <- STREAMING orchestrator (runs forever, event-driven)
├── seed_demo_data.py               <- creates sample data in source_sys.sales for testing
│
├── docker-compose.yml              <- spins up Kafka, Zookeeper, and Kafka Connect (with Debezium)
├── register-connector.json         <- Debezium connector config (which table to watch, how)
│
├── reports/                        <- auto-created; dated CSV outputs land here
│   ├── sales_report_YYYY-MM-DD.csv
│   └── ceo_dashboard_YYYY-MM-DD.csv
│
└── pipeline.log                    <- auto-created; every run's log output (console + file)
```

### File responsibilities, in plain terms

- **`config.py`** — the only file with environment-specific values (DB
  user/password/host, schema names). Change this file when moving from a
  local machine to another environment.
- **`logger.py`** — every other file imports `log` from here so all
  output goes to the same format, same file (`pipeline.log`), same console.
- **`db_setup.py`** — idempotent (`CREATE SCHEMA IF NOT EXISTS`), safe to
  call every run.
- **`ingestion.py`** — the *only* file that talks to the source database
  for reading. Combines Extract + Load-to-staging (the ingestion layer).
- **`quality_checks.py`** — no database or business logic, just validation
  rules. Meant to be readable/editable by a non-engineer if needed.
- **`transform.py`** — pure functions, no side effects, no I/O. Fully
  unit-testable with a small in-memory DataFrame.
- **`warehouse_loader.py`** — the *only* file that writes to the warehouse
  schema.
- **`reporting.py`** — the *only* file that decides what the final report
  files look like. Swap CSV for email/dashboard here later without
  touching anything upstream.
- **`main.py`** — a thin orchestrator with no business logic of its own;
  it just calls each layer in order and stops if a step fails.
- **`consumer.py`** — the real-time equivalent of `main.py`. Listens to
  the Kafka topic forever and runs the same quality → staging → transform
  → warehouse flow per event instead of per night.
- **`seed_demo_data.py`** — for testing only; populates `source_sys.sales`
  with sample rows.
- **`docker-compose.yml`** — defines three containers: `zookeeper`,
  `kafka`, and `connect` (Kafka Connect + Debezium).
- **`register-connector.json`** — tells the running Kafka Connect
  instance: connect to this Postgres, watch this table, use this
  replication slot name.

---

## 4. Prerequisites

- **Python 3.10+** (for the `dict[str, pd.DataFrame]` type hints used
  throughout)
- **PostgreSQL** (running locally, managed via pgAdmin)
- **Docker Desktop** (for Kafka + Kafka Connect + Debezium)
- **Git Bash** (or any terminal) for running commands

### Python packages

```bash
pip install pandas sqlalchemy psycopg2-binary kafka-python
```

| Package | Used for |
|---|---|
| `pandas` | all data manipulation (extract, transform, reports) |
| `sqlalchemy` | database connection + ORM-style `to_sql` / `read_sql` |
| `psycopg2-binary` | the actual PostgreSQL driver SQLAlchemy uses |
| `kafka-python` | the Kafka client used by `consumer.py` |

---

## 5. PostgreSQL Setup

### 5.1 Create the database

In pgAdmin: **Servers → Databases → right-click → Create → Database**,
name it `sales_pipeline`.

### 5.2 Enable logical replication (required for Debezium/CDC)

Run in pgAdmin's Query Tool (connected to any database, e.g. `postgres`):

```sql
ALTER SYSTEM SET wal_level = logical;
```

This writes the setting to `postgresql.auto.conf` automatically — no need
to locate and hand-edit `postgresql.conf`.

**A full PostgreSQL service restart is required** for this to take
effect (a config reload is not enough, since `wal_level` changes the
server's startup behavior).

Restart via Windows: press **Win + R**, type `services.msc`, find the
PostgreSQL service (e.g. `postgresql-x64-18`), right-click → **Restart**.

Confirm it took effect:

```sql
SHOW wal_level;
```

Expected output: `logical`

### 5.3 Allow replication connections

In `pg_hba.conf`, add (for local development):

```
host  replication  postgres  0.0.0.0/0  md5
```

### 5.4 Schemas

The three schemas (`source_sys`, `staging`, `warehouse`) are created
automatically by `db_setup.py` — no manual step needed, as long as
`ensure_schemas()` is called (it is, at the top of both `main.py` and
`seed_demo_data.py`).

---

## 6. Configuration

Edit `config.py` — this is the **only** file with environment-specific
values:

```python
PG_USER = "postgres"
PG_PASSWORD = "your_password_here"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "sales_pipeline"

SCHEMAS = {
    "source": "source_sys",
    "staging": "staging",
    "warehouse": "warehouse",
}
```

---

## 7. Running the Batch Pipeline (Nightly Mode)

```bash
cd "ELT pipeline"
source venv/Scripts/activate      # Git Bash on Windows
python seed_demo_data.py          # first time only, creates sample data
python main.py
```

**What happens, in order:**
1. Schemas are ensured to exist.
2. `extract_sales()` pulls yesterday's rows from `source_sys.sales`.
3. `quality_check()` validates the extracted rows.
4. If valid, `load_to_staging()` writes the raw rows to `staging.raw_sales`.
5. `transform()` builds two tables in memory: a per-rep daily rollup and
   a company-wide KPI summary.
6. `load_to_warehouse()` writes both into the `warehouse` schema.
7. `generate_reports()` writes two dated CSVs into `reports/`.

Verify with pgAdmin:

```sql
SELECT * FROM staging.raw_sales ORDER BY order_id DESC;
SELECT * FROM warehouse.fact_sales_by_rep ORDER BY order_date DESC;
SELECT * FROM warehouse.daily_kpis ORDER BY report_date DESC;
```

Schedule nightly via cron (Linux/Mac) or Windows Task Scheduler:

```
0 2 * * *  /path/to/venv/bin/python /path/to/main.py
```

---

## 8. Setting Up Real-Time Streaming (CDC Mode)

### 8.1 Start Kafka + Debezium

```bash
docker compose up -d
```

This starts three containers: `zookeeper`, `kafka`, `connect`. Give it
30–60 seconds to fully initialize. Check status:

```bash
docker compose ps
```

All three should show `Up`.

### 8.2 Register the Debezium connector

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @register-connector.json http://localhost:8083/connectors
```

Verify it's running:

```bash
curl http://localhost:8083/connectors/sales-postgres-connector/status
```

Expected: `"state": "RUNNING"` for both the `connector` and its `tasks`.

Confirm a replication slot was created:

```sql
SELECT * FROM pg_replication_slots;
```

Expected: a row named `debezium_slot`.

### 8.3 Start the consumer

```bash
python consumer.py
```

This runs **continuously** — leave the terminal open. It listens to the
Kafka topic `salesdb.source_sys.sales` and processes each new order the
moment Debezium publishes it.

### 8.4 End-to-end test

Insert a test row via pgAdmin:

```sql
INSERT INTO source_sys.sales (order_id, sales_rep, region, product, amount, order_date)
VALUES (5001, 'Asha Rao', 'South', 'Widget A', 499.99, CURRENT_DATE);
```

Within a few seconds, the `consumer.py` terminal should log:

```
New order detected: order_id=5001
...
Order 5001 loaded into warehouse
```

Verify in pgAdmin:

```sql
SELECT * FROM staging.raw_sales ORDER BY order_id DESC LIMIT 5;
SELECT * FROM warehouse.fact_sales_by_rep ORDER BY order_date DESC LIMIT 5;
```

### 8.5 Useful diagnostic commands

Check what's actually landing in the Kafka topic (bypassing the Python
consumer, to isolate whether the issue is Debezium or `consumer.py`):

```bash
docker exec -it eltpipeline-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic salesdb.source_sys.sales \
  --from-beginning --timeout-ms 10000
```

Restart a failed connector task without re-registering from scratch:

```bash
curl -X POST http://localhost:8083/connectors/sales-postgres-connector/tasks/0/restart
```

View live Kafka Connect logs:

```bash
docker compose logs -f connect
```

---

## 9. Batch vs. Streaming — When Each Runs

| | `main.py` (batch) | `consumer.py` (streaming) |
|---|---|---|
| Trigger | Schedule (cron, nightly) | Event (Kafka message, real-time) |
| Run pattern | Runs once, exits | Runs forever, always listening |
| Data window | "Yesterday's" rows | Every row the instant it's inserted |
| Good for | Predictable, simple ops; historical backfill | Live dashboards, instant visibility |

Both currently use the same `quality_checks.py`, `transform.py`, and
`warehouse_loader.py` — the business logic is identical; only the trigger
differs.

---

## 10. Roadmap — Next Improvements

Reliability / production-readiness (recommended next, roughly in order):

1. Make `daily_kpis` update incrementally instead of recomputing from
   scratch on every streamed event.
2. Add idempotency to `consumer.py` (skip an `order_id` already loaded,
   in case of consumer restart/reprocessing).
3. Route quality-check failures to a dead-letter table/topic instead of
   silently skipping them.
4. Run `consumer.py` as a proper background service (Windows Task
   Scheduler / `nssm`) with auto-restart on crash.
5. Add monitoring/alerting (e.g. notify if the consumer stops or Kafka
   lag grows).
6. Handle source schema changes gracefully (new columns shouldn't crash
   the pipeline).
7. Handle `update` and `delete` events from Debezium, not just `create`.
8. Move credentials out of `config.py` into environment variables / a
   `.env` file excluded from version control.
9. Decide whether `main.py` (batch) stays as a backfill/disaster-recovery
   tool alongside streaming, or is retired entirely.
10. Tune Kafka topic retention and partitioning for real production volume.
11. Connect a BI tool (Power BI / Metabase / Tableau) directly to the
    warehouse schema instead of distributing static CSVs.

Enterprise-scale additions (for multi-team environments):

- Automated tests (unit tests for `transform.py`, integration tests for
  the full flow)
- CI/CD pipeline (e.g. GitHub Actions)
- Infrastructure as Code (Terraform / Kubernetes, replacing manual
  `docker compose`)
- Data catalog & lineage tracking (e.g. DataHub, OpenMetadata)
- Access control / RBAC on warehouse schemas
- A dedicated orchestrator (Airflow / Dagster) instead of cron
- Backup & disaster recovery plan for both Postgres and Kafka
- A written runbook for on-call failure scenarios