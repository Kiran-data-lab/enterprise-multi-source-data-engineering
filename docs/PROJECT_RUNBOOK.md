# Project Runbook — Enterprise Multi-Source Data Engineering Pipeline

## 1. Purpose and implementation boundary

This document records the implementation represented by the uploaded source files and the previously confirmed successful Airflow run.

The implemented workflow is:

1. Read the latest `UPDATED_AT` watermark for each entity from its Snowflake RAW table.
2. Extract changed rows from Oracle or SQL Server using a five-minute overlap.
3. Reconcile the three source-view partitions and write CSV, JSON, and Parquet files.
4. Upload the six files for that source to a Snowflake internal stage with `PUT`.
5. Load all three format files into a temporary Snowflake table with `COPY INTO`.
6. Deduplicate by business key and merge into the corresponding RAW table.
7. After both source pipelines finish, run `dbt build`.
8. Airflow marks the DAG successful only when the two source tasks and dbt task complete successfully.

The implementation does **not** currently use Snowpipe, an Airflow Snowflake provider, or an external-file ingestion task. The Snowflake Python connector performs `PUT`, `COPY INTO`, and `MERGE`. External-file ingestion remains a future extension.

## 2. Project locations and execution environments

### 2.1 Project root

Windows:
```text
E:\Snowflake\snowflake-data-engineering
```

WSL:
```text
/mnt/e/Snowflake/snowflake-data-engineering
```

The source-ingestion Python scripts resolve their project root from `__file__` and load `.env` from that root.

### 2.2 Source-ingestion environment (WSL)

- OS: Ubuntu 26.04.1 LTS
- Python: 3.14.4
- Virtual environment: `~/snowflake-oracle-venv`
- Activate:
```bash
source ~/snowflake-oracle-venv/bin/activate
```

The uploaded code imports these packages:
- `oracledb`
- `pyodbc`
- `pandas`
- `pyarrow` (used by pandas Parquet output)
- `python-dotenv`
- `snowflake-connector-python`

The repository includes `requirements-project.txt` as a package list. It is intentionally not version-pinned because the uploaded archive did not contain the installed package freeze.

### 2.3 Airflow environment (WSL)

- Airflow version: 3.3.2
- Virtual environment: `~/airflow-project/.venv`
- Airflow home: `~/airflow-project/airflow-home`
- DAG folder: `/home/kk_ubuntu/airflow-project/airflow-home/dags`

Start Airflow:
```bash
source ~/airflow-project/.venv/bin/activate
export AIRFLOW_HOME=~/airflow-project/airflow-home
airflow standalone
```

Airflow UI:
```text
http://localhost:8080
```

The DAG file is versioned in this repository at:
```text
airflow/dags/enterprise_multi_source_dag.py
```

The deployed DAG file was placed in the Airflow home DAG folder. Copy it there when deploying a checkout:
```bash
cp /mnt/e/Snowflake/snowflake-data-engineering/airflow/dags/enterprise_multi_source_dag.py \
   "$AIRFLOW_HOME/dags/enterprise_multi_source_dag.py"
```

The DAG uses `airflow.sdk` (`@dag`, `@task`) and does not import an Airflow Snowflake provider. Snowflake access is performed by the project's own Python environment and Snowflake connector.

### 2.4 dbt environment (Windows)

- dbt project: `E:\Snowflake\snowflake-data-engineering\dbt_enterprise`
- Python environment: `C:\snowflake-dbt\.venv`
- dbt Core: 1.12.5
- dbt-snowflake adapter: 1.12.1
- Windows Python: 3.13.16
- Project name/profile name: `dbt_enterprise`

Manual dbt build from PowerShell:
```powershell
Set-Location 'E:\Snowflake\snowflake-data-engineering\dbt_enterprise'
& 'C:\snowflake-dbt\.venv\Scripts\python.exe' -m dbt.cli.main build
```

The dbt profile is normally stored at:
```text
%USERPROFILE%\.dbt\profiles.yml
```

The actual profile was not in the uploaded ZIP. A credential-safe template is provided as `dbt_enterprise/profiles.example.yml`. Copy/adapt it to the normal dbt profile location and provide the referenced `DBT_SNOWFLAKE_*` environment variables in Windows before running dbt. The root `.env` is not automatically loaded by dbt.

## 3. Environment configuration

### 3.1 Root `.env`

Create a local `.env` in the project root using `.env.example` as a template. Do not commit `.env`.

Required variables used by the uploaded code:

| Variable | Used by | Meaning |
|---|---|---|
| `ORACLE_USER` | Oracle extractor, connection test, audit utility | Oracle extraction username |
| `ORACLE_PASSWORD` | Oracle extractor, connection test, audit utility | Oracle password |
| `ORACLE_DSN` | Oracle extractor, connection test, audit utility | Oracle connect descriptor/service |
| `SQLSERVER_SERVER` | SQL Server extractor, connection test, audit utility | SQL Server instance |
| `SQLSERVER_DATABASE` | SQL Server extractor, connection test, audit utility | Source database |
| `SQLSERVER_USER` | SQL Server extractor, connection test, audit utility | SQL Server login |
| `SQLSERVER_PASSWORD` | SQL Server extractor, connection test, audit utility | SQL Server password |
| `SQLSERVER_DRIVER` | Connection test and audit utility | ODBC driver name; the extractor itself hardcodes ODBC Driver 18 |
| `SNOWFLAKE_ACCOUNT` | Orchestrator, uploader, RAW loader, connection test | Snowflake account identifier |
| `SNOWFLAKE_USER` | Same Snowflake scripts | Snowflake user |
| `SNOWFLAKE_PASSWORD` | Same Snowflake scripts | Snowflake password |
| `SNOWFLAKE_ROLE` | Same Snowflake scripts | Snowflake role |
| `SNOWFLAKE_DATABASE` | Same Snowflake scripts | Default database; project uses `ENTERPRISE_DW` |
| `SNOWFLAKE_SCHEMA` | Same Snowflake scripts | Default schema; project uses `RAW` |

`run_source_pipeline.py`, `upload_to_snowflake_stage.py`, `load_raw_to_snowflake.py`, and `test_snowflake_connection.py` load `.env` using the project-root path. The source extractors do the same. `test_connections.py` uses `load_dotenv()` and should be run with the project root as the working directory.

### 3.2 dbt profile variables

The supplied `profiles.example.yml` expects:
- `DBT_SNOWFLAKE_ACCOUNT`
- `DBT_SNOWFLAKE_USER`
- `DBT_SNOWFLAKE_PASSWORD`
- `DBT_SNOWFLAKE_ROLE`

Set these in the Windows environment used by the dbt process. Use the actual role and account values from the existing working profile; do not copy credentials into Git.

## 4. Source databases: creation and preparation

The SQL files are working scripts/notes collected during project construction. Some are alternatives or validation scripts; do not execute every file in a folder blindly.

### 4.1 Oracle

Source details:
- Oracle 21c
- PDB/service: `ORCLPDB` (the DSN used by the Python client must resolve to this PDB)
- Source schema: `ORACLE_SRC`
- Extraction account: `ORACLE_ETL`
- Tables: `CUSTOMERS` (5,000 initial rows), `ORDERS` (20,000 initial rows)

Recommended order represented by the files:

| Order | File | Purpose / execution context |
|---:|---|---|
| 1 | `Oracle Scripts/Oracle_Query_1.sql` | Run as an Oracle administrative account connected to the intended PDB. Checks container/service, creates `ORACLE_SRC`, grants session/table/sequence/view privileges, and grants quota. |
| 2 | `Oracle Scripts/Oracle_Query_2.sql` | Run while connected as `ORACLE_SRC`. Creates `CUSTOMERS` and `ORDERS`, inserts 5,000 customers and 20,000 orders, commits, and checks row counts/orphan orders. |
| 3 | `Oracle Scripts/Oracle_ORACLE_ETL-user.sql` | Run as an administrative account. Creates `ORACLE_ETL`, grants `CREATE SESSION` and SELECT on the base tables. The password is masked in the uploaded file; supply a real password locally. |
| 4 | `Oracle Scripts/Oracle - view created.sql` | Run as `ORACLE_SRC` or an appropriately privileged account. Creates six format-partition views and grants SELECT on them to `ORACLE_ETL`. |
| 5 | `Oracle Scripts/update customers.sql` | Optional CDC test: updates customer 1 and soft-deletes customer 2, then verifies the records. It requires the `IS_DELETED` column to exist. |

The Oracle extractor reads these views:

| Entity | CSV view | JSON view | Parquet view |
|---|---|---|---|
| Customers | `ORACLE_SRC.CSV_CUSTOMERS` | `ORACLE_SRC.JSON_CUSTOMERS` | `ORACLE_SRC.PARQUET_CUSTOMERS` |
| Orders | `ORACLE_SRC.CSV_ORDERS` | `ORACLE_SRC.JSON_ORDERS` | `ORACLE_SRC.PARQUET_ORDERS` |

The customer views partition by `MOD(CUSTOMER_ID, 3)` values 0, 1, and 2. The order views join to customers and partition orders by the related customer's ID modulo 3. The three views collectively cover the entity rows.

**Oracle DDL completeness note:** The uploaded `Oracle_Query_2.sql` creates `CUSTOMERS.UPDATED_AT`, but does not create `IS_DELETED`; its `ORDERS` definition does not create `UPDATED_AT` or `IS_DELETED`. The current extractor explicitly requires both `UPDATED_AT` and `IS_DELETED` on each view. The live source schema used for the successful run therefore had additional CDC columns not fully represented in the uploaded Oracle creation script. The exact ALTER statements were not present in the ZIP. Add the verified live-schema ALTER statements to source control before treating the Oracle setup scripts as a clean-room rebuild.

### 4.2 SQL Server

Source details:
- Instance: `DESKTOP-T90O6I3\KIRAN_DATA`
- Database: `ENTERPRISE_SOURCE`
- Schema: `SQLSERVER_SRC`
- Extraction login/user: `ETL_READER`
- Tables: `PRODUCTS` (500 initial rows), `INVENTORY` (1,500 initial rows)

Recommended order represented by the files:

| Order | File | Purpose / execution context |
|---:|---|---|
| 1 | `SQL Server scripts/SQL Server - Create Database_Schema.sql` | Run in SSMS with database-creation permission. Creates `ENTERPRISE_SOURCE` and schema `SQLSERVER_SRC`. |
| 2 | `SQL Server scripts/SQL_Server-PRODUCTS table.sql` | Run in `ENTERPRISE_SOURCE`. Creates `PRODUCTS`. |
| 3 | `SQL Server scripts/SQL_Server-INVENTORY table.sql` | Run in `ENTERPRISE_SOURCE`. Creates `INVENTORY`, its product foreign key, and product/warehouse unique constraint. |
| 4 | `SQL Server scripts/SQL_Server - Load PRODUCTS table.sql` | Loads 500 products if the table is empty. |
| 5 | `SQL Server scripts/SQL_Server - Load Inventory table.sql` | Loads three warehouse rows per product (1,500 inventory rows) if the table is empty. |
| 6 | `SQL Server scripts/Alter-scripts-1.sql` | Adds/backfills `UPDATED_AT`, adds `IS_DELETED`, and creates update triggers for both tables. |
| 7 | `SQL Server scripts/Alter-scripts-2.sql` | Creates the six format-partition views consumed by the extractor. |
| 8 | `SQL Server scripts/Create ETL_READER login to python connect.sql` | Run with appropriate server/database permissions. Creates the login, maps the database user, and grants SELECT on `SQLSERVER_SRC`. The password is masked in the uploaded file. |
| 9 | `SQL Server scripts/Validation check.sql`, `Validation query.sql`, `table counts.sql` | Validation scripts for database/login mapping, orphan inventory, and table counts. |

The SQL Server extractor reads:

| Entity | CSV view | JSON view | Parquet view |
|---|---|---|---|
| Products | `SQLSERVER_SRC.CSV_PRODUCTS` | `SQLSERVER_SRC.JSON_PRODUCTS` | `SQLSERVER_SRC.PARQUET_PRODUCTS` |
| Inventory | `SQLSERVER_SRC.CSV_INVENTORY` | `SQLSERVER_SRC.JSON_INVENTORY` | `SQLSERVER_SRC.PARQUET_INVENTORY` |

The views partition products by `PRODUCT_ID % 3`; inventory is partitioned by its related `PRODUCT_ID % 3`. The three partitions collectively cover the source rows.

Other SQL Server files are supporting/alternative scripts:
- `sql queries views created.sql` creates equivalent partition views, with inventory views explicitly joining to PRODUCTS.
- `Alter - login details of user.sql` changes the `ETL_READER` password/default database.
- `alter script - user details.sql` contains user/default-schema configuration.
- `Update products and Inventory.sql` is for CDC update and soft-delete testing.
- `Alter - script -2.sql` contains Oracle SQL view definitions despite being stored in the SQL Server folder; do not treat it as SQL Server DDL.

The SQL Server extractor uses ODBC Driver 18 in its connection string. The test and audit utilities read `SQLSERVER_DRIVER` from `.env`.

## 5. Snowflake environment and objects

Snowflake setup SQL is stored in `Snowflake Scripts/`. The files are dated learning scripts, not a single migration framework. Some contain hardcoded historical run IDs, duplicate setup statements, and destructive `TRUNCATE` commands. Review each statement before replaying it.

### 5.1 Database, schemas, warehouse, stage

`Snowflake Scripts/Project-2_Snowflake_DBT_Airflow_Day-1.sql` creates:

- Database: `ENTERPRISE_DW`
- Schemas: `RAW`, `STAGING`, `CURATED`, `AUDIT`, `UTILS`
- Warehouse: `ENTERPRISE_WH`, XSMALL, auto-suspend 60 seconds, auto-resume true, initially suspended
- Internal stage: `ENTERPRISE_DW.RAW.STG_ENTERPRISE_LANDING`

File formats:
- `FF_ENTERPRISE_CSV`
- `FF_ENTERPRISE_JSON`
- `FF_ENTERPRISE_PARQUET`

### 5.2 Inference formats

`Snowflake Scripts/Project-2_Snowflake_DBT_Airflow_Day-2.sql` adds formats used by the Python RAW loader:

- `FF_ENTERPRISE_CSV_INFER`: CSV with header parsing enabled, optional quote handling, empty fields as NULL, and `ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE` (the setting is adjusted in the day-3 script).
- `FF_ENTERPRISE_JSON_ARRAY`: JSON with `STRIP_OUTER_ARRAY = TRUE`, because pandas `orient="records"` writes a JSON array.
- `FF_ENTERPRISE_PARQUET_INFER`: Parquet with `USE_LOGICAL_TYPE = TRUE`, so logical date/timestamp annotations are interpreted.

The day-2 script also contains `INFER_SCHEMA` exercises and the initial RAW table definitions.

### 5.3 RAW tables

The loader expects these four tables and column sets:

| RAW table | Business key | Source entity |
|---|---|---|
| `RAW_ORACLE_CUSTOMERS` | `CUSTOMER_ID` | Oracle `CUSTOMERS` |
| `RAW_ORACLE_ORDERS` | `ORDER_ID` | Oracle `ORDERS` |
| `RAW_SQLSERVER_PRODUCTS` | `PRODUCT_ID` | SQL Server `PRODUCTS` |
| `RAW_SQLSERVER_INVENTORY` | `INVENTORY_ID` | SQL Server `INVENTORY` |

The day-2 DDL initially creates the base columns and declares primary keys. The day-3 script enables schema evolution on all four RAW tables and reloads the snapshot. The final loader expects `UPDATED_AT` and `IS_DELETED` to be present in each RAW table. Do not omit the schema-evolution preparation when reproducing the final state.

### 5.4 Day-based Snowflake SQL files

| File | Main content | Operational caution |
|---|---|---|
| `Project-2_Snowflake_DBT_Airflow_Day-1.sql` | Database/schemas/warehouse, initial formats, internal stage, account/context checks, stage examples | Contains early file-name examples that differ from the final extractor naming convention. |
| `Project-2_Snowflake_DBT_Airflow_Day-2.sql` | Format inference, schema comparison, initial RAW table DDL, initial file loads and validation | Contains historical run IDs and early experiments. |
| `Project-2_Snowflake_DBT_Airflow_Day-3.sql` | Enables schema evolution, reloads snapshot, validates CDC columns, creates/initializes `AUDIT.ETL_WATERMARK`, and contains manual CDC merge exercises | Includes `TRUNCATE` statements and historical stage paths. Do not rerun blindly. |
| `Project-2_Snowflake_DBT_Airflow_Day-4.sql` | Manual Oracle order CDC load/merge and checks | Uses a hardcoded historical run ID. |
| `Project-2_Snowflake_DBT_Airflow_Day-5.sql` | Repeats initial environment setup and file-format/stage creation | Duplicates Day 1 setup; not a separate pipeline phase. |

The Python orchestrator does **not** read `AUDIT.ETL_WATERMARK`. It reads `MAX(UPDATED_AT)` directly from each RAW table. The watermark table is a separate audit/design artifact from the SQL exercises.

## 6. Python ingestion scripts

### 6.1 `ingestion/oracle/extract_oracle.py`

**Runs in:** WSL project venv `~/snowflake-oracle-venv`, from the project root or through the orchestrator.

**Reads:** `.env` variables `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN`; Oracle views under `ORACLE_SRC`.

**Does:**
- Requires `--customers-watermark` and `--orders-watermark`.
- Parses ISO timestamps and subtracts `--overlap-minutes` (default 5).
- Creates a UTC run ID in `YYYYMMDDTHHMMSSZ` format.
- Reads each entity's CSV, JSON, and Parquet partition view.
- Uses separate count and data cursors and explicitly binds Oracle timestamps as `DB_TYPE_TIMESTAMP`.
- Checks expected vs fetched count and validates non-null `UPDATED_AT` and `IS_DELETED`.
- Writes six files under `data/oracle/<run_id>/`.

Files created:
```text
customers_csv.csv
customers_json.json
customers_parquet.parquet
orders_csv.csv
orders_json.json
orders_parquet.parquet
```

The extractor checks the sum of the three view counts against the base table count at the same cutoff.

### 6.2 `ingestion/sql_server/extract_sqlserver.py`

**Runs in:** WSL project venv `~/snowflake-oracle-venv`.

**Reads:** `.env` variables `SQLSERVER_SERVER`, `SQLSERVER_DATABASE`, `SQLSERVER_USER`, `SQLSERVER_PASSWORD`; SQL Server partition views.

**Does:** The SQL Server equivalent of the Oracle extractor. It requires product and inventory watermarks, subtracts the overlap window, validates row counts and CDC columns, creates a UTC run ID, and writes six files under `data/sql_server/<run_id>/`.

Files created:
```text
products_csv.csv
products_json.json
products_parquet.parquet
inventory_csv.csv
inventory_json.json
inventory_parquet.parquet
```

The script hardcodes `ODBC Driver 18 for SQL Server` in its connection string.

### 6.3 `run_source_pipeline.py` — source orchestrator

**Runs in:** WSL project venv. It is the entry point used by Airflow for each source.

**Does:**
1. Connects to Snowflake using the root `.env`.
2. Reads `MAX(UPDATED_AT)` from the relevant RAW tables:
   - Oracle: `RAW_ORACLE_CUSTOMERS`, `RAW_ORACLE_ORDERS`
   - SQL Server: `RAW_SQLSERVER_PRODUCTS`, `RAW_SQLSERVER_INVENTORY`
3. Fails if any watermark is NULL. The initial full snapshot must exist first.
4. Calls the matching extractor with the two watermarks and a five-minute overlap.
5. Captures the extractor's printed run ID.
6. Calls `upload_to_snowflake_stage.py` with that source and run ID.
7. Calls `load_raw_to_snowflake.py` with that source and run ID.

Commands:
```bash
python run_source_pipeline.py --source oracle
python run_source_pipeline.py --source sql_server
```

This script's watermark is derived from RAW, not from `AUDIT.ETL_WATERMARK`.

### 6.4 `upload_to_snowflake_stage.py`

**Runs in:** WSL project venv.

**Does:**
- Connects to Snowflake using `.env`.
- Selects a specific source/run folder under `data/`.
- Validates that exactly the expected six file names exist for that source.
- Uses Snowflake `PUT` to upload the files to:
  - `@ENTERPRISE_DW.RAW.STG_ENTERPRISE_LANDING/oracle/<run_id>/`
  - `@ENTERPRISE_DW.RAW.STG_ENTERPRISE_LANDING/sql_server/<run_id>/`
- Uses `AUTO_COMPRESS=FALSE` and `OVERWRITE=TRUE`.
- Fails if any upload fails or the successful count is not six.

Example:
```bash
python upload_to_snowflake_stage.py --source oracle --run-id 20261112T120435Z
```

The run ID is an example from a previous run; use the ID printed by the extractor.

### 6.5 `load_raw_to_snowflake.py`

**Runs in:** WSL project venv.

**Does:**
- Validates the run ID format.
- Creates a temporary table `LIKE` the target RAW table for each entity.
- Executes three `COPY INTO` commands for CSV, JSON, and Parquet using the corresponding file formats and `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`.
- Uses `ON_ERROR = 'ABORT_STATEMENT'`.
- Merges the temporary table into the RAW table using the business key.
- Deduplicates the source set with `ROW_NUMBER()` over the business key ordered by `UPDATED_AT DESC NULLS LAST`.
- Updates a matched row only when the incoming `UPDATED_AT` is at least as recent as the target; inserts new keys.
- Commits on success and rolls back on exception.

Command:
```bash
python load_raw_to_snowflake.py --source oracle --run-id <RUN_ID>
```

The same pattern is used for `sql_server`.

**Note on the three formats:** The source views divide the entity rows among CSV, JSON, and Parquet partitions. The three files together represent the full entity delta. The loader's log text says “including format duplicates”; that wording does not describe the current modulo-partition view design, where the three view partitions are intended to be disjoint.

### 6.6 Connection and utility scripts

| File | Purpose | Status |
|---|---|---|
| `test_connections.py` | Opens Oracle and SQL Server connections and reports counts for the four source tables | Diagnostic; run from project root |
| `test_snowflake_connection.py` | Tests Snowflake login and prints current account/user/role/database/schema | Diagnostic |
| `generate_source_data.py` | Earlier standalone CSV generator writing to `data/oracle_extracts` and `data/sqlserver_extracts` | Not used by the current Oracle/SQL Server database extraction pipeline; its schema and output paths differ |
| `audit_extractions.py` | Attempts to compare extracted file counts with current source table counts | Not compatible with the current incremental extractor naming/semantics as uploaded; see Repository Audit |

## 7. Extracted file layout

Each extractor creates a UTC run directory:

```text
data/
├── oracle/
│   └── <YYYYMMDDTHHMMSSZ>/
│       ├── customers_csv.csv
│       ├── customers_json.json
│       ├── customers_parquet.parquet
│       ├── orders_csv.csv
│       ├── orders_json.json
│       └── orders_parquet.parquet
└── sql_server/
    └── <YYYYMMDDTHHMMSSZ>/
        ├── products_csv.csv
        ├── products_json.json
        ├── products_parquet.parquet
        ├── inventory_csv.csv
        ├── inventory_json.json
        └── inventory_parquet.parquet
```

The generated `data/` directory is runtime output and must be excluded from Git.

## 8. dbt transformation layer

### 8.1 Project configuration

`dbt_enterprise/dbt_project.yml`:
- Project/profile name: `dbt_enterprise`
- Model path: `models`
- Test path: `tests`
- Macro path: `macros`
- Staging models materialize as views in schema `STAGING`.
- Marts materialize as tables in schema `CURATED`.

`dbt_enterprise/macros/generate_schema_name.sql` overrides schema naming so a configured custom schema is used as-is instead of automatically prefixing it with the target schema.

`dbt_enterprise/models/sources/raw_sources.yml` declares the four Snowflake RAW source tables.

### 8.2 Staging models

| Model file | Source | Main transformations |
|---|---|---|
| `models/staging/stg_oracle_customers.sql` | `RAW_ORACLE_CUSTOMERS` | Trims names/contact fields; lowercases email; uppercases city, country, customer type and status; retains CDC columns |
| `models/staging/stg_oracle_orders.sql` | `RAW_ORACLE_ORDERS` | Uppercases order status, payment method and currency; defaults NULL `IS_DELETED` to FALSE |
| `models/staging/stg_sqlserver_products.sql` | `RAW_SQLSERVER_PRODUCTS` | Trims product name/brand; uppercases category/status; defaults NULL `IS_DELETED` to FALSE |
| `models/staging/stg_sqlserver_inventory.sql` | `RAW_SQLSERVER_INVENTORY` | Uppercases warehouse ID; defaults NULL `IS_DELETED` to FALSE |

Staging models are views.

### 8.3 Curated models

| Model file | Materialization | Logic |
|---|---|---|
| `models/marts/dim_customers.sql` | Table | Customer dimension enriched with order count, lifetime spend, average order value, first/last order dates, and customer order category |
| `models/marts/dim_products.sql` | Table | Product dimension enriched with inventory record count, warehouse count, total stock and reorder metrics; excludes deleted products and deleted inventory from its metrics |
| `models/marts/fct_orders.sql` | Table | Order fact joined to `dim_customers` to include customer name/country/type |
| `models/marts/fct_inventory.sql` | Table | Inventory fact joined to `dim_products`, derives `STOCK_STATUS`, and excludes deleted inventory |

**Soft-delete behavior in the actual SQL:** `dim_customers` does not filter deleted customers, which preserves customer records for order context. `dim_products` filters deleted products. `dim_products` inventory metrics and `fct_inventory` filter deleted inventory. `fct_orders` does not filter `O.IS_DELETED` in the supplied SQL.

### 8.4 dbt tests

`models/staging/schema.yml` and `models/marts/schema.yml` define:
- `unique`
- `not_null`
- `relationships`

Tests cover business-key uniqueness and non-null checks for key/name columns, order-to-customer references, inventory-to-product references, and fact-to-dimension relationships.

Run:
```powershell
Set-Location 'E:\Snowflake\snowflake-data-engineering\dbt_enterprise'
& 'C:\snowflake-dbt\.venv\Scripts\python.exe' -m dbt.cli.main build
```

`dbt build` builds models and executes the configured tests.

## 9. Airflow orchestration

DAG file:
```text
airflow/dags/enterprise_multi_source_dag.py
```

DAG ID:
```text
enterprise_multi_source_pipeline
```

Configuration in the uploaded DAG:
- `schedule=None` — manually triggered, not scheduled.
- `start_date`: 2026-01-01, timezone `Asia/Kolkata`.
- `catchup=False`
- `max_active_runs=1`
- Tags: `snowflake`, `oracle`, `sql-server`, `dbt`

The DAG uses three task instances:
1. `run_source` — Oracle source pipeline.
2. `run_source_1` — SQL Server source pipeline.
3. `run_dbt` — Windows PowerShell call that runs `dbt build` in `dbt_enterprise`.

Dependency:
```text
run_source (Oracle) ─┐
                     ├──> run_dbt
run_source_1 (SQL) ──┘
```

The two source tasks are allowed to run in parallel. dbt waits for both upstream tasks to succeed.

The DAG hardcodes these machine-specific paths:
- Project: `/mnt/e/Snowflake/snowflake-data-engineering`
- Source Python: `/home/kk_ubuntu/snowflake-oracle-venv/bin/python`
- dbt project: `E:\Snowflake\snowflake-data-engineering\dbt_enterprise`
- dbt Python: `C:\snowflake-dbt\.venv\Scripts\python.exe`

Update these values if the repository or environments move to another machine.

### Trigger and verify

1. Start Airflow with the command in section 2.3.
2. Open `http://localhost:8080`.
3. Find `enterprise_multi_source_pipeline`.
4. Trigger it manually.
5. Open the DAG run's Graph view.
6. Verify `run_source`, `run_source_1`, and `run_dbt` are all green/Success.

**Completion confirmation:** The user confirmed that all three tasks were successful on the same DAG run. This is the project's recorded end-to-end orchestration outcome.

## 10. Normal operating procedure

### First-time setup / initial snapshot

The orchestrator requires pre-existing RAW rows because it derives watermarks from `MAX(UPDATED_AT)`. Therefore, the initial full snapshot must be loaded before incremental runs. The SQL learning scripts document the initial staged load and validation.

### Subsequent incremental run

Preferred manual source run:
```bash
cd /mnt/e/Snowflake/snowflake-data-engineering
source ~/snowflake-oracle-venv/bin/activate
python run_source_pipeline.py --source oracle
python run_source_pipeline.py --source sql_server
```

Or run the Airflow DAG to execute both source pipelines and dbt in dependency order.

The overlap is five minutes. This intentionally re-reads a small recent time window to reduce the risk of missing timestamp ties. Snowflake MERGE handles already-present keys idempotently when the source timestamp is not older.

### Manual diagnostic commands

```bash
python test_connections.py
python test_snowflake_connection.py
```

Use the test scripts to confirm connectivity before triggering a pipeline if necessary. They are not DAG tasks.

## 11. Data flow and object reference map

| Component | Reads from | Writes to / invokes |
|---|---|---|
| Oracle extractor | `ORACLE_SRC.CSV_*`, `JSON_*`, `PARQUET_*` views | `data/oracle/<run_id>/` |
| SQL Server extractor | `SQLSERVER_SRC.CSV_*`, `JSON_*`, `PARQUET_*` views | `data/sql_server/<run_id>/` |
| `run_source_pipeline.py` | Snowflake RAW `MAX(UPDATED_AT)` | Calls source extractor, uploader, RAW loader |
| Stage uploader | Local run folder, root `.env` | Snowflake internal stage path by source/run ID |
| RAW loader | Stage files + RAW table definitions | Temporary tables, then four RAW tables |
| dbt source YAML | Four RAW tables | dbt source references |
| dbt staging models | dbt sources | Views in `ENTERPRISE_DW.STAGING` |
| dbt marts | Staging models and `ref()` dependencies | Tables in `ENTERPRISE_DW.CURATED` |
| Airflow DAG | Local project path + dbt Windows path | Runs the two source pipelines, then dbt |

## 12. Initial load and validation record

The first full snapshot recorded in the project context contained:
- Oracle customers: 5,000
- Oracle orders: 20,000
- SQL Server products: 500
- SQL Server inventory: 1,500
- Total: 27,000 rows

The stage upload script validates six files per source. The RAW loader uses all three formats and merges by business key. dbt schema tests validate uniqueness, nullability, and relationships.

A later manual CDC exercise recorded:
- Oracle: two customer changes and one order change.
- SQL Server: one product change and one inventory change.

The final Airflow run was confirmed by the user with all three tasks in Success state.

## 13. Git check-in

Before committing:
1. Create `.env` locally from `.env.example`; never add it to Git.
2. Copy the real dbt profile into `%USERPROFILE%\.dbt\profiles.yml`; never add it to Git.
3. Keep generated `data/`, dbt `target/`, Python bytecode, virtual environments, Airflow logs and metadata out of Git.
4. Review the source-bootstrap gaps in `docs/REPOSITORY_AUDIT.md`.
5. Review machine-specific paths in the Airflow DAG.
6. Add the source, SQL, dbt, DAG and documentation files.

Example:
```bash
git init
git status --short
git add .
git status --short
git commit -m "Add enterprise multi-source Snowflake pipeline"
```
