# Repository Audit and Reproducibility Notes

This audit is based on the files in the uploaded ZIP. It distinguishes the working pipeline from utilities, historical SQL exercises, and files/configuration that were not supplied.

## 1. Confirmed working flow

The current Airflow DAG calls:
- `run_source_pipeline.py --source oracle`
- `run_source_pipeline.py --source sql_server`
- dbt `build`

The two source tasks are upstream of `run_dbt`. The user confirmed all three Airflow tasks succeeded in the same run.

The current pipeline uses:
- Oracle and SQL Server as relational sources
- Python extraction to CSV, JSON, and Parquet
- Snowflake internal stage uploads via `PUT`
- `COPY INTO` temporary tables
- `MERGE` into Snowflake RAW
- dbt staging views and curated tables
- Airflow orchestration

## 2. Files/configuration absent from the ZIP

The following were not included:
- Root `.env`
- dbt's actual `%USERPROFILE%\.dbt\profiles.yml`
- Python `pip freeze` / exact installed dependency versions
- Any SQL Server/Oracle connection descriptor details beyond the source identifiers in the conversation
- A single canonical Oracle ALTER script for all CDC columns
- A reproducible migration/bootstrap script for the complete final source schema

This repository supplies `.env.example`, `requirements-project.txt`, and `dbt_enterprise/profiles.example.yml` as templates, not as copies of the real local credentials/configuration.

## 3. Oracle source DDL gap

`Oracle Scripts/Oracle_Query_2.sql` creates:
- `CUSTOMERS` with `UPDATED_AT`, but without `IS_DELETED`
- `ORDERS` without `UPDATED_AT` and without `IS_DELETED`

The current `ingestion/oracle/extract_oracle.py` checks that every extracted view contains both `UPDATED_AT` and `IS_DELETED`. `Oracle Scripts/update customers.sql` also updates `IS_DELETED`.

Therefore, the uploaded Oracle DDL set is not a complete clean-room rebuild of the final source schema. The live source used for the successful extraction had the CDC fields, but the ALTER statements that added them are absent from the ZIP. Capture the exact verified DDL from the live `ORACLE_SRC` schema and add it as a new versioned SQL file. Do not guess data types/defaults from the extractor alone.

## 4. Audit utility does not match the current extractor

`audit_extractions.py` expects files such as:
- `customers.csv`
- `customers.json`
- `customers.parquet`

The current Oracle extractor writes:
- `customers_csv.csv`
- `customers_json.json`
- `customers_parquet.parquet`

The SQL Server naming has the same suffix convention. In addition, `audit_extractions.py` compares the file row count with the current full source table count, while the current extractor is incremental and writes only rows at or after the watermark cutoff. These two assumptions make the audit utility unsuitable for the current incremental pipeline without modification.

Do not list `audit_extractions.py` as a successful DAG stage or run it as the current incremental reconciliation. The extractors themselves do compare the three partition counts with the source-table count at the same cutoff.

## 5. Standalone data generator is not part of the final pipeline

`generate_source_data.py` creates older-style CSV files under:
- `data/oracle_extracts/`
- `data/sqlserver_extracts/`

The current extractors instead connect to Oracle and SQL Server and write to:
- `data/oracle/<run_id>/`
- `data/sql_server/<run_id>/`

The generator's column names and schemas do not match the final source tables and RAW loader. It is retained as a standalone earlier exercise, not as an input to the successful Airflow DAG.

## 6. External files are not yet integrated

`ingestion/file_ingestion/` is empty. There is no external CSV/JSON/Parquet ingestion script in the supplied project that is called by the DAG. The current pipeline should be described as Oracle + SQL Server → Snowflake → dbt, not as a completed three-source pipeline with external files.

## 7. Snowflake SQL files are learning scripts, not safe migrations

The `Snowflake Scripts/Project-2_Snowflake_DBT_Airflow_Day-*.sql` files contain:
- Historical run IDs and hardcoded stage paths
- Early file names that do not match the final extractor output names
- Repeated setup statements (Day 5 repeats Day 1 setup)
- `TRUNCATE TABLE` statements
- Manual CDC test and MERGE exercises

They are useful project history and reference material. They are not intended to be executed sequentially as a single deployment script. Review each statement before rerunning it against an existing environment.

## 8. Snowflake watermark audit table is not the active orchestrator watermark

The Day-3 Snowflake SQL creates/initializes `ENTERPRISE_DW.AUDIT.ETL_WATERMARK`. However, `run_source_pipeline.py` reads `MAX(UPDATED_AT)` directly from each RAW table. The audit table is not referenced by the current orchestrator.

## 9. Three-format partition semantics

The source views partition rows among CSV, JSON and Parquet using modulo-three predicates. For a given entity, the three file formats are different row partitions, not three complete copies of the same source rows. The combined three files are intended to cover the entity delta.

The RAW loader prints a staged-row message containing the phrase “including format duplicates”. That log wording is misleading for the current disjoint-view design. The loader also uses `ROW_NUMBER()` by business key, which protects the merge source from duplicate keys if they are present.

## 10. Machine-specific paths

The Airflow DAG has hardcoded paths for:
- WSL project directory
- WSL project Python interpreter
- Windows dbt project directory
- Windows dbt Python interpreter

These are correct for the development workstation recorded in the project documentation. They must be updated for another machine or deployment.

## 11. SQL Server connection setting

The SQL Server extractor hardcodes `ODBC Driver 18 for SQL Server`. The `test_connections.py` and `audit_extractions.py` utilities read `SQLSERVER_DRIVER` from `.env`. Keep the environment value consistent with the installed ODBC driver.

## 12. dbt soft-delete policy is model-specific

The SQL models do not apply one universal soft-delete filter:
- `dim_customers` retains deleted customers.
- `dim_products` filters deleted products and uses only non-deleted inventory for inventory metrics.
- `fct_inventory` filters deleted inventory.
- `fct_orders` does not filter `IS_DELETED` from the order source.

This reflects the uploaded SQL as it stands. Confirm the intended business policy before changing it.

## 13. Generated artifacts removed from the Git-ready package

The uploaded ZIP contained dbt `target/` artifacts and Python `__pycache__` bytecode. They are generated outputs, so they have been removed from the Git-ready package. The dbt project will recreate `target/` when dbt runs.


## 14. Python Snowflake connector warehouse context

The Python connector calls in the orchestrator, uploader, RAW loader, and Snowflake connection test do not pass a `warehouse` argument. They set role, database, and schema. The working environment had a usable default warehouse/session context. For a fresh account/user, make `ENTERPRISE_WH` the user's default warehouse or update the connector calls to set it explicitly.
