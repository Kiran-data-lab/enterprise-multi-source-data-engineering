
import argparse
import os
import re
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

DATABASE = "ENTERPRISE_DW"
SCHEMA = "RAW"
STAGE = f"{DATABASE}.{SCHEMA}.STG_ENTERPRISE_LANDING"

FILE_FORMATS = {
    "csv": f"{DATABASE}.{SCHEMA}.FF_ENTERPRISE_CSV_INFER",
    "json": f"{DATABASE}.{SCHEMA}.FF_ENTERPRISE_JSON_ARRAY",
    "parquet": f"{DATABASE}.{SCHEMA}.FF_ENTERPRISE_PARQUET_INFER",
}

ENTITIES = {
    "oracle": {
        "customers": {
            "table": "RAW_ORACLE_CUSTOMERS",
            "key": "CUSTOMER_ID",
            "columns": [
                "CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL",
                "PHONE", "CITY", "COUNTRY", "CUSTOMER_TYPE",
                "REGISTRATION_DATE", "CUSTOMER_STATUS",
                "CREATED_AT", "UPDATED_AT", "IS_DELETED",
            ],
        },
        "orders": {
            "table": "RAW_ORACLE_ORDERS",
            "key": "ORDER_ID",
            "columns": [
                "ORDER_ID", "CUSTOMER_ID", "ORDER_DATE",
                "ORDER_AMOUNT", "ORDER_STATUS", "PAYMENT_METHOD",
                "CURRENCY_CODE", "CREATED_AT", "UPDATED_AT",
                "IS_DELETED",
            ],
        },
    },
    "sql_server": {
        "products": {
            "table": "RAW_SQLSERVER_PRODUCTS",
            "key": "PRODUCT_ID",
            "columns": [
                "PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "BRAND",
                "UNIT_PRICE", "PRODUCT_STATUS", "LAUNCH_DATE",
                "CREATED_AT", "UPDATED_AT", "IS_DELETED",
            ],
        },
        "inventory": {
            "table": "RAW_SQLSERVER_INVENTORY",
            "key": "INVENTORY_ID",
            "columns": [
                "INVENTORY_ID", "PRODUCT_ID", "WAREHOUSE_ID",
                "QUANTITY_ON_HAND", "REORDER_LEVEL",
                "LAST_STOCK_UPDATE", "UPDATED_AT", "IS_DELETED",
            ],
        },
    },
}


def merge_entity(cursor, source, run_id, entity, config):
    target = f"{DATABASE}.{SCHEMA}.{config['table']}"
    temp = f"TMP_LOAD_{config['table']}"
    key = config["key"]
    columns = config["columns"]

    stage_path = f"{STAGE}/{source}/{run_id}"

    print(f"\nLoading {source}.{entity} into {target}")

    cursor.execute(
        f"CREATE OR REPLACE TEMP TABLE {temp} LIKE {target}"
    )

    for file_type in ("csv", "json", "parquet"):
        filename = f"{entity}_{file_type}.{file_type}"
        staged_file = f"@{stage_path}/{filename}"
        file_format = FILE_FORMATS[file_type]

        copy_sql = f"""
            COPY INTO {temp}
            FROM {staged_file}
            FILE_FORMAT = (FORMAT_NAME = '{file_format}')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'ABORT_STATEMENT'
        """

        cursor.execute(copy_sql)

        result = cursor.fetchone()
        print(
            f"  {filename}: "
            f"{result[1] if result else 'COPY completed'}"
        )

    column_list = ", ".join(columns)
    update_list = ",\n".join(
        f"T.{col} = S.{col}"
        for col in columns
        if col != key
    )
    insert_values = ", ".join(f"S.{col}" for col in columns)

    merge_sql = f"""
        MERGE INTO {target} T
        USING (
            SELECT *
            FROM {temp}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {key}
                ORDER BY UPDATED_AT DESC NULLS LAST
            ) = 1
        ) S
        ON T.{key} = S.{key}

        WHEN MATCHED
             AND (
                 T.UPDATED_AT IS NULL
                 OR S.UPDATED_AT >= T.UPDATED_AT
             )
        THEN UPDATE SET
            {update_list}

        WHEN NOT MATCHED THEN INSERT (
            {column_list}
        )
        VALUES (
            {insert_values}
        )
    """

    cursor.execute(merge_sql)
    print(f"  MERGE completed for {config['table']}")

    cursor.execute(f"SELECT COUNT(*) FROM {temp}")
    staged_count = cursor.fetchone()[0]

    print(f"  Rows staged (including format duplicates): {staged_count}")


def main():
    parser = argparse.ArgumentParser(
        description="Load Oracle or SQL Server files into Snowflake RAW."
    )

    parser.add_argument(
        "--source",
        choices=["oracle", "sql_server"],
        required=True,
    )
    parser.add_argument("--run-id", required=True)

    args = parser.parse_args()

    if not re.fullmatch(r"\d{8}T\d{6}Z", args.run_id):
        parser.error("Invalid run ID format.")

    connection = None

    try:
        connection = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            login_timeout=30,
        )

        cursor = connection.cursor()

        for entity, config in ENTITIES[args.source].items():
            merge_entity(
                cursor,
                args.source,
                args.run_id,
                entity,
                config,
            )

        connection.commit()
        print(f"\nRAW loading completed: {args.source}, {args.run_id}")

    except Exception:
        if connection:
            connection.rollback()
        raise

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
