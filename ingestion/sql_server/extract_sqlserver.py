
import os
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pyodbc
import pandas as pd
from dotenv import load_dotenv


# ---------------------------------------------------------
# Project configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

OUTPUT_ROOT = PROJECT_ROOT / "data" / "sql_server"

SOURCE_PARTITIONS = {
    "PRODUCTS": {
        "CSV": "SQLSERVER_SRC.CSV_PRODUCTS",
        "JSON": "SQLSERVER_SRC.JSON_PRODUCTS",
        "PARQUET": "SQLSERVER_SRC.PARQUET_PRODUCTS",
    },
    "INVENTORY": {
        "CSV": "SQLSERVER_SRC.CSV_INVENTORY",
        "JSON": "SQLSERVER_SRC.JSON_INVENTORY",
        "PARQUET": "SQLSERVER_SRC.PARQUET_INVENTORY",
    },
}


# ---------------------------------------------------------
# Arguments and watermark handling
# ---------------------------------------------------------

def get_arguments():
    parser = argparse.ArgumentParser(
        description="Incremental SQL Server data extraction"
    )

    parser.add_argument(
        "--products-watermark",
        required=True,
        help="Last successful PRODUCTS watermark",
    )

    parser.add_argument(
        "--inventory-watermark",
        required=True,
        help="Last successful INVENTORY watermark",
    )

    parser.add_argument(
        "--overlap-minutes",
        type=int,
        default=5,
        help="Overlap window to avoid missing timestamp ties",
    )

    return parser.parse_args()


def parse_watermark(value):
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid watermark format: {value}. "
            "Expected YYYY-MM-DD HH:MM:SS.ffffff"
        ) from exc


# ---------------------------------------------------------
# SQL Server connection
# ---------------------------------------------------------

def get_connection():

    required_variables = [
        "SQLSERVER_SERVER",
        "SQLSERVER_DATABASE",
        "SQLSERVER_USER",
        "SQLSERVER_PASSWORD",
    ]

    missing = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing:
        raise RuntimeError(
            f"Missing SQL Server environment variables: {missing}"
        )

    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.getenv('SQLSERVER_SERVER')};"
        f"DATABASE={os.getenv('SQLSERVER_DATABASE')};"
        f"UID={os.getenv('SQLSERVER_USER')};"
        f"PWD={os.getenv('SQLSERVER_PASSWORD')};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=15;"
    )

    return pyodbc.connect(connection_string)


# ---------------------------------------------------------
# Extract one partition incrementally
# ---------------------------------------------------------

def extract_partition(
    connection,
    entity,
    file_type,
    view_name,
    output_dir,
    cutoff,
):

    print(f"\nExtracting {view_name}...")
    print(f"UPDATED_AT >= {cutoff}")

    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {view_name}
            WHERE UPDATED_AT >= ?
            """,
            cutoff,
        )
        expected_count = cursor.fetchone()[0]

        cursor.execute(
            f"""
            SELECT *
            FROM {view_name}
            WHERE UPDATED_AT >= ?
            ORDER BY 1
            """,
            cutoff,
        )

        columns = [
            column[0] for column in cursor.description
        ]

        required_cdc_columns = {
            "UPDATED_AT",
            "IS_DELETED",
        }

        missing_columns = (
            required_cdc_columns - set(columns)
        )

        if missing_columns:
            raise RuntimeError(
                f"{view_name} is missing CDC columns: "
                f"{missing_columns}"
            )

        records = [
            tuple(row) for row in cursor.fetchall()
        ]

    finally:
        cursor.close()

    dataframe = pd.DataFrame(
        records,
        columns=columns,
    )

    actual_count = len(dataframe)

    if actual_count != expected_count:
        raise RuntimeError(
            f"Row count mismatch for {view_name}: "
            f"expected={expected_count}, actual={actual_count}"
        )

    if not dataframe.empty:
        if dataframe["UPDATED_AT"].isna().any():
            raise RuntimeError(
                f"NULL UPDATED_AT found in {view_name}"
            )

        if dataframe["IS_DELETED"].isna().any():
            raise RuntimeError(
                f"NULL IS_DELETED found in {view_name}"
            )

    output_file = (
        output_dir
        / f"{entity.lower()}_{file_type.lower()}"
    )

    if file_type == "CSV":
        dataframe.to_csv(
            f"{output_file}.csv",
            index=False,
        )

    elif file_type == "JSON":
        dataframe.to_json(
            f"{output_file}.json",
            orient="records",
            date_format="iso",
        )

    elif file_type == "PARQUET":
        dataframe.to_parquet(
            f"{output_file}.parquet",
            index=False,
        )

    print(
        f"Completed: {view_name} | "
        f"Changed rows: {actual_count} | "
        f"File type: {file_type}"
    )

    return actual_count


# ---------------------------------------------------------
# Validate incremental partition counts
# ---------------------------------------------------------

def validate_entity_total(
    connection,
    entity,
    partition_counts,
    cutoff,
):

    total_partition_count = sum(
        partition_counts.values()
    )

    base_table = f"SQLSERVER_SRC.{entity}"

    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {base_table}
            WHERE UPDATED_AT >= ?
            """,
            cutoff,
        )
        base_count = cursor.fetchone()[0]

    finally:
        cursor.close()

    if total_partition_count != base_count:
        raise RuntimeError(
            f"Incremental reconciliation failed for {entity}: "
            f"partition total={total_partition_count}, "
            f"base table total={base_count}"
        )

    print(
        f"\n{entity} incremental reconciliation PASSED: "
        f"{total_partition_count} rows"
    )

    return total_partition_count


# ---------------------------------------------------------
# Main extraction process
# ---------------------------------------------------------

def main():

    args = get_arguments()

    watermarks = {
        "PRODUCTS": parse_watermark(
            args.products_watermark
        ),
        "INVENTORY": parse_watermark(
            args.inventory_watermark
        ),
    }

    if args.overlap_minutes < 0:
        raise ValueError(
            "Overlap minutes cannot be negative"
        )

    cutoffs = {
        entity: watermark - timedelta(
            minutes=args.overlap_minutes
        )
        for entity, watermark in watermarks.items()
    }

    run_id = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")

    output_dir = OUTPUT_ROOT / run_id
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("=" * 65)
    print("SQL SERVER INCREMENTAL DATA EXTRACTION")
    print(f"Run ID: {run_id}")
    print(f"Output directory: {output_dir}")
    print(f"Overlap: {args.overlap_minutes} minutes")
    print("=" * 65)

    for entity, cutoff in cutoffs.items():
        print(f"{entity} cutoff: {cutoff}")

    connection = None

    try:
        connection = get_connection()

        total_rows = 0

        for entity, partitions in SOURCE_PARTITIONS.items():

            print(f"\n{'-' * 55}")
            print(f"ENTITY: {entity}")
            print(f"{'-' * 55}")

            partition_counts = {}

            for file_type, view_name in partitions.items():

                count = extract_partition(
                    connection=connection,
                    entity=entity,
                    file_type=file_type,
                    view_name=view_name,
                    output_dir=output_dir,
                    cutoff=cutoffs[entity],
                )

                partition_counts[file_type] = count

            entity_total = validate_entity_total(
                connection=connection,
                entity=entity,
                partition_counts=partition_counts,
                cutoff=cutoffs[entity],
            )

            total_rows += entity_total

        print("\n" + "=" * 65)
        print("SQL SERVER INCREMENTAL EXTRACTION COMPLETED")
        print(f"Total rows extracted: {total_rows}")
        print("Total files created: 6")
        print(f"Output directory: {output_dir}")
        print("=" * 65)

    except Exception:
        print("\nSQL SERVER INCREMENTAL EXTRACTION FAILED.")
        raise

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
