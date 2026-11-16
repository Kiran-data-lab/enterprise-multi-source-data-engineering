
import os
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta

import oracledb
import pandas as pd
from dotenv import load_dotenv


# ---------------------------------------------------------
# Project configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

OUTPUT_ROOT = PROJECT_ROOT / "data" / "oracle"

SOURCE_PARTITIONS = {
    "CUSTOMERS": {
        "CSV": "ORACLE_SRC.CSV_CUSTOMERS",
        "JSON": "ORACLE_SRC.JSON_CUSTOMERS",
        "PARQUET": "ORACLE_SRC.PARQUET_CUSTOMERS",
    },
    "ORDERS": {
        "CSV": "ORACLE_SRC.CSV_ORDERS",
        "JSON": "ORACLE_SRC.JSON_ORDERS",
        "PARQUET": "ORACLE_SRC.PARQUET_ORDERS",
    },
}


# ---------------------------------------------------------
# Arguments and watermark handling
# ---------------------------------------------------------

def get_arguments():
    parser = argparse.ArgumentParser(
        description="Incremental Oracle data extraction"
    )

    parser.add_argument(
        "--customers-watermark",
        required=True,
        help="Last successful CUSTOMERS watermark",
    )

    parser.add_argument(
        "--orders-watermark",
        required=True,
        help="Last successful ORDERS watermark",
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
# Oracle connection
# ---------------------------------------------------------

def get_connection():
    required_variables = [
        "ORACLE_USER",
        "ORACLE_PASSWORD",
        "ORACLE_DSN",
    ]

    missing = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing:
        raise RuntimeError(
            f"Missing Oracle environment variables: {missing}"
        )

    return oracledb.connect(
        user=os.getenv("ORACLE_USER"),
        password=os.getenv("ORACLE_PASSWORD"),
        dsn=os.getenv("ORACLE_DSN"),
    )


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

    query = f"""
        SELECT *
        FROM {view_name}
        WHERE UPDATED_AT >= :cutoff
        ORDER BY 1
    """

    # -----------------------------------------------------
    # Step 1: Get expected count using a dedicated cursor
    # -----------------------------------------------------

    with connection.cursor() as count_cursor:

        count_cursor.setinputsizes(
            cutoff=oracledb.DB_TYPE_TIMESTAMP
        )

        count_cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {view_name}
            WHERE UPDATED_AT >= :cutoff
            """,
            cutoff=cutoff,
        )

        expected_count = count_cursor.fetchone()[0]

    # -----------------------------------------------------
    # Step 2: Fetch actual records using another cursor
    # -----------------------------------------------------

    with connection.cursor() as data_cursor:

        data_cursor.setinputsizes(
            cutoff=oracledb.DB_TYPE_TIMESTAMP
        )

        data_cursor.execute(
            query,
            cutoff=cutoff,
        )

        columns = [
            column[0]
            for column in data_cursor.description
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

        records = data_cursor.fetchall()

    # -----------------------------------------------------
    # Step 3: Convert records to DataFrame
    # -----------------------------------------------------

    dataframe = pd.DataFrame(
        records,
        columns=columns,
    )

    actual_count = len(dataframe)

    # -----------------------------------------------------
    # Step 4: Validate count
    # -----------------------------------------------------

    if actual_count != expected_count:
        raise RuntimeError(
            f"Row count mismatch for {view_name}: "
            f"expected={expected_count}, actual={actual_count}"
        )

    # -----------------------------------------------------
    # Step 5: Validate CDC columns
    # -----------------------------------------------------

    if not dataframe.empty:

        if dataframe["UPDATED_AT"].isna().any():
            raise RuntimeError(
                f"NULL UPDATED_AT found in {view_name}"
            )

        if dataframe["IS_DELETED"].isna().any():
            raise RuntimeError(
                f"NULL IS_DELETED found in {view_name}"
            )

    # -----------------------------------------------------
    # Step 6: Write output file
    # -----------------------------------------------------

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

    base_table = f"ORACLE_SRC.{entity}"

    with connection.cursor() as cursor:

        cursor.setinputsizes(
            cutoff=oracledb.DB_TYPE_TIMESTAMP
        )

        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {base_table}
            WHERE UPDATED_AT >= :cutoff
            """,
            cutoff=cutoff,
        )

        base_count = cursor.fetchone()[0]

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
        "CUSTOMERS": parse_watermark(
            args.customers_watermark
        ),
        "ORDERS": parse_watermark(
            args.orders_watermark
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
    print("ORACLE INCREMENTAL DATA EXTRACTION")
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
        print("ORACLE INCREMENTAL EXTRACTION COMPLETED")
        print(f"Total rows extracted: {total_rows}")
        print("Total files created: 6")
        print(f"Output directory: {output_dir}")
        print("=" * 65)

    except Exception:

        print("\nORACLE INCREMENTAL EXTRACTION FAILED.")
        raise

    finally:

        if connection:
            connection.close()


if __name__ == "__main__":
    main()
