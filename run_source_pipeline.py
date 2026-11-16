
import argparse
import os
import re
import subprocess
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
PYTHON = Path.home() / "snowflake-oracle-venv/bin/python"
load_dotenv(PROJECT_ROOT / ".env")

SOURCE_CONFIG = {
    "oracle": {
        "extractor": "ingestion/oracle/extract_oracle.py",
        "tables": {
            "customers": "ENTERPRISE_DW.RAW.RAW_ORACLE_CUSTOMERS",
            "orders": "ENTERPRISE_DW.RAW.RAW_ORACLE_ORDERS",
        },
        "arguments": {
            "customers": "--customers-watermark",
            "orders": "--orders-watermark",
        },
    },
    "sql_server": {
        "extractor": "ingestion/sql_server/extract_sqlserver.py",
        "tables": {
            "products": "ENTERPRISE_DW.RAW.RAW_SQLSERVER_PRODUCTS",
            "inventory": "ENTERPRISE_DW.RAW.RAW_SQLSERVER_INVENTORY",
        },
        "arguments": {
            "products": "--products-watermark",
            "inventory": "--inventory-watermark",
        },
    },
}


def run_command(command):
    result = subprocess.run(
        [str(x) for x in command],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    if result.stdout:
        print(result.stdout, flush=True)
    if result.stderr:
        print(result.stderr, flush=True)

    return result.stdout


def get_watermarks(source):
    config = SOURCE_CONFIG[source]

    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        login_timeout=30,
    )

    watermarks = {}

    try:
        cursor = connection.cursor()

        for entity, table in config["tables"].items():
            cursor.execute(
                f"""
                SELECT TO_VARCHAR(
                    MAX(UPDATED_AT),
                    'YYYY-MM-DD HH24:MI:SS.FF6'
                )
                FROM {table}
                """
            )

            value = cursor.fetchone()[0]

            if value is None:
                raise RuntimeError(
                    f"No existing watermark found for {table}. "
                    "Initialize the RAW snapshot before running CDC."
                )

            watermarks[entity] = value

            print(f"{table} watermark: {value}", flush=True)

    finally:
        connection.close()

    return watermarks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        choices=["oracle", "sql_server"],
        required=True,
    )
    args = parser.parse_args()

    source = args.source
    config = SOURCE_CONFIG[source]

    print(f"\nStarting {source} pipeline", flush=True)

    watermarks = get_watermarks(source)

    extract_command = [
        PYTHON,
        PROJECT_ROOT / config["extractor"],
    ]

    for entity, argument in config["arguments"].items():
        extract_command.extend([argument, watermarks[entity]])

    extract_command.extend(["--overlap-minutes", "5"])

    output = run_command(extract_command)

    match = re.search(r"Run ID:\s*(\S+)", output)

    if not match:
        raise RuntimeError(
            f"Could not capture extraction run ID for {source}"
        )

    run_id = match.group(1)
    print(f"Captured run ID: {run_id}", flush=True)

    run_command([
        PYTHON,
        PROJECT_ROOT / "upload_to_snowflake_stage.py",
        "--source", source,
        "--run-id", run_id,
    ])

    run_command([
        PYTHON,
        PROJECT_ROOT / "load_raw_to_snowflake.py",
        "--source", source,
        "--run-id", run_id,
    ])

    print(f"\n{source} pipeline completed: {run_id}", flush=True)


if __name__ == "__main__":
    main()
