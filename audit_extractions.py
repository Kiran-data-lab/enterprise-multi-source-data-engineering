
import os
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import oracledb
import pyodbc
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

FORMATS = ["csv", "json", "parquet"]

TABLES = {
    "ORACLE": {
        "output_folder": "oracle",
        "tables": ["CUSTOMERS", "ORDERS"]
    },
    "SQL_SERVER": {
        "output_folder": "sql_server",
        "tables": ["PRODUCTS", "INVENTORY"]
    }
}


def latest_run_folder(source_folder):

    parent = PROJECT_ROOT / "data" / source_folder

    run_folders = sorted(
        path for path in parent.iterdir()
        if path.is_dir()
    )

    if not run_folders:
        raise FileNotFoundError(
            f"No extraction folders found under {parent}"
        )

    return run_folders[-1]


def get_oracle_connection():

    return oracledb.connect(
        user=os.getenv("ORACLE_USER"),
        password=os.getenv("ORACLE_PASSWORD"),
        dsn=os.getenv("ORACLE_DSN")
    )


def get_sqlserver_connection():

    connection_string = (
        f"DRIVER={{{os.getenv('SQLSERVER_DRIVER')}}};"
        f"SERVER={os.getenv('SQLSERVER_SERVER')};"
        f"DATABASE={os.getenv('SQLSERVER_DATABASE')};"
        f"UID={os.getenv('SQLSERVER_USER')};"
        f"PWD={os.getenv('SQLSERVER_PASSWORD')};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=15;"
    )

    return pyodbc.connect(connection_string)


def get_source_count(connection, source, table):

    if source == "ORACLE":
        query = (
            f"SELECT COUNT(*) "
            f"FROM ORACLE_SRC.{table}"
        )
    else:
        query = (
            f"SELECT COUNT(*) "
            f"FROM SQLSERVER_SRC.{table}"
        )

    cursor = connection.cursor()

    try:
        cursor.execute(query)
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def get_file_count(file_path, file_format):

    if file_format == "csv":
        dataframe = pd.read_csv(file_path)

    elif file_format == "json":
        dataframe = pd.read_json(file_path, orient="records")

    elif file_format == "parquet":
        dataframe = pd.read_parquet(file_path)

    else:
        raise ValueError(
            f"Unsupported format: {file_format}"
        )

    return len(dataframe)


def main():

    print("=" * 65)
    print("MULTI-SOURCE EXTRACTION RECONCILIATION STARTED")
    print("=" * 65)

    connections = {}
    audit_results = []

    try:
        connections["ORACLE"] = get_oracle_connection()
        connections["SQL_SERVER"] = get_sqlserver_connection()

        for source, config in TABLES.items():

            run_folder = latest_run_folder(
                config["output_folder"]
            )

            print(f"\nSource: {source}")
            print(f"Extraction folder: {run_folder}")

            for table in config["tables"]:

                source_count = get_source_count(
                    connections[source],
                    source,
                    table
                )

                print(f"\nTable: {table}")
                print(f"Current source count: {source_count}")

                for file_format in FORMATS:

                    file_path = (
                        run_folder /
                        f"{table.lower()}.{file_format}"
                    )

                    if not file_path.exists():

                        file_count = None
                        status = "FAIL"

                    else:

                        file_count = get_file_count(
                            file_path,
                            file_format
                        )

                        status = (
                            "PASS"
                            if file_count == source_count
                            else "FAIL"
                        )

                    audit_results.append({
                        "SOURCE": source,
                        "TABLE_NAME": table,
                        "FILE_FORMAT": file_format.upper(),
                        "SOURCE_COUNT": source_count,
                        "FILE_COUNT": file_count,
                        "STATUS": status,
                        "FILE_PATH": str(file_path)
                    })

                    print(
                        f"{file_format.upper():10} "
                        f"File count: {file_count} "
                        f"Status: {status}"
                    )

        # Create audit output
        audit_df = pd.DataFrame(audit_results)

        audit_run_id = datetime.now(
            timezone.utc
        ).strftime("%Y%m%dT%H%M%SZ")

        audit_dir = (
            PROJECT_ROOT / "data" / "audit" / audit_run_id
        )

        audit_dir.mkdir(parents=True, exist_ok=True)

        audit_df.to_csv(
            audit_dir / "extraction_audit.csv",
            index=False
        )

        audit_df.to_json(
            audit_dir / "extraction_audit.json",
            orient="records",
            indent=2
        )

        passed = (audit_df["STATUS"] == "PASS").sum()
        failed = (audit_df["STATUS"] == "FAIL").sum()

        print("\n" + "=" * 65)
        print("RECONCILIATION SUMMARY")
        print(f"Files checked : {len(audit_df)}")
        print(f"Passed        : {passed}")
        print(f"Failed        : {failed}")
        print(f"Audit output  : {audit_dir}")
        print("=" * 65)

        if failed:
            raise RuntimeError(
                f"Reconciliation failed for {failed} file(s)."
            )

        print("ALL EXTRACTION FILES PASSED RECONCILIATION.")

    finally:
        for connection in connections.values():
            connection.close()

        print("Database connections closed.")


if __name__ == "__main__":
    main()
