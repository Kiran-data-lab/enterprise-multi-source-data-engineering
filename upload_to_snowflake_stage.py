import argparse
import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"

STAGE_NAME = "ENTERPRISE_DW.RAW.STG_ENTERPRISE_LANDING"

SOURCE_FOLDERS = ["oracle", "sql_server"]

EXPECTED_FILES = {
    "oracle": {
        "customers_csv.csv",
        "customers_json.json",
        "customers_parquet.parquet",
        "orders_csv.csv",
        "orders_json.json",
        "orders_parquet.parquet"
    },
    "sql_server": {
        "products_csv.csv",
        "products_json.json",
        "products_parquet.parquet",
        "inventory_csv.csv",
        "inventory_json.json",
        "inventory_parquet.parquet"
    }
}

load_dotenv(PROJECT_ROOT / ".env")


# --------------------------------------------------
# 1. Identify latest extraction folder per source
# --------------------------------------------------

#
def get_latest_extraction_folder(source_folder, run_id=None):

    source_path = DATA_ROOT / source_folder

    if run_id:
        selected_folder = source_path / run_id

        if not selected_folder.is_dir():
            raise FileNotFoundError(
                f"Extraction folder not found: {selected_folder}"
            )

        latest_folder = selected_folder

    else:
        run_folders = sorted(
            [
                folder for folder in source_path.iterdir()
                if folder.is_dir()
                and len(folder.name) == 16
                and folder.name.endswith("Z")
                and folder.name[:8].isdigit()
            ],
            key=lambda folder: folder.name,
            reverse=True
        )

        if not run_folders:
            raise FileNotFoundError(
                f"No timestamped extraction folders found: {source_path}"
            )

        latest_folder = run_folders[0]

    actual_files = {
        file.name.lower()
        for file in latest_folder.iterdir()
        if file.is_file()
    }

    expected_files = EXPECTED_FILES[source_folder]

    if actual_files != expected_files:
        raise RuntimeError(
            f"File validation failed for {latest_folder}\n"
            f"Expected: {sorted(expected_files)}\n"
            f"Found: {sorted(actual_files)}"
        )

    print(
        f"Selected {source_folder} extraction: "
        f"{latest_folder.name}"
    )

    return latest_folder
#
# """
# #def get_latest_extraction_folder(source_folder):
# def get_latest_extraction_folder(source_folder, run_id=None):

#     source_path = DATA_ROOT / source_folder

#     #added to select one source and one specific run
#         if run_id:
#         selected_folder = source_path / run_id

#         if not selected_folder.is_dir():
#             raise FileNotFoundError(
#                 f"Extraction folder not found: {selected_folder}"
#             )

#         run_folders = [selected_folder]
#     else:
#         run_folders = sorted(
#             [
#                 folder for folder in source_path.iterdir()
#                 if folder.is_dir()
#                 and len(folder.name) == 16
#                 and folder.name.endswith("Z")
#                 and folder.name[:8].isdigit()
#             ],
#             key=lambda folder: folder.name,
#             reverse=True
#         )
#     #added to select one source and one specific run 

#     #run_folders = sorted(
#     #   [
#     #       folder for folder in source_path.iterdir()
#     #      if folder.is_dir()
#     #       and len(folder.name) == 16
#     #       and folder.name.endswith("Z")
#     #       and folder.name[:8].isdigit()
#     #   ],
#     #   key=lambda folder: folder.name,
#     #   reverse=True
#     #)

#     if not run_folders:
#         raise FileNotFoundError(
#             f"No timestamped extraction folders found: {source_path}"
#         )

#     latest_folder = run_folders[0]

#     actual_files = {
#         file.name.lower()
#         for file in latest_folder.iterdir()
#         if file.is_file()
#     }

#     expected_files = EXPECTED_FILES[source_folder]

#     if actual_files != expected_files:
#         raise RuntimeError(
#             f"File validation failed for {latest_folder}\n"
#             f"Expected: {sorted(expected_files)}\n"
#             f"Found: {sorted(actual_files)}"
#         )

#     print(
#         f"Latest {source_folder} extraction: "
#         f"{latest_folder.name}"
#     )

#     return latest_folder
# """

# --------------------------------------------------
# 2. Main upload process
# --------------------------------------------------

def main():

    #to run single source to single run
        #parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source",
        choices=["oracle", "sql_server", "all"],
        default="all"
    )

    parser.add_argument("--run-id")

    args = parser.parse_args()

    if args.run_id and args.source == "all":
        parser.error("--run-id requires --source")
    #to run single source to single run

    connection = None
    cursor = None

    try:
        print("=" * 65)
        print("SNOWFLAKE INTERNAL STAGE UPLOAD STARTED")
        print("=" * 65)

        connection = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            login_timeout=30
        )

        cursor = connection.cursor()

        print("Connected to Snowflake successfully.")

        # Discover only the latest six files per source
        files = []

        #for source_folder in SOURCE_FOLDERS:

        #   latest_folder = get_latest_extraction_folder(
        #       source_folder
        #   )

        #to single source for single run
        selected_sources = (
            SOURCE_FOLDERS if args.source == "all"
            else [args.source]
        )

        for source_folder in selected_sources:

            latest_folder = get_latest_extraction_folder(
                source_folder,
                args.run_id
            )
        #to single source for single run

            files.extend(
                sorted(
                    latest_folder.iterdir(),
                    key=lambda file: file.name
                )
            )

        #if len(files) != 12:
        #   raise RuntimeError(
        #       f"Expected exactly 12 files, found {len(files)}"
        #   )

        #
        expected_count = 6 * len(selected_sources)

        if len(files) != expected_count:
            raise RuntimeError(
                f"Expected {expected_count} files, found {len(files)}"
            )
        #

        print(f"\nFiles selected: {len(files)}")
        print(f"Target stage: {STAGE_NAME}")

        successful = 0
        failed = 0

        # Upload files to Snowflake internal stage
        for file_path in files:

            relative_path = file_path.relative_to(DATA_ROOT)
            stage_folder = relative_path.parent.as_posix()

            local_uri = file_path.as_posix()

            put_command = (
                f"PUT 'file://{local_uri}' "
                f"@{STAGE_NAME}/{stage_folder}/ "
                "AUTO_COMPRESS=FALSE "
                "OVERWRITE=TRUE"
            )

            print(f"\nUploading: {relative_path.as_posix()}")

            try:
                cursor.execute(put_command)
                results = cursor.fetchall()

                for result in results:

                    status = result[6]

                    print(f"  Status: {status}")

                    if status in ("UPLOADED", "SKIPPED"):
                        successful += 1
                    else:
                        failed += 1
                        print(f"  Details: {result}")

            except Exception as error:
                failed += 1
                print(f"  UPLOAD FAILED: {error}")

        # Upload summary
        print("\n" + "=" * 65)
        print("UPLOAD SUMMARY")
        print("=" * 65)
        print(f"Files selected  : {len(files)}")
        print(f"Successful      : {successful}")
        print(f"Failed          : {failed}")
        print(f"Target stage    : {STAGE_NAME}")
        print("=" * 65)

        if failed or successful != expected_count:
            raise RuntimeError(
                "One or more files failed to upload."
            )

        print(f"ALL {expected_count} EXTRACTION FILES UPLOADED SUCCESSFULLY.")
        # if failed or successful != 12:
        #     raise RuntimeError(
        #         "One or more files failed to upload."
        #     )

        # print("ALL 12 EXTRACTION FILES UPLOADED SUCCESSFULLY.")

    finally:

        if cursor:
            cursor.close()

        if connection:
            connection.close()
            print("\nSnowflake connection closed.")


if __name__ == "__main__":
    main()
