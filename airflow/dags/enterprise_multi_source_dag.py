import subprocess
import pendulum
from airflow.sdk import dag, task

PROJECT_DIR = "/mnt/e/Snowflake/snowflake-data-engineering"
PROJECT_PYTHON = "/home/kk_ubuntu/snowflake-oracle-venv/bin/python"

@dag(
    dag_id="enterprise_multi_source_pipeline",
    description="Oracle and SQL Server to Snowflake RAW and dbt CURATED",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "oracle", "sql-server", "dbt"],
)
def enterprise_multi_source_pipeline():

    @task
    def run_source(source):
        subprocess.run(
            [
                PROJECT_PYTHON,
                f"{PROJECT_DIR}/run_source_pipeline.py",
                "--source",
                source,
            ],
            cwd=PROJECT_DIR,
            check=True,
        )

    @task
    def run_dbt():
        powershell_command = (
            "$ErrorActionPreference = 'Stop'; "
            "Set-Location 'E:\\Snowflake\\snowflake-data-engineering\\dbt_enterprise'; "
            "& 'C:\\snowflake-dbt\\.venv\\Scripts\\python.exe' "
            "-m dbt.cli.main build; "
            "if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }"
        )
        subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                powershell_command,
            ],
            check=True,
        )

    oracle = run_source("oracle")
    sql_server = run_source("sql_server")
    dbt = run_dbt()

    [oracle, sql_server] >> dbt


enterprise_multi_source_pipeline()
