----Create environement for new project
CREATE DATABASE IF NOT EXISTS ENTERPRISE_DW;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.RAW;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.STAGING;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.CURATED;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.AUDIT;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.UTILS;
CREATE WAREHOUSE IF NOT EXISTS ENTERPRISE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

--Step-2
--first batch of source data from multiple systems.
--Create file formats and the landing stage

--CSV FILE FORMAT
CREATE OR REPLACE FILE FORMAT ENTERPRISE_DW.RAW.FF_ENTERPRISE_CSV
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = TRUE
    NULL_IF = ('NULL', '');

--JSON FILE FORMAT
CREATE OR REPLACE FILE FORMAT ENTERPRISE_DW.RAW.FF_ENTERPRISE_JSON
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE;

--PARQUET FILE FORMAT
CREATE OR REPLACE FILE FORMAT ENTERPRISE_DW.RAW.FF_ENTERPRISE_PARQUET
    TYPE = PARQUET;

--INTERNAL STAGE CREATED
CREATE OR REPLACE STAGE ENTERPRISE_DW.RAW.STG_ENTERPRISE_LANDING
    DIRECTORY = (ENABLE = TRUE);

--validation
SHOW FILE FORMATS IN SCHEMA ENTERPRISE_DW.RAW;

SHOW STAGES IN SCHEMA ENTERPRISE_DW.RAW;




--open New Terminal in VS Code and , excute below comands in VS code. it will create project folders in given drive
/*
New-Item -ItemType Directory -Force -Path `
"E:\Snowflake\snowflake-data-engineering\data\oracle_extracts", `
"E:\Snowflake\snowflake-data-engineering\data\sqlserver_extracts", `
"E:\Snowflake\snowflake-data-engineering\data\external_files", `
"E:\Snowflake\snowflake-data-engineering\ingestion\oracle", `
"E:\Snowflake\snowflake-data-engineering\ingestion\sql_server", `
"E:\Snowflake\snowflake-data-engineering\ingestion\file_ingestion", `
"E:\Snowflake\snowflake-data-engineering\dbt_enterprise"
*/
