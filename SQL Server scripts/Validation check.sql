-- Check database status
SELECT
    name,
    state_desc,
    user_access_desc
FROM sys.databases
WHERE name = 'ENTERPRISE_SOURCE';

-- Check server login
SELECT
    name,
    is_disabled,
    default_database_name
FROM sys.server_principals
WHERE name = 'ETL_READER';

-- Check database user mapping
USE ENTERPRISE_SOURCE;
GO

SELECT
    dp.name AS DATABASE_USER,
    dp.type_desc,
    dp.authentication_type_desc,
    sp.name AS MAPPED_SERVER_LOGIN
FROM sys.database_principals dp
LEFT JOIN sys.server_principals sp
    ON dp.sid = sp.sid
WHERE dp.name = 'ETL_READER';


USE ENTERPRISE_SOURCE;
GO

SELECT
    dp.name AS DATABASE_USER,
    sp.name AS MAPPED_SERVER_LOGIN
FROM sys.database_principals dp
LEFT JOIN sys.server_principals sp
    ON dp.sid = sp.sid
WHERE dp.name = 'ETL_READER';