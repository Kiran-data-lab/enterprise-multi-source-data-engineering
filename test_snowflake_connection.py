
import os
import snowflake.connector
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

connection = None

try:
    print("=" * 60)
    print("SNOWFLAKE CONNECTION TEST")
    print("=" * 60)

    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        login_timeout=30
    )

    print("Connected to Snowflake successfully!")

    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT
                CURRENT_ACCOUNT() AS ACCOUNT_NAME,
                CURRENT_USER() AS USER_NAME,
                CURRENT_ROLE() AS ROLE_NAME,
                CURRENT_DATABASE() AS DATABASE_NAME,
                CURRENT_SCHEMA() AS SCHEMA_NAME
        """)

        result = cursor.fetchone()

        print("\nConnection details:")
        print(f"Account  : {result[0]}")
        print(f"User     : {result[1]}")
        print(f"Role     : {result[2]}")
        print(f"Database : {result[3]}")
        print(f"Schema   : {result[4]}")

    finally:
        cursor.close()

except Exception as error:
    print("\nSNOWFLAKE CONNECTION FAILED")
    print(f"Error: {error}")
    raise

finally:
    if connection:
        connection.close()
        print("\nSnowflake connection closed.")
