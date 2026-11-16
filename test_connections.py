import os

import oracledb
import pyodbc
from dotenv import load_dotenv

load_dotenv()


def test_oracle():
    print("\nConnecting to Oracle...")

    connection = None

    try:
        connection = oracledb.connect(
            user=os.getenv("ORACLE_USER"),
            password=os.getenv("ORACLE_PASSWORD"),
            dsn=os.getenv("ORACLE_DSN")
        )

        cursor = connection.cursor()

        cursor.execute("""
            SELECT COUNT(*) FROM ORACLE_SRC.CUSTOMERS
        """)
        customer_count = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM ORACLE_SRC.ORDERS
        """)
        order_count = cursor.fetchone()[0]

        print("Oracle connection successful!")
        print(f"CUSTOMERS records: {customer_count}")
        print(f"ORDERS records: {order_count}")

    except Exception as error:
        print("Oracle connection failed:")
        print(error)

    finally:
        if connection:
            connection.close()


def test_sqlserver():
    print("\nConnecting to SQL Server...")

    connection = None

    try:
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

        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        cursor.execute("""
            SELECT COUNT(*) FROM SQLSERVER_SRC.PRODUCTS
        """)
        product_count = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM SQLSERVER_SRC.INVENTORY
        """)
        inventory_count = cursor.fetchone()[0]

        print("SQL Server connection successful!")
        print(f"PRODUCTS records: {product_count}")
        print(f"INVENTORY records: {inventory_count}")

    except Exception as error:
        print("SQL Server connection failed:")
        print(error)

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    test_oracle()
    test_sqlserver()