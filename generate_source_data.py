
import csv
import random
from datetime import date, timedelta
from pathlib import Path

# All project data is stored on E: drive.
BASE_DIR = Path(r"E:\Snowflake\snowflake-data-engineering")

ORACLE_DIR = BASE_DIR / "data" / "oracle_extracts"
SQLSERVER_DIR = BASE_DIR / "data" / "sqlserver_extracts"

random.seed(42)

def random_date(start_date, end_date):
    days = (end_date - start_date).days
    return start_date + timedelta(days=random.randint(0, days))


def write_csv(file_path, columns, rows):
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with file_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Created: {file_path} | Rows: {len(rows):,}")


# --------------------------------------------------
# 1. ORACLE SOURCE: CUSTOMERS
# --------------------------------------------------

first_names = [
    "Kiran", "Rao", "Anita", "Sravani", "John",
    "David", "Priya", "Michael", "Sarah", "Arun"
]

last_names = [
    "Kumar", "Sharma", "Reddy", "Mathew", "Smith",
    "Wilson", "Patel", "Thomas", "Rao", "Miller"
]

countries = [
    "India", "Germany", "Netherlands",
    "Luxembourg", "France", "United Kingdom"
]

customers = []

for customer_id in range(100001, 105001):
    first = random.choice(first_names)
    last = random.choice(last_names)

    customers.append({
        "CUSTOMER_ID": customer_id,
        "CUSTOMER_NAME": f"{first} {last}",
        "EMAIL": f"{first.lower()}.{last.lower()}.{customer_id}@example.com",
        "COUNTRY": random.choice(countries),
        "CREATED_DATE": random_date(
            date(2022, 1, 1), date(2026, 10, 31)
        ).isoformat()
    })

write_csv(
    ORACLE_DIR / "customers.csv",
    ["CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL", "COUNTRY", "CREATED_DATE"],
    customers
)


# --------------------------------------------------
# 2. ORACLE SOURCE: ORDERS
# --------------------------------------------------

orders = []

for order_id in range(500001, 520001):
    order_date = random_date(
        date(2024, 1, 1), date(2026, 10, 31)
    )

    orders.append({
        "ORDER_ID": order_id,
        "CUSTOMER_ID": random.choice(customers)["CUSTOMER_ID"],
        "ORDER_DATE": order_date.isoformat(),
        "ORDER_STATUS": random.choice(
            ["NEW", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
        ),
        "ORDER_AMOUNT": round(random.uniform(100, 50000), 2)
    })

write_csv(
    ORACLE_DIR / "orders.csv",
    ["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE",
     "ORDER_STATUS", "ORDER_AMOUNT"],
    orders
)


# --------------------------------------------------
# 3. SQL SERVER SOURCE: PRODUCTS
# --------------------------------------------------

categories = [
    "Electronics", "Furniture", "Clothing",
    "Home Appliances", "Accessories"
]

products = []

for product_id in range(2001, 2501):
    category = random.choice(categories)

    products.append({
        "PRODUCT_ID": product_id,
        "PRODUCT_NAME": f"{category} Product {product_id}",
        "CATEGORY": category,
        "UNIT_PRICE": round(random.uniform(50, 100000), 2),
        "IS_ACTIVE": random.choice(["Y", "Y", "Y", "N"])
    })

write_csv(
    SQLSERVER_DIR / "products.csv",
    ["PRODUCT_ID", "PRODUCT_NAME", "CATEGORY",
     "UNIT_PRICE", "IS_ACTIVE"],
    products
)


# --------------------------------------------------
# 4. SQL SERVER SOURCE: INVENTORY
# --------------------------------------------------

inventory = []

for product in products:
    inventory.append({
        "INVENTORY_ID": len(inventory) + 1,
        "PRODUCT_ID": product["PRODUCT_ID"],
        "WAREHOUSE_CODE": random.choice(
            ["HYD01", "BLR01", "DEL01", "MUM01"]
        ),
        "AVAILABLE_QUANTITY": random.randint(0, 5000),
        "LAST_UPDATED": random_date(
            date(2026, 1, 1), date(2026, 10, 31)
        ).isoformat()
    })

write_csv(
    SQLSERVER_DIR / "inventory.csv",
    ["INVENTORY_ID", "PRODUCT_ID", "WAREHOUSE_CODE",
     "AVAILABLE_QUANTITY", "LAST_UPDATED"],
    inventory
)


print("\nSource data generation completed successfully.")
print("Oracle: 5,000 customers and 20,000 orders")
print("SQL Server: 500 products and 500 inventory records")
