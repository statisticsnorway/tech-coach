from pathlib import Path

import duckdb
import pandas as pd


conn = duckdb.connect()


def create_table(name: str) -> None:
    data_dir = Path(__file__).parent / "dataset"
    file_path = str(data_dir / f"{name}.parquet")
    conn.execute(
        f"""
    CREATE OR REPLACE TABLE {name} AS
        SELECT
            * FROM '{file_path}'
    """
    )


def create_tables():
    create_table("customers")
    create_table("orders")
    create_table("products")
    create_table("details")
    create_table("categories")


def get_customer_orders(customer: str) -> None:
    print("hello customer", customer)

    create_tables()
    result_customers = conn.execute(f"SELECT * FROM customers").df()
    result_details = conn.execute(f"SELECT * FROM details").df()
    result_orders = conn.execute(f"SELECT * FROM orders").df()

    print("\ncustomers\n", result_customers)
    print("\ndetails\n", result_details)
    print("\norders\n", result_orders)

    print(conn.execute("DESCRIBE orders").df())
    #   print(conn.execute("DESCRIBE customers").df())
    print(conn.execute("SELECT CustomerID FROM orders").df())

    query_str = f"""SELECT
        c.FirstName,
        c.CustomerID,
        o.CustomerID,
        o.OrderID,
        d.OrderID,
        d.ProductID,
        p.ProductID,
        p.ProductName
        FROM customers AS c
        JOIN orders AS o ON c.CustomerID = o.CustomerID
        JOIN details AS d ON o.OrderID = d.OrderID
        JOIN products AS p ON d.ProductID = p.ProductID
        WHERE c.FirstName = '{customer}'
    """

    result1 = conn.execute(query_str).df()

    print(result1)


get_customer_orders("Bob")
