from pathlib import Path

import duckdb
import pandas as pd


conn = duckdb.connect()


def create_table(name: str) -> None:
    data_dir = Path(__file__).parent / "dataset"
    file_path = str(data_dir / f"{name}.parquet")
    conn.execute(
        f"""CREATE OR REPLACE TABLE {name} AS
            SELECT * FROM '{file_path}'
        """
    )


def create_tables():
    create_table("customers")
    create_table("orders")
    create_table("products")
    create_table("details")
    create_table("categories")


def get_customer_orders(customer: str) -> pd.DataFrame:
    create_tables()

    query_str = f"""SELECT
        c.FirstName,
        p.ProductName
        FROM customers AS c
        JOIN orders AS o ON c.CustomerID = o.CustomerID
        JOIN details AS d ON o.OrderID = d.OrderID
        JOIN products AS p ON d.ProductID = p.ProductID
        WHERE c.FirstName = '{customer}'
    """

    return conn.execute(query_str).df()


if __name__ == "__main__":
    result = get_customer_orders("Alice")
    print(result)
