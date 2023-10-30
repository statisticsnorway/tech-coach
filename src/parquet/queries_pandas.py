from pathlib import Path

import pandas as pd


def load_table(name: str) -> pd.DataFrame:
    data_dir = Path(__file__).parent / "dataset"
    file_path = data_dir / f"{name}.parquet"
    return pd.read_parquet(file_path)


def get_customer_orders(customer: str) -> pd.DataFrame:
    # Load tables
    customers = load_table("customers")
    orders = load_table("orders")
    details = load_table("details")
    products = load_table("products")

    # Merge tables and filter data
    df = pd.merge(customers, orders, on="CustomerID")
    df = pd.merge(df, details, on="OrderID")
    df = pd.merge(df, products, on="ProductID")

    # Filter customer
    df = df[df["FirstName"] == customer][["FirstName", "ProductName"]]

    return df


if __name__ == "__main__":
    result = get_customer_orders("Alice")
    print(result)
