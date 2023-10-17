from pathlib import Path

import pandas as pd


def create_dataset() -> None:
    data_dir = Path(__file__).parent / "dataset"
    customers_df = pd.read_csv(data_dir / "customers.csv")
    print(customers_df.head())
    customers_df.to_parquet(data_dir / "customers.parquet")

    orders_df = pd.read_csv(data_dir / "orders.csv")
    print(orders_df.head())
    orders_df.to_parquet(data_dir / "orders.parquet")

    products_df = pd.read_csv(data_dir / "products.csv")
    print(products_df.head())
    products_df.to_parquet(data_dir / "products.parquet")

    orderdetails_df = pd.read_csv(data_dir / "details.csv")
    print(orderdetails_df.head())
    orderdetails_df.to_parquet(data_dir / "details.parquet")

    categories_df = pd.read_csv(data_dir / "categories.csv")
    print(categories_df.head())
    categories_df.to_parquet(data_dir / "categories.parquet")


if __name__ == "__main__":
    create_dataset()
