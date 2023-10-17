from pathlib import Path
import pandas as pd


def create_dataset() -> None:
    data_dir = Path(__file__).parent / "dataset"
    customers_df = pd.read_csv(data_dir / "customers.csv")
    print(customers_df.head())
    customers_df.to_parquet(data_dir / "customers.parquet")


if __name__ == "__main__":
    create_dataset()
