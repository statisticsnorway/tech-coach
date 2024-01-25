# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %%
import contextlib
import time
from pathlib import Path

import dapla as dp
import pandas as pd


# %%
dataset_dir = dp.repo_root_dir().parent / "datasets"
csv_file = dataset_dir / "BooksDatasetClean.csv"
parquet_file = dataset_dir / "BooksDatasetClean.parquet"
# df = dp.read_pandas()
df = pd.read_csv(csv_file)
df.rename(
    columns={
        "Publish Date (Year)": "PublishYear",
        "Publish Date (Month)": "PublishMonth",
    },
    inplace=True,
)
df.head()

# %%
# Write unpartitioned and partitioned parquet file
df.to_parquet(parquet_file)
dp.write_pandas(
    df,
    "gs://ssb-tech-coach-data-produkt-test/temp/parquet-test2/BooksDatasetClean.parquet",
)

df.to_parquet(
    dataset_dir / "parquet_partition", partition_cols=["PublishYear", "PublishMonth"]
)
df.to_parquet(
    "gs://ssb-tech-coach-data-produkt-test/temp/parquet-test2/parquet_partition",
    storage_options=dp.pandas.get_storage_options(),
    partition_cols=["PublishYear", "PublishMonth"],
)


# %%
@contextlib.contextmanager
def time_block(label):
    start_time = time.time()
    yield
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"{label}: {elapsed_time} seconds")


# %%
# Read unpartitioned and partitioned parquet files
with time_block("Unpartitioned"):
    df_up = pd.read_parquet(parquet_file, engine="pyarrow")

# %%
with time_block("Partitioned"):
    df_p = pd.read_parquet(dataset_dir / "parquet_partition", engine="pyarrow")

# %%
# Read unpartitioned from bucket
with time_block("Unpartitioned, bucket"):
    df_upb = dp.read_pandas(
        "gs://ssb-tech-coach-data-produkt-test/temp/parquet-test2/BooksDatasetClean.parquet"
    )
    # df_upb = pd.read_parquet("gs://ssb-tech-coach-data-produkt-test/temp/parquet-test2/BooksDatasetClean.parquet", storage_options=dp.pandas.get_storage_options())


# %%
# Read partitioned from bucket
with time_block("Partitioned, bucket"):
    df_pb = pd.read_parquet(
        "gs://ssb-tech-coach-data-produkt-test/temp/parquet-test2/parquet_partition",
        storage_options=dp.pandas.get_storage_options(),
    )


# %%
df.info()

# %%
