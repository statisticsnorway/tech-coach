# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # Testing a ytelse med parquet-filer
# Denne koden tester leseytelse ved lesing av parquet-filer, både fra lokalt filsystem og fra bøtter på Dapla.
#
# Datasettene som er brukt er åpne og hentet fra Kaggle.
# BooksDataset: https://www.kaggle.com/datasets/elvinrustam/books-dataset (103 063 rader og 15 kolonner).
#
# For filtest er de lagret i repoet som CSV-fil, og for Dapla-test er de lagret i bøtten:
# `gs://ssb-prod-dapla-felles-data-delt/tech-coach/parquet-test/books/BooksDatasetClean.parquet`

# %%
import contextlib
import time
from pathlib import Path

import dapla as dp
import pandas as pd


# %%
# Read and display dataset
dataset_dir = dp.repo_root_dir() / "datasets"
csv_file = dataset_dir / "BooksDatasetClean.csv"
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
df.info()

# %% [markdown]
# ## Skriving av dataset og partisjonering
# Når vi tester partisjonering så har vi partisjonert datasettet på år og måned, totalt 1132 partisjoner.

# %%
# Write unpartitioned and partitioned parquet file to local file system and buckets
# Write to local file system
filename = "BooksDatasetClean.parquet"
parquet_file = dataset_dir / filename
df.to_parquet(parquet_file)
df.to_parquet(
    dataset_dir / "partitioned", partition_cols=["PublishYear", "PublishMonth"]
)

# Write to buckets
bucket_with_dir = "gs://ssb-prod-dapla-felles-data-delt/tech-coach/parquet-test/books"
dp.write_pandas(df, f"{bucket_with_dir}/unpartitioned/{filename}")
df.to_parquet(
    f"{bucket_with_dir}/partitioned",
    storage_options=dp.pandas.get_storage_options(),
    partition_cols=["PublishYear", "PublishMonth"],
)


# %%
# Function for performance measurements
@contextlib.contextmanager
def time_block(label):
    start_time = time.time()
    yield
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"{label}: {elapsed_time:.3f} seconds")


# %%
# Read unpartitioned and partitioned parquet files
with time_block("Reading unpartitioned from file"):
    df_up = pd.read_parquet(parquet_file, engine="pyarrow")

# %%
with time_block("Reading partitioned from file"):
    df_p = pd.read_parquet(dataset_dir / "parquet_partition", engine="pyarrow")

# %%
# Read unpartitioned from bucket
with time_block("Reading unpartitioned from bucket"):
    df_upb = dp.read_pandas(f"{bucket_with_dir}/unpartitioned/{filename}")
    # df_upb = pd.read_parquet("gs://ssb-tech-coach-data-produkt-test/temp/parquet-test2/BooksDatasetClean.parquet", storage_options=dp.pandas.get_storage_options())


# %%
# Read partitioned from bucket
with time_block("Reading partitioned from bucket"):
    df_pb = pd.read_parquet(
        f"{bucket_with_dir}/partitioned",
        storage_options=dp.pandas.get_storage_options(),
    )
