# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # Test på sammenslåing av parquet-filer i bøtter

# %%
import contextlib
import time

import dapla as dp
import pandas as pd


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
def create_transaction_df(id: int) -> pd.DataFrame:
    pass
