# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # Parsing av frost json-fil og lagring til parquet
# Denne filen leser en frost json-fil fra kildebøtta, dataminimerer og lagrer den til en parquet-fil i inndata.

# %%
import json
import os
from datetime import datetime, timezone

import dapla as dp
import pandas as pd


# %%
# inn i configfil
# bucket = "gs://ssb-tech-coach-data-kilde-prod"
source_bucket = "gs://ssb-tech-coach-data-kilde-test"
target_bucket = "gs://ssb-tech-coach-data-produkt-test"
folder = "tip-tutorials/frost_data"
filename = "frost_p2010-01-01_2010-12-31_2023-12-20T10-58-19.json"
source_path = f"{source_bucket}/{folder}/{filename}"
source_filename = source_path.split("/")[-1].replace("json", "parquet")
target_path = f"{target_bucket}/{folder}/{filename.replace('json', 'parquet')}"
print(f"Source file: {source_path}")
print(f"Target file: {target_path}")
print(f"Source filename: {source_filename}")

# %%
# Read json file
with dp.FileClient.get_gcs_file_system().open(path, "r") as f:
    data = json.load(f)


# %%
# This will return a Dataframe with all of the observations in a table format
df = pd.json_normalize(
    data, record_path=["observations"], meta=["sourceId", "referenceTime"]
)
df.head()

# %% [markdown]
# ## Dataminimering og konvertering til inndata

# %%
# These columns will be kept
columns = ["sourceId", "referenceTime", "elementId", "value", "unit", "timeOffset"]
df2 = df[columns].copy()
# Convert the time value to something Python understands
df2["referenceTime"] = pd.to_datetime(df2["referenceTime"])
df2.head()

# %% [markdown]
# ## Skriv til inndata bøtte

# %%
# Write to parquet file
print(f"Write parquet file: {target_path}")
dp.write_pandas(df=df, gcs_path=target_path)
