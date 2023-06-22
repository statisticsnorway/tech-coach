# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # Les meteorologiske data fra frost api'et

# %%
import json
import os

import dapla as dp
import pandas as pd
import requests
from dotenv import load_dotenv


year = "2010"
version = "1"
bucket = "gs://ssb-prod-tech-coach-data-kilde"
folder = "tip-tutorials/frost_data"
filename = f"frost_p{year}_v{version}.parquet"

path = f"{bucket}/{folder}/{filename}"
print(f"Storage file: {path}")


# %% [markdown]
# For å få tilgang til frost API'et trenger du en client id som du får ved
# å registrere deg som bruker på https://frost.met.no/howto.html.
# Client id'en du får må du legge i en `.env` fil i rot-katalogen på repoet,
# og den skal i form se ut som dette:
#
# ```bash
# FROST_CLIENT_ID="5dc4-mange-nummer-e71cc"
# ```


# %%
def frost_client_id() -> str:
    """Get the frost_client_id secret.

    Read the client id from environment variable or .env file.

    Returns:
        The frost_client_id secret
    """
    load_dotenv()
    return os.getenv("FROST_CLIENT_ID")


# %%
# Define endpoint and parameters
endpoint = "https://frost.met.no/observations/v0.jsonld"
parameters = {
    "sources": "SN18700,SN90450",
    "elements": "mean(air_temperature P1D),sum(precipitation_amount P1D),mean(wind_speed P1D)",
    "referencetime": "2010-01-01/2010-12-31",
}
# Issue an HTTP GET request
r = requests.get(endpoint, parameters, auth=(frost_client_id(), ""))
# Extract JSON data
result = r.json()

# %%
# Check if the request worked, print out any errors
if r.status_code == 200:
    data = result["data"]
    print("Data retrieved from frost.met.no!")
    json_str = json.dumps(data, indent=2)  # Pretty print json
    # print(json_str)
else:
    print("Error! Returned status code %s" % r.status_code)
    print("Message: %s" % result["error"]["message"])
    print("Reason: %s" % result["error"]["reason"])

# %%
# This will return a Dataframe with all of the observations in a table format
df = pd.json_normalize(
    data, record_path=["observations"], meta=["sourceId", "referenceTime"]
)
df.head()

# %%
# Write to parquet file
dp.write_pandas(df=df, gcs_path=path)

# If you want to write to .parquet with plain Pandas, use the function below
# df.to_parquet(path, storage_options=dp.pandas.get_storage_options())

# %% [markdown]
# ## Konvertering til inndata

# %%
# These columns will be kept
columns = ["sourceId", "referenceTime", "elementId", "value", "unit", "timeOffset"]
df2 = df[columns].copy()
# Convert the time value to something Python understands
df2["referenceTime"] = pd.to_datetime(df2["referenceTime"])
df2.head()
