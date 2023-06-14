# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # Read meteorological data from the frost API

# %%
import json

import pandas as pd
import requests


# Insert your own client ID here
client_id = "5dc47394-a994-4dc6-bbdd-088e323e71cc"

# %%
# Define endpoint and parameters
endpoint = "https://frost.met.no/observations/v0.jsonld"
parameters = {
    "sources": "SN18700,SN90450",
    "elements": "mean(air_temperature P1D),sum(precipitation_amount P1D),mean(wind_speed P1D)",
    "referencetime": "2010-04-01/2010-04-03",
}
# Issue an HTTP GET request
r = requests.get(endpoint, parameters, auth=(client_id, ""))
# Extract JSON data
result = r.json()

# %%
# Check if the request worked, print out any errors
if r.status_code == 200:
    data = result["data"]
    print("Data retrieved from frost.met.no!")
    json_str = json.dumps(data, indent=2)  # Pretty print json
    print(json_str)
else:
    print("Error! Returned status code %s" % r.status_code)
    print("Message: %s" % json["error"]["message"])
    print("Reason: %s" % json["error"]["reason"])


# %%
# This will return a Dataframe with all of the observations in a table format
df = pd.json_normalize(
    data, record_path=["observations"], meta=["sourceId", "referenceTime"]
)
df.head()

# %%
# These columns will be kept
columns = ["sourceId", "referenceTime", "elementId", "value", "unit", "timeOffset"]
df2 = df[columns].copy()
# Convert the time value to something Python understands
df2["referenceTime"] = pd.to_datetime(df2["referenceTime"])
df2.head()
