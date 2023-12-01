# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
# ---

# # Innlesing av døde fra Excel
# Basert på spørring i sattistikkbanken, https://www.ssb.no/statbank/table/08425/

# +
import pandas as pd
from dapla import AuthClient


# Hent token
token = AuthClient.fetch_google_credentials()
# -

# Les inn fil
df = pd.read_excel(
    "gs://ssb-prod-tech-coach-data-kilde/tip-tutorials/dode-08425_20230615-103429.xlsx",
    header=2,
    storage_options={"token": token},
)
df.rename(columns={"Unnamed: 1": "Kommune"}, inplace=True)
df.head()
