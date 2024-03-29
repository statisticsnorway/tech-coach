# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # Les inn data fra en BigQuery tabell/view
# KPI har behov for å lese inn togpriser fra en BigQuery tabell hos Entur.
# Dette er et eksempel som viser innlesning fra en tabell i staging BQ-miljøet til folkeregisteret
# og lagrer den ned som parquet-fil i kildedatabøtta, samt som csv-fil i synk-ned bøtta og
# som overføres til bakken.

# %%
import os

import dapla as dp
import pandas as pd
from dapla import AuthClient
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account


year = "2023"
version = "1"
bucket = "gs://ssb-prod-tech-coach-data-kilde"
transfer_bucket = "gs://ssb-prod-tech-coach-data-synk-ned"
folder = "tip-tutorials/entur_data"
filename = f"entur_p{year}_v{version}.parquet"

source_path = f"{bucket}/{folder}/{filename}"
transfer_path = f"{transfer_bucket}/{folder}/{filename}"
print(f"Storage file: {source_path}")
print(f"Transfer file: {transfer_path}")


# %%
# Define the path to the service account key file
key_path = "/home/jovyan/credentials/dev-tech-coach-6cc5-bq-entur-sa-f840b8741dd4.json"


def get_bq_client(project: str) -> bigquery.Client:
    """
    Return a BigQuery client for a given project - initialized with a personal
    Google Identity token.
    """
    # credentials = AuthClient.fetch_google_credentials()
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    return bigquery.Client(project=project, credentials=credentials)


# %% [markdown]
# Client id'en du får må du legge i en .env fil i rot-katalogen på repoet, og den skal i form se ut som dette:
#
# GCP_PROJECT_ID_ID="staging-mange-nummer-71345"


# %%
def gcp_id() -> str:
    """Get the GCP project ID.

    Read the client id from environment variable or .env file.

    Returns:
        The GCP project id.
    """
    load_dotenv()
    return os.getenv("GCP_PROJECT_ID")


# %%
bq_client = get_bq_client(gcp_id())
print(bq_client)

# %%
query = f"""
    SELECT folkeregisteridentifikator, ajourholdstidspunkt, erGjeldende, kilde,
    aarsak, gyldighetstidspunkt, opphoerstidspunkt, adressegradering,
    vegadresse, adresseIdentifikatorFraMatrikkelen
    FROM `{gcp_id()}.inndata.v_postadresse` LIMIT 10
"""
print(gcp_id())
query_job = bq_client.query(query)

# df = bq_client.query(query).to_dataframe()
# df.head()

# %%
dp.write_pandas(df=df, gcs_path=source_path)

# %%
dp.write_pandas(df=df, gcs_path=transfer_path, file_format="csv")
