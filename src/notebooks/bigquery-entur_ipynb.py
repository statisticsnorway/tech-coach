# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
# ---

# # Les inn data fra en BigQuery tabell/view
# KPI har behov for å lese inn togpriser fra en BigQuery tabell hos Entur.
# Dette er et eksempel som viser innlesning fra en tabell i staging BQ-miljøet til folkeregisteret
# og lagrer den ned som parquet-fil i kildedatabøtta, samt som csv-fil i synk-ned bøtta og
# som overføres til bakken.

# +
import dapla as dp
import pandas as pd
from dapla import AuthClient
from google.cloud import bigquery
from dotenv import load_dotenv
import os

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


# -

def get_bq_client(project: str) -> bigquery.Client:
    """
    Return a BigQuery client for a given project - initialized with a personal
    Google Identity token.
    """
    credentials = AuthClient.fetch_google_credentials()
    return bigquery.Client(project=project, credentials=credentials)


# Client id'en du får må du legge i en .env fil i rot-katalogen på repoet, og den skal i form se ut som dette:
#
# GCP_PROJECT_ID_ID="staging-mange-nummer-71345"

def gcp_id() -> str:
    """Get the GCP project ID.

    Read the client id from environment variable or .env file.

    Returns:
        The GCP project id.
    """
    load_dotenv()
    return os.getenv("GCP_PROJECT_ID")


bq_client = get_bq_client(gcp_id())
print(bq_client)

query = f"""
    SELECT folkeregisteridentifikator, ajourholdstidspunkt, erGjeldende, kilde, 
    aarsak, gyldighetstidspunkt, opphoerstidspunkt, adressegradering, 
    vegadresse, adresseIdentifikatorFraMatrikkelen 
    FROM `{gcp_id()}.inndata.v_postadresse` LIMIT 10
"""
df = bq_client.query(query).to_dataframe()
df.head()

dp.write_pandas(df=df, gcs_path=source_path)

dp.write_pandas(df = df,
                gcs_path = transfer_path,
                file_format = "csv")

