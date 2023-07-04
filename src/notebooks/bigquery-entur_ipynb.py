# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # Hente togpriser fra Entur med BigQuery
# Denne filen må kjøres av en som er kildedataansvarlig, det vil si medlem av
# `prisstat-data-admins` gruppa.
#
# ## Initielt oppsett
# Første gang etter at man har clonet repoet må man opprette en `.env`-fil i
# rotkatalogen. Denne brukes til å lese inn hemmeligheter og ting som ikke skal
# være synlige i kildekoden, ref SSBs regel om at kildekode på GitHub ikke skal
# inneholde passord eller hemmeligheter, [R-003]. Se veiledning for håndtering
# av [hemmeligheter] og passord i git for flere detaljer.
#
# For å få tilgang til Entur sin BigQuery database bruker vi en service account (SA).
# Nøkkelen til denne SA'en ligger i en fil som du må angi i `.env`-fila.
#
# I `.env`-fila legger du til en linje som angir stien til hvor fila med nøkkelen til
# SA'en ligger. Legg til en linje for prod-miljøet og en for staging-miljøet, slik som
# vist nedenfor, men bytt ut med din sti og filnavn:
#
# ```shell
# ENTUR_PROD_SA_KEY_PATH="/home/jovyan/credentials/entur-prod-sa.json"
# ENTUR_STAGING_SA_KEY_PATH="/home/jovyan/credentials/entur-staging-sa.json"
# ```
#
# [hemmeligheter]:  https://statistics-norway.atlassian.net/wiki/spaces/BEST/pages/3216703491/Hvordan+h+ndtere+hemmeligheter+og+passord+i+git
# [R-003]: https://statistics-norway.atlassian.net/wiki/spaces/BEST/pages/3041492993/Regler+og+anbefalinger+for+versjonskontroll+med+Git#R-003-Regel%3A-Kildekode-i-GitHub-skal-ikke-inneholde-ukrypterte-passord-eller-hemmeligheter.
#
# ## Kode

# %%
import os
from datetime import datetime

import dapla as dp
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account


# %% [markdown]
# ### Parametre
# Det skal holde å kun endre datoer i cellen nedenfor for normale datainnhentinger.
#
# `from_date` angir fra og med dato.
# `to_date` angir fram til den angitte datoen, men inkluderer ikke den angitte datoen.

# %%
from_date = "2023-06-01"
to_date = "2023-06-03"

query = f"""
    SELECT *
    FROM `ent-data-external-ext-prd.ssb_sales_insight.orders_dashboard_tickets_ssb_view_v1`
    WHERE startTime >= '{from_date}' AND startTime < '{to_date}' and operator_group = 'Togselskap'
"""
print(query)

# %%
sourcedata_bucket = "gs://ssb-prod-tech-coach-data-kilde"  # kildedata
sourcedata_folder = "kpi/entur"
sourcedata_file_prefix = "togpriser"

transfer_bucket = "gs://ssb-prod-tech-coach-data-synk-ned"


# %%
def get_bq_sa_client(key_path: str) -> bigquery.Client:
    """
    Return a BigQuery client for a given project - initialized with a service account
    token.
    """
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


# %%
load_dotenv()
key_path = os.getenv("ENTUR_STAGING_SA_KEY_PATH")
bq_client = get_bq_sa_client(key_path)

query_job = bq_client.query(query)
results = query_job.result()  # Waits for the job to complete

df = results.to_dataframe()
df.head()

# %%
df.info()

# %%
# Create filename and path for sourcedata and transferdata
last_date = df["startTime"].max().strftime("%Y-%m-%d")
now = datetime.now().replace(microsecond=0).isoformat()
sourcedata_path = f"{sourcedata_bucket}/{sourcedata_folder}/{sourcedata_file_prefix}_p{from_date}_p{last_date}_{now}.parquet"
transfer_path = f"{transfer_bucket}/{sourcedata_folder}/{sourcedata_file_prefix}_p{from_date}_p{last_date}_{now}.cvs"

# %%
# Write to parquet file in sourcedata bucket
dp.write_pandas(df=df, gcs_path=sourcedata_path)
print(f"Wrote sourcedata  : {sourcedata_path}")

# Write to csv file in transfer bucket
dp.write_pandas(df=df, gcs_path=transfer_path, file_format="csv")
print(f"Wrote transferdata: {transfer_path}")
