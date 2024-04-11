# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # Test av metoder for å håndtere hemmeligheter

# %% [markdown]
# ## Bruk av .env fil
# Som beskrevet på https://statistics-norway.atlassian.net/wiki/spaces/BEST/pages/3216703491/Hvordan+h+ndtere+hemmeligheter+og+passord+i+git
#
# Dette virker, men det kan være en utfordring å distribuere endringer av hemmelighetene i teamet.
# Hvor skal de lagres, og hvordan skal de sendes til de andre i teamet ved oppdateringer?
#
# ### Laste ned .env fil fra en bøtte
# Et alternativ vi vurderte var å laste ned ned .env-filen fra en felles bøtte,
# slik som vist i koden nedenfor. Det virker teknisk, men løsningen ble avvist i
# møte med produkteier Dapla og sikkerhetssenteret 4. april 2024.

# %%
import os

import dapla as dp
from dapla import AuthClient
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import secretmanager
from google.cloud.secretmanager_v1.types import Replication, Secret


# %%
def download_env_file_from_bucket(gcs_path):
    """Download .env-file from bucket to root directory in repo"""
    dest_path = dp.repo_root_dir() / ".env"
    with dp.FileClient.gcs_open(gcs_path, "rb") as src, open(dest_path, "wb") as dst:
        dst.write(src.read())


# %%
download_env_file_from_bucket("gs://ssb-tech-coach-data-produkt-prod/temp/.env")

# %%
load_dotenv()  # Laster inn .env fil og setter miljøvariable
project_id = os.getenv("TECH_COACH_DEV_GCP_PROJECT_ID")
print(project_id)

# %% [markdown]
# ## Bruk av Google Secret Manager
# Google Secret Manager er per nå ikke tilgjengelig for standard Dapla-team. Behovet for
# en god måte å håndtere og distribuere hemmeligheter på er meldt til produkteier Dapla,
# og enten blir det en tjeneste på Dapla eller som minimum at blir en feature som kan
# enables.
#
# Koden nedenfor tar utgangspunkt i et GCP-prosjekt hvor Google Secret Manager er enablet.
#
# ### Verdt å tenke på
#
# For å hente ut en hemmelighet fra Google Secret Manager så trenger man GCP projekt ID
# og en secret_id, navnet på hemmeligheten, man skal hente.
#
# Per nå er GCP projekt ID definert til å håndteres som en hemmelighet, det vil si leses
# inn fra .env-fil. Man NAV behandler den ikke som hemmelig, så det er mulig at det blir
# en endring på det.
#
# Secret_id kan være sensitiv hvis man navngir den med et beskrivende navn, for eksempel
# SSB_ORACLE_MASTER_PASSWORD, men trenger ikke være det hvis man navngir den mer anonymt,
# for eksempel SECRET1. Så i en del tilfeller kan secret_id også måtte leses fra .env-fil.
#
# Så det man vinner på å bruke Google Secret Manager er først og fremst at man slipper
# å distribuere endringer. Dette siden secret_id og GCP prosjekt id er statiske.
#
# ### Opprett hemmelighet

# %%
# ID of the secret to create.
secret_id = "YOUR_SECRET_ID1"

base_name = f"projects/{project_id}"
secret_name = f"{base_name}/secrets/{secret_id}"
version_name = f"{secret_name}/versions/latest"

# Create the Secret Manager client.
credentials = AuthClient.fetch_google_credentials()
client = secretmanager.SecretManagerServiceClient(credentials=credentials)

replication_policy = Replication(
    user_managed=Replication.UserManaged(
        replicas=[Replication.UserManaged.Replica(location="europe-north1")]
    )
)

try:
    # Attempt to access the secret
    secret = client.access_secret_version(request={"name": version_name})
    print(f"Secret {secret_id} already exist, create a new version of the secret.")
    versions = client.list_secret_versions(request={"parent": secret_name})
    for version in versions:
        print(f"Version: {version.name}, State: {version.state.name}")
except NotFound:
    # If not found, create a new secret
    secret = client.create_secret(
        request={
            "parent": base_name,
            "secret_id": secret_id,
            "secret": Secret(replication=replication_policy),
        }
    )
    print(f"New secret {secret_id} created")

# Add the secret version.
version = client.add_secret_version(
    request={"parent": secret_name, "payload": {"data": b"hello world!"}}
)

# %% [markdown]
# ## Les hemmelighet

# %%
# Access the secret version
response = client.access_secret_version(request={"name": version_name})

# Print the secret payload.
payload = response.payload.data.decode("UTF-8")
print(f"Secret {response.name} payload: {payload}")
