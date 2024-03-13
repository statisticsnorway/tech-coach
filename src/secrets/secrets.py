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

# %%
import os

from dapla import AuthClient
from dotenv import load_dotenv
from google.cloud import secretmanager
from google.cloud.secretmanager_v1.types import Replication, Secret


# %%
load_dotenv()  # Laster inn .env fil og setter miljøvariable
project = os.getenv("TECH_COACH_DEV_GCP_PROJECT_ID")
write_path_base = f"gs://ssb-{project}-sync-down/data/"

# %%
print(project)

# %% [markdown]
# ## Bruk av Google Secret Manager
# ### Opprett hemmelighet

# %%
# ID of the secret to create.
secret_id = "YOUR_SECRET_ID5"

# Create the Secret Manager client.
credentials = AuthClient.fetch_google_credentials()
client = secretmanager.SecretManagerServiceClient(credentials=credentials)

# Build the parent name from the project.
parent = f"projects/{project}"

replication_policy = Replication(
    user_managed=Replication.UserManaged(
        replicas=[
            Replication.UserManaged.Replica(
                location="europe-north1"
            )
        ]
    )
)

# Create the parent secret.
secret = client.create_secret(
    request={
        "parent": parent,
        "secret_id": secret_id,
        "secret": Secret(replication=replication_policy),
    }
)

# Add the secret version.
version = client.add_secret_version(
    request={"parent": secret.name, "payload": {"data": b"hello world!"}}
)

# %% [markdown]
# ## Les hemmelighet

# %%
secret_name = f"projects/{project}/secrets/{secret_id}/versions/latest"

# Access the secret version
response = client.access_secret_version(request={"name": secret_name})

# Alternatively, access a specific version of the secretet.
# response = client.access_secret_version(request={"name": version.name})

# Print the secret payload.
payload = response.payload.data.decode("UTF-8")
print(f"Plaintext: {payload}")
