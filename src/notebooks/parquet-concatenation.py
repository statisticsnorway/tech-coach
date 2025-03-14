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
#
# For transaksjonsbaserte statistikker kan det være en utfordring å jobbe med mange små
# parquet-filer. Hver transaksjon lagres som en parquet-fil etterhvert som de kommer
# inn. Og det kan ta minutter eller timer å lese inn alle småfilene som trengs. Derfor
# trenger man å slå sammen alle småfilene til en, eller noen få, parquet-filer for å
# kunne jobbe effektivt med dem.
#
# Koden her lager og lagrer mange små parquet-filer til en bøtte, og leser dem så inn
# igjen, slår dem sammen til en fil, og leser den samlede filen. Tiden måles på alle
# operasjoner, slik at man kan teste ytelse.
#
# Noen konkrete behov: Grunnboka har et par millioner transaksjoner som skal slås
# sammen, og jakt og fangst rundt 200 000 filer.
#
# ## Hvordan teste med forskjellige størrelser?
#
# Endre på variaabelen `number_of_files` til ønskede antall filer og sett `write_files`
# til `False` hvis du bare vil teste leseytelse.

# %%
import contextlib
import time

import dapla as dp
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.fs
import pyarrow.parquet as pq
from faker import Faker


# %%
number_of_files = 11  # datasets for 11 and 5000 has been pre-generated
write_files = False
bucket = "gs://ssb-tech-coach-data-produkt-prod"
dir = f"/temp/parquet-test/concat/dataset-{number_of_files}/"
bucket_with_dir = bucket + dir
print(bucket_with_dir)
concat_file = (
    bucket + f"/temp/parquet-test/concat/dataset-{number_of_files}-concat.parquet"
)
concat_arrow_file = (
    bucket + f"/temp/parquet-test/concat/dataset-{number_of_files}-concat-arrow.parquet"
)


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
# Create in instance of Faker
fake = Faker("no_NO")


# %%
def create_transaction(tr_id: int) -> pd.DataFrame:
    data = {
        "transaction_id": tr_id,
        "name": fake.name(),
        "street_address": fake.street_address(),
        "city": fake.city(),
        "email": fake.email(),
        "job": fake.job(),
    }
    return pd.DataFrame(data, index=[0])


# %%
df = create_transaction(1)
df.head()


# %% [markdown]
# ## Skriv transaksjonsfiler
# %%
if write_files:
    num_digits = len(str(number_of_files))
    with time_block(f"Writing {number_of_files} transaction files"):
        for i in range(number_of_files):
            filename = f"{i:0{num_digits}}.parquet"  # pad the number with leading zeros
            df = create_transaction(i)
            dp.write_pandas(df, f"{bucket_with_dir}{filename}")

# %% [markdown]
# ## Les transaksjonsfiler og slå sammen

# %%
# Get list of parquet files
fs = dp.FileClient.get_gcs_file_system()
parquet_files = []
for root, dirs, files in fs.walk(bucket_with_dir):
    for file in files:
        if file.endswith(".parquet"):
            parquet_files.append(f"{root}{file}")

# %%
# Read transcation files
transactions_df_list = []
with time_block(f"Reading and concatenating {number_of_files} transaction files"):
    for file in parquet_files:
        transactions_df_list.append(dp.read_pandas(file))
    df_all = pd.concat(transactions_df_list)
    dp.write_pandas(df_all, concat_file)


# %%
# Read and concatenate using pyarrow
with time_block("Reading and concatenating using pyarrow dataset"):
    gcs = pyarrow.fs.GcsFileSystem()

    # Build a dataset from the GCS bucket path
    dataset = ds.dataset(
        bucket_with_dir.removeprefix("gs://"), format="parquet", filesystem=gcs
    )
    table = dataset.to_table()
    pq.write_table(table, concat_arrow_file.removeprefix("gs://"), filesystem=gcs)
