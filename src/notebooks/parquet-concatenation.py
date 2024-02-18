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
from faker import Faker


# %%
number_of_files = 11
bucket = "gs://ssb-prod-dapla-felles-data-delt"
dir = f"/tech-coach/parquet-test/concat/dataset-{number_of_files}/"
bucket_with_dir = bucket + dir
print(bucket_with_dir)
concat_file = (
    bucket + f"/tech-coach/parquet-test/concat/dataset-{number_of_files}-concat.parquet"
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
# # Skriv transaksjonsfiler
# %%
num_digits = len(str(number_of_files))
with time_block(f"Writing {number_of_files} transaction files"):
    for i in range(number_of_files):
        filename = f"{i:0{num_digits}}.parquet"  # pad the number with leading zeros
        df = create_transaction(i)
        dp.write_pandas(df, f"{bucket_with_dir}{filename}")

# %%
