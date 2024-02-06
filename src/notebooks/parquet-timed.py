# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %%
from datetime import datetime
import time
import tarfile
import pandas as pd
import dapla as dp
import json
from faker import Faker

# Create an instance of Faker
fake = Faker()

# %%
bucket = "gs://ssb-tech-coach-data-produkt-test/temp/leb/"
# Define the base filename
base_filename = "file_"
# Number of data items
num_items = 5
files_written = []


# %%
# A function that returns faker data 
def get_person():
    bd = fake.date_of_birth(minimum_age=18, maximum_age=99)
    age = datetime.today().year - bd.year - ((datetime.today().month, datetime.today().day) < (bd.month, bd.day))
    return {
        'name': fake.name(),
        'birthdate':bd,
        'age': age,
        'address': fake.address(),
        'city': fake.city(),
        'email': fake.email(),
        'job': fake.job(),
        'company': fake.company(),
    }


# %%
start_write = time.time()
for i in range(num_items):
    # Construct the filename based on the counter
    filename = base_filename + str(i) + ".parquet"
    bucket_file = bucket + filename
#    print("Filename:", filename)
#    print("Bucket File:", bucket_file)
    # Get random person
    person_data = get_person()
#    print(person_data)
    # Create a Pandas DataFrame from the generated data
    df = pd.DataFrame([person_data])
    df.to_parquet(filename)
    dp.write_pandas(df, bucket_file,)
    files_written.append(bucket_file)
    
end_write = time.time()
write_time = end_write - start_write
print("Write time:", write_time)


# %%
#print(files_written)

# %%
#read_data = []
# Read data from files
start_read = time.time()

for file_read in files_written:
    dp_r = dp.read_pandas(file_read)
#    print(file_read)
#    print(dp_r['name'])

end_read = time.time()
read_time = end_read - start_read
print("Read time:", read_time)

# %%
