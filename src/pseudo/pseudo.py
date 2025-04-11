import pandas as pd
from pathlib import Path
from dapla_pseudo import Pseudonymize
from dapla_pseudo import Depseudonymize


def read_data():
    datafile = Path(__name__).parent.parent / "datasets" / "iq_register.csv"

    df = pd.read_csv(datafile)
    return df

def do_pseudo(df, encryption_type=None):
    # Create a pseudonymizer instance from the Pandas DataFrame
    pseudo_processor = Pseudonymize.from_pandas(df)

    # Specify the field(s) to process and set default encryption
    pseudo_processor = pseudo_processor.on_fields("SSN")

    # Choose encryption method based on the input encryption_type
    if encryption_type == "papis":
        pseudo_processor = pseudo_processor.with_papis_compatible_encryption()
    elif encryption_type is None:
        pseudo_processor = pseudo_processor.with_default_encryption()
    else:
        print(f"Warning: Unknown encryption_type '{encryption_type}', using default encryption.")
        pseudo_processor = pseudo_processor.with_default_encryption()

    # Run the pseudonymization process
    processed_result = pseudo_processor.run()

    # Convert the processed result back to a Polars DataFrame
    pseudo_df = processed_result.to_pandas()

    return pseudo_df

def do_depseudo(df, encryption_type=None):
    # Create a depseudonymizer instance from the Pandas DataFrame
    pseudo_processor = Depseudonymize.from_pandas(df)

    # Specify the field(s) to process and set default encryption
    pseudo_processor = pseudo_processor.on_fields("SSN")

    # Choose encryption method based on the input encryption_type
    if encryption_type == "papis":
        pseudo_processor = pseudo_processor.with_papis_compatible_encryption()
    elif encryption_type is None:
        pseudo_processor = pseudo_processor.with_default_encryption()
    else:
        print(f"Warning: Unknown encryption_type '{encryption_type}', using default encryption.")
        pseudo_processor = pseudo_processor.with_default_encryption()

    # Run the pseudonymization process
    processed_result = pseudo_processor.run()

    # Convert the processed result back to a Polars DataFrame
    pseudo_df = processed_result.to_pandas()

    return pseudo_df


df = read_data()

pseudo_df = do_pseudo(df)
pseudo_df_papis = do_pseudo(df, "papis")

depseudo_df = do_depseudo(pseudo_df)
depseudo_df_papis = do_depseudo(pseudo_df, "papis")

print(df.head())
print('/n')

print(pseudo_df.head())
print('/n')
print(pseudo_df_papis.head())
print('/n')

print(depseudo_df.head())
print('/n')
print(depseudo_df_papis.head())
print('/n')
