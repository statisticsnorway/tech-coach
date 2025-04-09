import pandas as pd
from pathlib import Path
from dapla_pseudo import Pseudonymize
from dapla_pseudo import Depseudonymize


def read_data():
    datafile = Path(__name__).parent.parent / "datasets" / "iq_register.csv"

    df = pd.read_csv(datafile)
    return df

def do_pseudo(df):
    # Create a pseudonymizer instance from the Pandas DataFrame
    pseudo_processor = Pseudonymize.from_pandas(df)

    # Specify the field(s) to process and set default encryption
    pseudo_processor = pseudo_processor.on_fields("SSN")
    # pseudo_processor: _Pseudonymizer = pseudo_processor.with_default_encryption()
    pseudo_processor = pseudo_processor.with_papis_compatible_encryption()

    # result = (Pseudonymize.from_pandas(df).on_fields("fnr").with_papis_compatible_encryption().run())result.to_polars().head()

    # Run the pseudonymization process
    processed_result = pseudo_processor.run()

    # Convert the processed result back to a Polars DataFrame
    pseudo_df = processed_result.to_pandas()

    return pseudo_df

def do_depseudo(df):
    # Create a pseudonymizer instance from the Pandas DataFrame
    pseudo_processor = Depseudonymize.from_pandas(df)

    # Specify the field(s) to process and set default encryption
    pseudo_processor = pseudo_processor.on_fields("SSN")
    # pseudo_processor: _Pseudonymizer = pseudo_processor.with_default_encryption()
    pseudo_processor = pseudo_processor.with_papis_compatible_encryption()

    # result = (Pseudonymize.from_pandas(df).on_fields("fnr").with_papis_compatible_encryption().run())result.to_polars().head()

    # Run the pseudonymization process
    processed_result = pseudo_processor.run()

    # Convert the processed result back to a Polars DataFrame
    pseudo_df = processed_result.to_pandas()

    return pseudo_df


df = read_data()
pseudo_df = do_pseudo(df)
depseudo_df = do_depseudo(pseudo_df)

print(df.head())
print('/n')
print(pseudo_df.head())
print('/n')
print(depseudo_df.head())
