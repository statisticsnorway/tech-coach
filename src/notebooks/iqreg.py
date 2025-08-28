import pandas as pd
from pathlib import Path

csv_file = Path(__file__).parent.parent.parent /"datasets" / "iq_register.csv"
df = pd.read_csv(csv_file)
print(df.head())

df.to_parquet("/buckets/produkt/iqreg/klargjorte-data/iq-register_p2025_v1.parquet")