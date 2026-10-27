import pandas as pd

df = pd.read_csv('/app/data/traffic_accidents.csv')

print("=== Traffic Accidents Analysis ===")
print(f"Total records: {len(df)}")
print(f"\nColumns: {list(df.columns)}")
print(f"\nFirst 5 rows:")
print(df.head())
print(f"\nBasic statistics:")
print(df.describe())
