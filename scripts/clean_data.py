import pandas as pd
import os

print("Loading dataset...")
df = pd.read_csv('data/raw/Daily_Port_Activity_Data_and_Trade_Estimates.csv')
print(f"Original shape: {df.shape}")

# 1. Drop unnecessary columns
df = df.drop(columns=['ObjectId'])

# 2. Clean date column
df['date'] = pd.to_datetime(df['date'], utc=True).dt.date

# 3. Drop rows where portname or country is null
df = df.dropna(subset=['portname', 'country'])

# 4. Fill numeric nulls with 0
numeric_cols = [
    'portcalls_container', 'portcalls_dry_bulk', 'portcalls_general_cargo',
    'portcalls_roro', 'portcalls_tanker', 'portcalls_cargo', 'portcalls',
    'import_container', 'import_dry_bulk', 'import_general_cargo',
    'import_roro', 'import_tanker', 'import_cargo', 'import',
    'export_container', 'export_dry_bulk', 'export_general_cargo',
    'export_roro', 'export_tanker', 'export_cargo', 'export'
]
df[numeric_cols] = df[numeric_cols].fillna(0)

# 5. Add region column (for MapReduce custom partitioner)
asia = ['China','Japan','South Korea','India','Singapore','Malaysia',
        'Indonesia','Thailand','Vietnam','Bangladesh','Pakistan',
        'Sri Lanka','Philippines','Taiwan','Hong Kong','Australia']
europe = ['Germany','Netherlands','Belgium','France','Spain','Italy',
          'United Kingdom','Turkey','Greece','Sweden','Denmark',
          'Norway','Finland','Poland','Portugal','Russia']

def assign_region(country):
    if country in asia:
        return 'Asia'
    elif country in europe:
        return 'Europe'
    else:
        return 'Americas'

df['region'] = df['country'].apply(assign_region)

# 6. Save cleaned file
out_path = 'data/processed/port_data_clean.csv'
os.makedirs('data/processed', exist_ok=True)
df.to_csv(out_path, index=False)

print(f"Cleaned shape: {df.shape}")
print(f"Saved to: {out_path}")
print(f"\nRegion distribution:\n{df['region'].value_counts()}")
print(f"\nNull check:\n{df.isnull().sum().sum()} total nulls remaining")
print(f"\nSample:\n{df[['date','portname','country','region','portcalls_container','export','import']].head()}")