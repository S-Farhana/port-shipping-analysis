import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

print("Generating vessel schedule...")

random.seed(42)
np.random.seed(42)

# Load port names from cleaned data
ports_df = pd.read_csv('data/processed/port_data_clean.csv', usecols=['portname', 'country', 'region'])
ports = ports_df.drop_duplicates().reset_index(drop=True)

vessel_prefixes = ['MV', 'SS', 'MT', 'CMA', 'MSC', 'EVER', 'COSCO', 'MAERSK']
vessel_names = ['ATLAS', 'TITAN', 'CORONA', 'PACIFIC', 'AURORA', 'HORIZON',
                'NEPTUNE', 'VOYAGER', 'PIONEER', 'EXPRESS', 'GLORY', 'STAR']

rows = []
for i in range(5000):
    port_row = ports.sample(1).iloc[0]
    vessel_id = f"{random.choice(vessel_prefixes)}-{random.choice(vessel_names)}-{random.randint(100,999)}"
    arrival = datetime(2019, 1, 1) + timedelta(days=random.randint(0, 1460))
    dwell_hours = random.randint(6, 120)
    departure = arrival + timedelta(hours=dwell_hours)

    rows.append({
        'vessel_id':   vessel_id,
        'portname':    port_row['portname'],
        'country':     port_row['country'],
        'region':      port_row['region'],
        'arrival':     arrival.strftime('%Y-%m-%d %H:%M:%S'),
        'departure':   departure.strftime('%Y-%m-%d %H:%M:%S'),
        'dwell_hours': dwell_hours,
        'cargo_type':  random.choice(['container', 'dry_bulk', 'tanker', 'roro', 'general_cargo']),
        'cargo_tons':  round(random.uniform(500, 85000), 2),
        'flag':        random.choice(['Panama', 'Liberia', 'Marshall Islands', 'Singapore', 'China'])
    })

df = pd.DataFrame(rows)
os.makedirs('data/processed', exist_ok=True)
df.to_csv('data/processed/vessel_schedule.csv', index=False)

print(f"Generated {len(df)} vessel records")
print(f"Unique ports: {df['portname'].nunique()}")
print(f"Unique vessels: {df['vessel_id'].nunique()}")
print(f"\nRegion distribution:\n{df['region'].value_counts()}")
print(f"\nSample:\n{df.head()}")