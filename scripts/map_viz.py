"""
Port & Shipping Cargo Analysis — Interactive World Map
Big Data Mini Project 2 | Farhana
Chart 7: folium world map (Integrated Coordinate Lookup)
"""

import pandas as pd
import folium
import os
import warnings
warnings.filterwarnings("ignore")

os.makedirs("output/charts", exist_ok=True)

# ── 1. Load MapReduce Results ─────────────────────────────────────
# Using quoting=3 to handle special characters in port names [cite: 95]
try:
    job1 = pd.read_csv("output/hdfs_results/job1/job1/part-r-00000",
                       sep="\t", header=None, names=["port", "volume"], quoting=3)
    job2 = pd.read_csv("output/hdfs_results/job2/job2/part-r-00000",
                       sep="\t", header=None, names=["port", "avg_delay"], quoting=3)
    print("✅ MapReduce job results loaded.")
except Exception as e:
    print(f"❌ Error loading Job results: {e}")
    exit()

# ── 2. Static Coordinate Mapping (Since Raw CSV lacks Lat/Lon) ─────
# Manual mapping for key global ports involved in the analysis [cite: 130, 131, 132]
port_coords = {
    'Singapore': [1.2901, 103.8519], 'Shanghai': [31.2304, 121.4737],
    'Rotterdam': [51.9225, 4.4792], 'Antwerp': [51.2194, 4.4025],
    'Los Angeles': [34.0522, -118.2437], 'New York': [40.7128, -74.0060],
    'Hamburg': [53.5511, 9.9937], 'Dubai': [25.2048, 55.2708],
    'Busan': [35.1796, 129.0756], 'Ningbo': [29.8683, 121.5440],
    'Shenzhen': [22.5431, 114.0579], 'Port Klang': [3.0000, 101.4000]
}

# Convert dictionary to DataFrame for merging
coords_df = pd.DataFrame.from_dict(port_coords, orient='index', 
                                   columns=['latitude', 'longitude']).reset_index()
coords_df.columns = ['portname', 'latitude', 'longitude']

# ── 3. Merge Data ─────────────────────────────────────────────────
merged = job1.merge(coords_df, left_on="port", right_on="portname", how="inner")
merged = merged.merge(job2, on="port", how="left").fillna(0)

print(f"🌍 Mapped {len(merged)} major ports for visualization.")

# ── 4. Build Map ──────────────────────────────────────────────────
m = folium.Map(location=[20, 10], zoom_start=2, tiles="CartoDB dark_matter")

# Legend and Title [cite: 166]
title_html = '<h3 align="center" style="font-size:16px; color:#E6EDF3"><b>Global Port Cargo Analysis</b></h3>'
m.get_root().html.add_child(folium.Element(title_html))

for _, row in merged.iterrows():
    # Scale radius based on cargo volume [cite: 158]
    radius = max(5, (row['volume'] / merged['volume'].max()) * 30)
    
    # Color based on volume (Blue for high, Amber for low) [cite: 158]
    color = "#58A6FF" if row['volume'] > merged['volume'].mean() else "#F0883E"

    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=radius,
        color=color,
        fill=True,
        fill_opacity=0.7,
        tooltip=f"Port: {row['port']}<br>Volume: {row['volume']:,.0f}<br>Avg Delay: {row['avg_delay']:.1f}h"
    ).add_to(m)

out_path = "output/charts/chart7_world_port_map.html"
m.save(out_path)
print(f"🎉 Success! Map saved to: {out_path}")