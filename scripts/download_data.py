import kagglehub
import shutil
import os

path = kagglehub.dataset_download("arunvithyasegar/daily-port-activity-data-and-trade-estimates")
print("Downloaded to:", path)

dest = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(dest, exist_ok=True)

for file in os.listdir(path):
    if file.endswith(".csv"):
        shutil.copy(os.path.join(path, file), os.path.join(dest, file))
        print(f"Copied {file} → data/raw/")