import sqlite3
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]   # same as in build_mexico_db.py
SQLITE_PATH = ROOT / "data" / "mexico" / "processed" / "mexico_devices.sqlite"

conn = sqlite3.connect(SQLITE_PATH)
df = pd.read_sql_query("SELECT * FROM mexico_devices", conn)
conn.close()

df_2017 = df[df["year"] == 2017].copy()

# Convert consecutivo to int, dropping weird/missing values
df_2017 = df_2017[df_2017["consecutivo"].notna()]
df_2017["consecutivo_int"] = df_2017["consecutivo"].astype(int)

print("Parsed 2017 rows:", len(df_2017))

max_consec = df_2017["consecutivo_int"].max()
min_consec = df_2017["consecutivo_int"].min()
print("Consecutivo range:", min_consec, "→", max_consec)

expected = set(range(1, max_consec + 1))  # 1..1592 (if that’s the max)
seen = set(df_2017["consecutivo_int"].tolist())
missing = sorted(expected - seen)

print("Missing consecutivo values:", missing[:50])
print("Number of missing:", len(missing))
