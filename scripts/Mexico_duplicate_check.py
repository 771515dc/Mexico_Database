import sqlite3
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SQLITE_PATH = ROOT / "data" / "mexico" / "processed" / "mexico_devices.sqlite"

conn = sqlite3.connect(SQLITE_PATH)
df = pd.read_sql_query("SELECT * FROM mexico_devices", conn)
conn.close()

df_2017 = df[df["year"] == 2017].copy()
df_2017 = df_2017[df_2017["consecutivo"].notna()]
df_2017["consecutivo_int"] = df_2017["consecutivo"].astype(int)

dup = (
    df_2017.groupby("consecutivo_int")
    .size()
    .reset_index(name="count")
    .query("count > 1")
    .sort_values("consecutivo_int")
)

print(dup.head(20))
print("Number of duplicated consecutivo values:", len(dup))
print("Total extra rows from duplicates:", dup["count"].sum() - len(dup))

