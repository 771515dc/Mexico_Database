import sqlite3, json, os
os.makedirs("reports", exist_ok=True)
con = sqlite3.connect(r"data/jp_pmda_devices.sqlite")
row_count = con.execute("SELECT COUNT(*) FROM devices").fetchone()[0]
null_cert = con.execute("SELECT COUNT(*) FROM devices WHERE certification_number IS NULL OR certification_number=''").fetchone()[0]
dupe_groups = con.execute("""
SELECT COUNT(*) FROM (
  SELECT certification_number, brand_name, certificate_holder_name, COUNT(*) c
  FROM devices GROUP BY 1,2,3 HAVING c>1
) t
""").fetchone()[0]
min_dt, max_dt = con.execute("SELECT MIN(certification_date), MAX(certification_date) FROM devices").fetchone()
con.close()
with open(r"reports/jp_pmda_qa.json","w",encoding="utf-8") as f:
    json.dump({
      "row_count": row_count,
      "null_certification_number": null_cert,
      "duplicate_groups": dupe_groups,
      "min_certification_date": min_dt,
      "max_certification_date": max_dt
    }, f, ensure_ascii=False, indent=2)
print("Wrote reports/jp_pmda_qa.json")
