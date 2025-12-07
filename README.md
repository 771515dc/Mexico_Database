## Mexico Medical Device Database (COFEPRIS)

This project extends the Japan PMDA device database with **Mexico’s medical device registrations** from COFEPRIS.  
The Mexico pipeline mirrors the Japan side: raw regulatory PDFs → normalized tabular data (Parquet + SQLite).

---

## 1. Directory Layout

```text
data/
  mexico/
    raw/
      reg_dm_2017.pdf
      Registros_Sanitarios_DM2018.pdf
      Registros_Sanitarios_DM2019.pdf
      Registros_Sanitarios_DM2020.pdf
      Registros_Sanitarios_DM2021-1.pdf
      Registros_Sanitarios_DM2022-1.pdf
      Registros_Sanitarios_DM2023.pdf
      Registros_Sanitarios_DM_2024.pdf
      Registros_Sanitarios_DM_abr_2025.pdf
      GUIA_*.pdf                  # optional regulatory guides, not used in table
    processed/
      mexico_devices.parquet
      mexico_devices.sqlite
scripts/
  build_mexico_db.py
requirements.txt

---

## 2. Setup
 - Install Python dependencies (from the repo root): pip install -r requirements.txt
 - requirements.txt (for reference):


