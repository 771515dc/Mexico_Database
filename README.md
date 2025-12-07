
# Mexico Medical Device Database (COFEPRIS)

## Data Sources

**Regulatory Authority:** Comisión Federal para la Protección contra Riesgos Sanitarios (COFEPRIS, Mexico)  
**Main Site:** [https://www.gob.mx/cofepris](https://www.gob.mx/cofepris)

### Source Files
All files were downloaded from COFEPRIS public pages listing *Registros Sanitarios de Dispositivos Médicos*. These PDFs are stored locally under `data/mexico/raw/`.

| Filename | Description |
| :--- | :--- |
| `reg_dm_2017.pdf` | Listado de registros sanitarios de dispositivos médicos 2017 |
| `Registros_Sanitarios_DM2018.pdf` | 2018 device registrations |
| `Registros_Sanitarios_DM2019.pdf` | 2019 device registrations |
| `Registros_Sanitarios_DM2020.pdf` | 2020 device registrations |
| `Registros_Sanitarios_DM2021-1.pdf` | 2021 device registrations |
| `Registros_Sanitarios_DM2022-1.pdf` | 2022 device registrations |
| `Registros_Sanitarios_DM2023.pdf` | 2023 device registrations |
| `Registros_Sanitarios_DM_2024.pdf` | 2024 device registrations |
| `Registros_Sanitarios_DM_abr_2025.pdf` | Device registrations as of April 2025 |

**Note:** Regulatory guides (e.g., `GUIA_*.pdf`) were downloaded but are not used in the table generation as they do not contain device listings.

**Access Dates:** All PDFs were downloaded between **2025-11** and **2025-12**.

---

## Extraction Approach

### Tools
* **pdfplumber:** Used for parsing (no OCR; relies on embedded text).
* **pandas:** DataFrame construction and output.
* **sqlite3 + pyarrow:** For SQLite and Parquet exports.

### Multi-file Aggregation
Each year’s PDF is parsed separately with a year-specific parser. The script `scripts/build_mexico_db.py` concatenates all rows into a single DataFrame and exports to:
* `data/mexico/processed/mexico_devices.parquet`
* `data/mexico/processed/mexico_devices.sqlite` (table: `mexico_devices`)

A `year` column is added based on the filename, and `country` is fixed to "MX".

### Year-Specific Parsing Logic

#### 2017 (`reg_dm_2017.pdf`)
Pages contain two stacked tables. We **only** parse the upper 5-column table:
* **Columns:** `[Consecutivo, No. Registro Sanitario, Denominación distintiva, Razón social, Clase]`
* **Logic:** Uses `page.extract_table()`. Rows are selected if the first cell is numeric and the row has 5 cells.
* **Note:** The lower 2-column table (Description/Date) is currently ignored.

#### 2018–2019
PDFs expose a 5-column table.
* **Logic:** `consecutivo` and `registro_sanitario` are read from columns 1–2.
* **Details:** Remaining cells are concatenated.
* **Date:** Extracted via regex (`dd/mm/yyyy`) -> `fecha_emision`.
* **Generic Name:** The last column is used as `denominacion_generica`.

#### 2020–2023
PDFs generally appear as single-column tables with embedded newlines.
* **Consecutivo:** Extracted from patterns like `\n0241`.
* **Registro Sanitario:** Extracted via regex: `\d{3,4}[A-Z]\d{4}\s+SSA` (e.g., `0241R2021 SSA`).
* **Date:** First `dd/mm/yyyy` found is stored as `fecha_emision`.
* **Raw Data:** The full row text is stored in `raw_line`. Structured fields (holder, trade name) are generally left `NULL` for these years.

#### 2024–2025
PDFs contain wide tables with clear headers.
* **Logic:** We read the header row, normalize text, and dynamically map positions to columns (`consecutivo`, `holder`, `registro_sanitario`, `denominacion_distintiva`, `denominacion_generica`, `categoria`, `clase`, `fecha_emision`).

### Language & Deduplication
* **Language:** All source text is Spanish. No translation is performed.
* **Deduplication:** We treat each printed row in the PDF as one database row. We do **not** drop rows based on duplicate `registro_sanitario`.
* **ID:** An internal `entry_id` is generated (`country_year_registro_sanitario_index`).

---

## Schema

**Table:** `mexico_devices` (SQLite)

| Column | Type | Example | Description |
| :--- | :--- | :--- | :--- |
| **entry_id** | TEXT | `MX_2017_1459E2017 SSA_0` | Internal unique row ID. |
| **country** | TEXT | `MX` | Fixed to "MX". |
| **year** | INT | `2019` | Year associated with the PDF file. |
| **source_file** | TEXT | `Registros_Sanitarios_DM2019.pdf` | Original PDF filename. |
| **consecutivo** | TEXT | `1459` | Row label from COFEPRIS list. |
| **registro_sanitario** | TEXT | `1459E2017 SSA` | Primary real-world identifier. |
| **holder** | TEXT | `S4OPTIK S.A. DE C.V.` | Company / "Razón social del titular". |
| **denominacion_distintiva** | TEXT | `LAMPARA DE HENDIDURA...` | Brand / trade name. |
| **denominacion_generica** | TEXT | `EQUIPO PARA DIAGNOSTICO...` | Generic name (mainly 2018–19, 2024+). |
| **categoria** | TEXT | `EQUIPO MÉDICO` | Device category (mainly 2024+). |
| **clase** | TEXT | `II` | Risk class (I, II, III). |
| **fecha_emision** | TEXT | `02/01/2019` | Registration date (`dd/mm/yyyy`). |
| **details** | TEXT | `Equipo para diagnóstico...` | Long free-text field (mostly 2018–19). |
| **raw_line** | TEXT | `...` | Full unstructured text (2020–23). |

---

## Limitations, Edge Cases, and Quality Checks

### Limitations
1.  **2017 Split Tables:** The pipeline currently processes the 5-column identity table but ignores the separate description/date table.
2.  **2020–2023 Unstructured Text:** These years are extracted mostly as raw text lines. Fields like `holder` or `denominacion_distintiva` are often `NULL`, with data residing in `raw_line`.
3.  **Consecutivo Gaps:** Gaps in numbering or duplicate labels in the source PDFs (e.g., 2017) reflect the official publication and are not parsing errors.
4.  **Missing GUIA Documents:** Informational PDFs that do not contain device rows were excluded.

### Quality Checks
Total Rows Extracted: **19,003**

* **2017:** 1,572 rows
* **2018:** 2,747 rows
* **2019:** 2,275 rows
* **2020:** 1,656 rows
* **2021:** 2,205 rows
* **2022:** 1,669 rows
* **2023:** 2,422 rows
* **2024:** 3,557 rows
* **2025:** 900 rows

*Verified that duplicate `consecutivo` or `registro_sanitario` values appearing in the source PDFs are preserved.*

---

## Reproduction Steps

1.  **Clone this repository (or download the source code):**
    ```bash
    git clone <this-repo-url>
    cd <repo-folder>
    ```

2.  **Place the raw PDFs in the expected folder:**
    Ensure the following files are present in `data/mexico/raw/`:
    * `reg_dm_2017.pdf`
    * `Registros_Sanitarios_DM2018.pdf`
    * `Registros_Sanitarios_DM2019.pdf`
    * `Registros_Sanitarios_DM2020.pdf`
    * `Registros_Sanitarios_DM2021-1.pdf`
    * `Registros_Sanitarios_DM2022-1.pdf`
    * `Registros_Sanitarios_DM2023.pdf`
    * `Registros_Sanitarios_DM_2024.pdf`
    * `Registros_Sanitarios_DM_abr_2025.pdf`

3.  **Create and activate a Python environment (optional but recommended):**
    ```bash
    python -m venv .venv
    source .venv/bin/activate       # macOS/Linux
    # or
    .venv\Scripts\activate          # Windows
    ```

4.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *Where `requirements.txt` contains at least:*
    ```text
    pandas>=2.0.0
    pdfplumber>=0.11.0
    pyarrow>=14.0.0
    ```

5.  **Run the build script:**
    From the repo root:
    ```bash
    python scripts/build_mexico_db.py
    ```
    On success, you should see row counts per year and the following output files:
    * `data/mexico/processed/mexico_devices.parquet`
    * `data/mexico/processed/mexico_devices.sqlite`

6.  **(Optional) Verify row counts:**
    You can quickly confirm the total rows with Python:
    ```python
    import sqlite3, pandas as pd

    conn = sqlite3.connect("data/mexico/processed/mexico_devices.sqlite")
    df = pd.read_sql_query("SELECT year, COUNT(*) AS n FROM mexico_devices GROUP BY year ORDER BY year", conn)
    conn.close()
    print(df)
    ```
    The counts should match the ones listed in the **Quality Checks** section above.



