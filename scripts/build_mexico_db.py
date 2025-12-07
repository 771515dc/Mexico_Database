"""
Build a normalized Mexico device table from COFEPRIS PDFs.

- Reads PDFs from data/mexico/raw
- Parses them with pdfplumber and some regex
- Writes:
    - data/mexico/processed/mexico_devices.parquet
    - data/mexico/processed/mexico_devices.sqlite (table: mexico_devices)

Design notes:
- We treat Registro Sanitario as the real-world identifier in analysis.
- 'consecutivo' is just an ordering/label column and may repeat or have gaps.
- For 2017, the PDF has:
    * First half: a 5-column table (Consecutivo, Registro, Nombre, Razón social, Clase)
    * Second half: a 2-column table (Descripción de insumo, Fecha de emisión)
  We currently ONLY parse the 5-column part (device list) and ignore the
  description/date pages. So we don't lose devices, only some extra text/date.
"""

import re
import io
import sqlite3
from pathlib import Path

import pdfplumber
import pandas as pd


# ---------------------------------------------------------------------
# Paths – adjust if your layout is different
# ---------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]   # repo root (.. from scripts/)
RAW_DIR = ROOT / "data" / "mexico" / "raw"
PROCESSED_DIR = ROOT / "data" / "mexico" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SQLITE_PATH = PROCESSED_DIR / "mexico_devices.sqlite"
PARQUET_PATH = PROCESSED_DIR / "mexico_devices.parquet"


# ---------------------------------------------------------------------
# Helper: load PDF bytes
# ---------------------------------------------------------------------

def _load_pdf_bytes(path: Path) -> bytes:
    return path.read_bytes()


# ---------------------------------------------------------------------
# 2017 parser – special-but-simple
# ---------------------------------------------------------------------

def parse_2017(pdf_bytes: bytes, source_name: str):
    """
    reg_dm_2017.pdf

    The PDF structure:
    - Pages with a 5-column table:
        [Consecutivo, No. Registro Sanitario, Denominación distintiva,
         Razón Social del Titular, Clase]
    - Followed by many pages with a 2-column table:
        [Descripción de insumo, Fecha de emisión]

    Here we:
    - Parse ONLY rows from the 5-column table.
    - Decide a device row by: first column is a number (consecutivo).
    - We do NOT deduplicate by consecutivo or registro_sanitario:
      if the PDF prints two rows, we keep two rows.
    - Description/date pages (2-column) are ignored for now.
    """

    rows = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page_index, page in enumerate(pdf.pages):
            table = page.extract_table()
            if not table:
                continue

            for row in table:
                if not row or not any(row):
                    continue

                cells = [(c or "").strip() for c in row]

                # 2-column description table rows have len(cells) == 2,
                # 5-column device table rows have len(cells) == 5.
                # We only want 5-column rows here.
                if len(cells) != 5:
                    continue

                # For the first page, the first two rows are:
                #   - combined header text
                #   - "ENERO ENERO ENERO..." month row
                # They don't start with a numeric consecutivo, so this check
                # naturally skips them.
                if not cells[0].isdigit():
                    continue

                # Pad to 5 just in case (defensive; should already be 5)
                while len(cells) < 5:
                    cells.append("")

                rows.append(
                    dict(
                        country="MX",
                        year=2017,
                        source_file=source_name,
                        consecutivo=cells[0] or None,
                        registro_sanitario=cells[1] or None,
                        denominacion_distintiva=cells[2] or None,
                        holder=cells[3] or None,
                        denominacion_generica=None,
                        categoria=None,
                        clase=cells[4] or None,
                        fecha_emision=None,  # could be enriched later from 2-col table
                        details=None,        # could also be enriched later
                        raw_line=None,
                    )
                )

    return rows


# ---------------------------------------------------------------------
# 2018–2019 parser
# ---------------------------------------------------------------------

def parse_2018_2019(pdf_bytes: bytes, year: int, source_name: str):
    """
    2018 & 2019 PDFs:
    pdfplumber gives a 5-column table like:
    [Consecutivo, Registro, (big composite cell), Fecha, Denom. genérica]

    We extract:
    - consecutivo
    - registro_sanitario
    - generic name (last column)
    - fecha_emision (from any dd/mm/yyyy)
    - details = the long composite column (indication of use etc.)
    """
    rows = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            for raw_row in table:
                if not raw_row or not any(raw_row):
                    continue

                cells = [(c or "") for c in raw_row]

                # skip obvious header rows
                first = cells[0].lower()
                if (
                    "conse" in first
                    or "consecutivo" in first
                    or ("registro" in " ".join(cells).lower() and not cells[0].strip().isdigit())
                ):
                    continue

                while len(cells) < 3:
                    cells.append("")

                consecutivo = cells[0].strip() or None
                registro = cells[1].strip() or None
                generic = (cells[-1].strip() or None) if len(cells) >= 3 else None

                all_text = " ".join(c.strip() for c in cells[2:])
                m_date = re.search(r"\d{2}/\d{2}/\d{4}", all_text)
                fecha = m_date.group(0) if m_date else None

                rows.append(
                    dict(
                        country="MX",
                        year=year,
                        source_file=source_name,
                        consecutivo=consecutivo,
                        registro_sanitario=registro,
                        denominacion_distintiva=None,
                        holder=None,  # usually buried in the composite cell
                        denominacion_generica=generic,
                        categoria=None,
                        clase=None,
                        fecha_emision=fecha,
                        details=all_text or None,
                        raw_line=None,
                    )
                )

    return rows


# ---------------------------------------------------------------------
# 2020–2023 parser – messy single-column text tables
# ---------------------------------------------------------------------

def parse_singlecol_weird(pdf_bytes: bytes, year: int, source_name: str):
    """
    2020–2023 PDFs: pdfplumber usually gives a 1-column "table"
    where each row is long concatenated text.

    We keep it simple:
    - raw_line: entire row text
    - consecutivo: number after a line break (e.g. '\n0241 ')
    - registro_sanitario: something like '0241R2021 SSA'
    - fecha_emision: first dd/mm/yyyy
    Everything else stays in raw_line/details for later manual/advanced parsing.
    """
    rows = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            for i, raw_row in enumerate(table):
                if not raw_row or not any(raw_row):
                    continue

                text = (raw_row[0] or "").strip()
                if not text:
                    continue

                # skip header row if present
                if i == 0 and "razón social" in text.lower():
                    continue
                if i == 0 and "consecutivo" in text.lower():
                    continue

                # consecutivo pattern (often after a newline)
                m_consec = re.search(r"\n(\d{3,4})\s", text)
                consecutivo = m_consec.group(1) if m_consec else None

                # registration pattern like '0241R2021 SSA', '0241E2021 SSA', etc.
                m_reg = re.search(r"\b(\d{3,4}[A-Z]\d{4}\s+SSA)\b", text)
                registro = m_reg.group(1) if m_reg else None

                m_date = re.search(r"\d{2}/\d{2}/\d{4}", text)
                fecha = m_date.group(0) if m_date else None

                rows.append(
                    dict(
                        country="MX",
                        year=year,
                        source_file=source_name,
                        consecutivo=consecutivo,
                        registro_sanitario=registro,
                        denominacion_distintiva=None,
                        holder=None,
                        denominacion_generica=None,
                        categoria=None,
                        clase=None,
                        fecha_emision=fecha,
                        details=None,
                        raw_line=text,
                    )
                )

    return rows


# ---------------------------------------------------------------------
# 2024+ parser – nice wide tables
# ---------------------------------------------------------------------

def parse_2024plus(pdf_bytes: bytes, year: int, source_name: str):
    """
    2024 & 2025 PDFs are nicer: wide tables with real columns.

    Header roughly:
    [Consecutivo, Razón Social, No. Registro Sanitario,
     Denominación distintiva, Denominación genérica,
     Categoria, Clase, Fecha de emisión]
    """
    rows = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            header = [c or "" for c in table[0]]
            norm = [h.lower().replace("\n", " ") for h in header]

            idx = {}
            for i, h in enumerate(norm):
                if "consecutivo" in h:
                    idx["consecutivo"] = i
                if "razón social" in h or "razon social" in h:
                    idx["holder"] = i
                if "registro" in h and "sanitario" in h:
                    idx["registro"] = i
                if "denominación" in h and "genérica" in h:
                    idx["denom_generica"] = i
                if "denominación" in h and "distintiva" in h:
                    idx["denom_dist"] = i
                if "categoria" in h or "categoría" in h:
                    idx["categoria"] = i
                if "clase" in h:
                    idx["clase"] = i
                if "fecha" in h:
                    idx["fecha"] = i

            def get(cells, key):
                j = idx.get(key)
                if j is None or j >= len(cells):
                    return None
                return (cells[j] or "").strip() or None

            # data rows
            for raw_row in table[1:]:
                if not raw_row or not any(raw_row):
                    continue
                cells = [c or "" for c in raw_row]

                rows.append(
                    dict(
                        country="MX",
                        year=year,
                        source_file=source_name,
                        consecutivo=get(cells, "consecutivo"),
                        registro_sanitario=get(cells, "registro"),
                        denominacion_distintiva=get(cells, "denom_dist"),
                        holder=get(cells, "holder"),
                        denominacion_generica=get(cells, "denom_generica"),
                        categoria=get(cells, "categoria"),
                        clase=get(cells, "clase"),
                        fecha_emision=get(cells, "fecha"),
                        details=None,
                        raw_line=None,
                    )
                )

    return rows


# ---------------------------------------------------------------------
# Main: iterate through all PDFs and build a single DataFrame
# ---------------------------------------------------------------------

def build_mexico_devices():
    all_rows = []

    files_and_years = {
        "reg_dm_2017.pdf": 2017,
        "Registros_Sanitarios_DM2018.pdf": 2018,
        "Registros_Sanitarios_DM2019.pdf": 2019,
        "Registros_Sanitarios_DM2020.pdf": 2020,
        "Registros_Sanitarios_DM2021-1.pdf": 2021,
        "Registros_Sanitarios_DM2022-1.pdf": 2022,
        "Registros_Sanitarios_DM2023.pdf": 2023,
        "Registros_Sanitarios_DM_2024.pdf": 2024,
        "Registros_Sanitarios_DM_abr_2025.pdf": 2025,
    }

    for fname, year in files_and_years.items():
        path = RAW_DIR / fname
        if not path.exists():
            print(f"[WARN] {path} does not exist, skipping.")
            continue

        print(f"Parsing {fname} (year={year})...")
        pdf_bytes = _load_pdf_bytes(path)

        if year == 2017:
            rows = parse_2017(pdf_bytes, fname)
        elif year in (2018, 2019):
            rows = parse_2018_2019(pdf_bytes, year, fname)
        elif year in (2020, 2021, 2022, 2023):
            rows = parse_singlecol_weird(pdf_bytes, year, fname)
        else:  # 2024, 2025+
            rows = parse_2024plus(pdf_bytes, year, fname)

        print(f"  -> {len(rows)} rows")
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows)

    # Internal unique row id: built around Registro Sanitario as the conceptual key,
    # but we do NOT deduplicate – every PDF row gets its own entry.
    df["entry_id"] = (
        df["country"].astype(str)
        + "_"
        + df["year"].astype(str)
        + "_"
        + df["registro_sanitario"].fillna("").astype(str)
        + "_"
        + df.groupby(["country", "year", "registro_sanitario"]).cumcount().astype(str)
    )

    # Basic clean-up: normalize empty strings to NA
    for col in [
        "consecutivo",
        "registro_sanitario",
        "holder",
        "denominacion_distintiva",
        "denominacion_generica",
        "categoria",
        "clase",
        "fecha_emision",
    ]:
        if col in df.columns:
            df[col] = df[col].replace("", pd.NA)

    print(f"Total rows: {len(df)}")

    # Save Parquet (optional dependency: pyarrow or fastparquet)
    try:
        df.to_parquet(PARQUET_PATH, index=False)
        print(f"Saved Parquet to {PARQUET_PATH}")
    except ImportError:
        print(
            "WARNING: Could not write Parquet (missing pyarrow/fastparquet). "
            "SQLite file will still be created."
        )

    # Save SQLite
    conn = sqlite3.connect(SQLITE_PATH)
    df.to_sql("mexico_devices", conn, if_exists="replace", index=False)

    # Indexes for faster lookups; registro_sanitario is the main identifier in analysis
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_mexico_registro "
        "ON mexico_devices (registro_sanitario)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_mexico_country_year "
        "ON mexico_devices (country, year)"
    )
    conn.commit()
    conn.close()
    print(f"Saved SQLite DB to {SQLITE_PATH} (table: mexico_devices)")


if __name__ == "__main__":
    build_mexico_devices()
