#!/usr/bin/env python
"""
Builds a Japan (JP) medical devices SQLite database from the PMDA
認証品目リスト Excel file.

Usage:
    python pmda_japan_build_db.py \
        --db-path data/jp_pmda_devices.sqlite \
        --excel-path data/raw/pmda_certified_devices.xlsx
"""

from __future__ import annotations

import argparse
import datetime as dt
import pathlib
import sqlite3
from typing import Dict, List

import pandas as pd
import requests

PMDA_EXCEL_URL = "https://www.pmda.go.jp/files/000277537.xlsx"
# URL taken from the official PMDA page; Excel link is published
# alongside the PDF認証品目リスト.:contentReference[oaicite:1]{index=1}


def download_excel(url: str, out_path: pathlib.Path, force: bool = False) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and not force:
        print(f"[download] {out_path} already exists – skipping download")
        return

    print(f"[download] Fetching {url}")
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()
    with out_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    print(f"[download] Saved to {out_path}")


def read_pmda_excel(excel_path: pathlib.Path) -> pd.DataFrame:
    # Try a couple of headers just in case the first row is a title row.
    for header_row in (0, 1, 2):
        df = pd.read_excel(
            excel_path,
            sheet_name=0,
            header=header_row,
            dtype=str
        )
        if any(df.columns.astype(str).str.contains("認証番号")):
            print(f"[read] Using header row {header_row}")
            break
    # Drop rows that are completely empty
    df = df.dropna(how="all")

    # Ensure column labels are strings
    df.columns = [str(c).strip() for c in df.columns]

    return df


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map Japanese PMDA column names to English lower_snake_case.
    Unknown columns are kept with generic names like extra_col_1, etc.
    """
    new_cols: Dict[str, str] = {}
    extra_idx = 1
    corp_no_seen = 0

    for orig in df.columns:
        col = str(orig)

        if "Ｎｏ" in col or col.lower().startswith("no"):
            new_cols[orig] = "row_number"
        elif "認証機関コード" in col:
            new_cols[orig] = "certification_body_code"
        elif "認証番号" in col:
            new_cols[orig] = "certification_number"
        elif "認証年月日" in col:
            new_cols[orig] = "certification_date"
        elif "販売名" in col:
            new_cols[orig] = "brand_name"
        elif "一般的名称" in col:
            new_cols[orig] = "generic_name"
        elif "業者名_認証取得者" in col:
            new_cols[orig] = "certificate_holder_name"
        elif "業者名_選任外国製造医療機器等製造販売業者" in col:
            new_cols[orig] = "designated_foreign_holder_name"
        elif "法人番号" in col:
            corp_no_seen += 1
            if corp_no_seen == 1:
                new_cols[orig] = "certificate_holder_corporate_number"
            else:
                new_cols[orig] = "designated_foreign_holder_corporate_number"
        elif "承認からの移行認証" in col:
            new_cols[orig] = "transition_from_approval_flag"
        elif "承継品目" in col:
            new_cols[orig] = "succession_flag"
        elif "承継年月日" in col:
            new_cols[orig] = "succession_date"
        elif "承継時認証機関変更" in col:
            new_cols[orig] = "cert_body_changed_on_succession_flag"
        elif "認証整理日" in col:
            new_cols[orig] = "certification_discontinuation_date"
        elif "認証取消日" in col:
            new_cols[orig] = "certification_cancellation_date"
        else:
            # Generic but still English / snake_case
            new_cols[orig] = f"extra_col_{extra_idx}"
            extra_idx += 1

    df = df.rename(columns=new_cols)
    return df


def normalize_flags(df: pd.DataFrame, colnames: List[str]) -> pd.DataFrame:
    """
    Convert columns like '○' / '' into '1' / '0'.
    Any non-empty non-○ value is treated as '1' conservatively.
    """

    def parse_flag(val):
        if pd.isna(val):
            return None
        s = str(val).strip()
        if not s:
            return None
        # Common "true" symbols in Japanese tables: ○, 〇, '1', 'Y'
        if s in {"○", "〇", "1", "Y", "YES", "Yes"}:
            return "1"
        return "1"  # treat any non-empty as true; absence is null

    for c in colnames:
        if c in df.columns:
            df[c] = df[c].map(parse_flag)

    return df


def normalize_dates(df: pd.DataFrame, colnames: list[str]) -> pd.DataFrame:
    """
    Convert date-like columns to ISO 8601 strings (YYYY-MM-DD).
    Robust to:
      - mixed string formats (e.g., 2025/08/01, 01-Aug-2025, 2024-12-31)
      - Excel serial numbers (e.g., 45500)
      - blanks and None -> NULL
    """
    from dateutil import parser as duparser

    out = df.copy()

    for c in colnames:
        if c not in out.columns:
            continue

        # First pass: try pandas native parsing
        ser = pd.to_datetime(out[c], errors="coerce")

        # Fallback A: Excel serial numbers (days since 1899-12-30)
        need = ser.isna()
        if need.any():
            nums = pd.to_numeric(out.loc[need, c], errors="coerce")
            mask = nums.notna()
            if mask.any():
                base = pd.Timestamp("1899-12-30")
                ser.loc[need[need].index[mask]] = base + pd.to_timedelta(nums[mask], unit="D")

        # Fallback B: dateutil for things like "01-Aug-2025", locale-independent
        need = ser.isna()
        if need.any():
            def _parse(x):
                if pd.isna(x) or str(x).strip() == "":
                    return pd.NaT
                try:
                    return pd.Timestamp(duparser.parse(str(x), dayfirst=False, yearfirst=False))
                except Exception:
                    return pd.NaT
            ser.loc[need] = out.loc[need, c].apply(_parse)

        # Final: format to ISO or leave as NaT -> becomes NULL later
        out[c] = ser.dt.strftime("%Y-%m-%d")

    return out



def add_provenance(df: pd.DataFrame, source_url: str, source_file: str) -> pd.DataFrame:
    df = df.copy()
    df["country_code"] = "JP"
    df["source_url"] = source_url
    df["source_file"] = source_file
    df["ingested_at"] = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    return df


def deduplicate_with_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Implement Baynovation rules:

    * Clear duplicates (same license number + same device + same holder and
      all other fields identical) -> keep one row.
    * Ambiguous duplicates (same license number + same holder + same brand
      but some differing fields, e.g. dates) -> keep all rows and set
      duplicate_flag = 1.
    """

    df = df.copy()
    df["duplicate_flag"] = 0

    key_cols = [
        c for c in [
            "certification_number",
            "brand_name",
            "certificate_holder_name",
        ] if c in df.columns
    ]

    if not key_cols:
        # Nothing to key on; just drop exact row duplicates
        core_cols = [c for c in df.columns if c not in ("ingested_at",)]
        df = df.drop_duplicates(subset=core_cols, keep="first")
        return df

    groups = []
    compare_exclude = {
        "row_number",
        "ingested_at",
        "source_url",
        "source_file",
        "duplicate_flag",
    }

    for _, g in df.groupby(key_cols, dropna=False):
        if len(g) == 1:
            groups.append(g)
            continue

        compare_cols = [c for c in df.columns if c not in compare_exclude]

        # Check if all rows are identical over compare_cols
        same_all = True
        for c in compare_cols:
            if g[c].nunique(dropna=False) > 1:
                same_all = False
                break

        if same_all:
            # Clear duplicate: keep first row only
            g = g.iloc[[0]].copy()
        else:
            # Ambiguous: keep all and flag
            g = g.copy()
            g["duplicate_flag"] = 1

        groups.append(g)

    return pd.concat(groups, ignore_index=True)


def write_sqlite(df: pd.DataFrame, db_path: pathlib.Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT,
                certification_body_code TEXT,
                certification_number TEXT,
                certification_date TEXT,
                brand_name TEXT,
                generic_name TEXT,
                certificate_holder_name TEXT,
                certificate_holder_corporate_number TEXT,
                designated_foreign_holder_name TEXT,
                designated_foreign_holder_corporate_number TEXT,
                transition_from_approval_flag TEXT,
                succession_flag TEXT,
                succession_date TEXT,
                cert_body_changed_on_succession_flag TEXT,
                certification_discontinuation_date TEXT,
                certification_cancellation_date TEXT,
                row_number TEXT,
                source_url TEXT,
                source_file TEXT,
                ingested_at TEXT,
                duplicate_flag INTEGER
            );
            """
        )

        # Replace table contents on each run
        cur.execute("DELETE FROM devices;")
        conn.commit()

        df.to_sql("devices", conn, if_exists="append", index=False)
        conn.commit()
    finally:
        conn.close()

def create_indexes(db_path):
    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("CREATE INDEX IF NOT EXISTS idx_devices_certnum ON devices(certification_number)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_devices_holder ON devices(certificate_holder_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_devices_brand  ON devices(brand_name)")
        conn.commit()
    finally:
        conn.close()



def build_pipeline(db_path: pathlib.Path, excel_path: pathlib.Path, force_download: bool) -> None:
    download_excel(PMDA_EXCEL_URL, excel_path, force=force_download)
    df = read_pmda_excel(excel_path)
    df = normalize_column_names(df)

    # Normalize flags and dates
    df = normalize_flags(
        df,
        [
            "transition_from_approval_flag",
            "succession_flag",
            "cert_body_changed_on_succession_flag",
        ],
    )

    df = normalize_dates(
        df,
        [
            "certification_date",
            "succession_date",
            "certification_discontinuation_date",
            "certification_cancellation_date",
        ],
    )

    # Add provenance
    df = add_provenance(df, PMDA_EXCEL_URL, excel_path.name)

    # Deduplicate
    df = deduplicate_with_flags(df)

    # Replace NaN with None so SQLite gets NULLs
    df = df.where(pd.notnull(df), None)

    # Write to SQLite
    write_sqlite(df, db_path)
    create_indexes(db_path)
    import json, sqlite3, os
    os.makedirs("reports", exist_ok=True)
    con = sqlite3.connect(db_path)
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
    with open("reports/jp_pmda_qa.json","w",encoding="utf-8") as f:
        json.dump({
        "row_count": row_count,
        "null_certification_number": null_cert,
        "duplicate_groups": dupe_groups,
        "min_certification_date": min_dt,
        "max_certification_date": max_dt
        }, f, ensure_ascii=False, indent=2)
    print("[qa] wrote reports/jp_pmda_qa.json")  
    print(f"[done] Wrote {len(df)} rows to {db_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build JP PMDA devices SQLite DB.")
    parser.add_argument(
        "--db-path",
        type=pathlib.Path,
        default=pathlib.Path("data/jp_pmda_devices.sqlite"),
        help="Output SQLite database path.",
    )
    parser.add_argument(
        "--excel-path",
        type=pathlib.Path,
        default=pathlib.Path("data/raw/pmda_certified_devices.xlsx"),
        help="Local path for the downloaded PMDA Excel file.",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download the Excel file even if it already exists.",
    )
    return parser.parse_args()

def read_pmda_excel(excel_path: pathlib.Path) -> pd.DataFrame:
    found = False
    for header_row in (0, 1, 2):
        df = pd.read_excel(excel_path, sheet_name=0, header=header_row, dtype=str)
        if any(df.columns.astype(str).str.contains("認証番号")):
            print(f"[read] Using header row {header_row}")
            found = True
            break
    if not found:
        raise ValueError("Could not locate PMDA header row containing '認証番号'.")
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    return df


if __name__ == "__main__":
    args = parse_args()
    build_pipeline(args.db_path, args.excel_path, args.force_download)
