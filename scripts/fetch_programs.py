"""
Fetches the current program list from api.itms21.sk and syncs it into a
DuckDB table: programs already known are always rewritten with the latest
API data (full overwrite of that row), new programs are inserted, and
programs that no longer appear in the API response are left untouched
(the table is never truncated, so nothing is ever deleted).

Exports the current table to docs/program_data.json for the published web page.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests

API_URL = "https://api.itms21.sk/public/v1/program?limit=-1"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate table inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file
JSON_OUT_PATH = Path("docs/program_data.json")

TABLE_PREFIX = "itms21_"
TABLE_CURRENT = f"{TABLE_PREFIX}programs_current"


def fetch_programs() -> list[dict]:
    """Call the API and return the list of program records."""
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    return payload["results"]


def flatten_program(d: dict) -> dict:
    """Flatten one program record into a single flat row."""
    typ_programu = d.get("typProgramu") or {}
    riadiaci_organ = d.get("riadiaciOrgan") or {}
    subjekt = riadiaci_organ.get("subjekt") or {}
    adresa = subjekt.get("adresa") or {}

    suma_eu = d.get("sumaEu")
    suma_sr = d.get("sumaSr")
    suma_spolu = d.get("sumaSpolu")

    return {
        "program_id": d.get("id"),
        "kod": d.get("kod"),
        "nazov_sk": d.get("nazovSk"),
        "nazov_en": d.get("nazovEn"),
        "skratka": d.get("skratka"),
        "suma_eu": float(suma_eu) if suma_eu is not None else None,
        "suma_sr": float(suma_sr) if suma_sr is not None else None,
        "suma_spolu": float(suma_spolu) if suma_spolu is not None else None,
        "kod_cci": d.get("kodCCI"),
        "typ_programu": typ_programu.get("typ"),
        "riadiaci_organ": riadiaci_organ.get("nazov"),
        "subjekt_nazov": subjekt.get("nazov"),
        "ico": subjekt.get("ico"),
        "obec": adresa.get("obec"),
        "created_at": d.get("createdAt"),
        "updated_at": d.get("updatedAt"),
    }


TABLE_COMMENTS: dict[str, str] = {
    TABLE_CURRENT: (
        "Current snapshot of every Operational Programme of the 2021-2027 "
        "programming period, as returned by the ITMS21 API. Rows are fully "
        "overwritten on every sync run, so this always reflects the latest "
        "published data; nothing is ever deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_CURRENT: {
        "program_id": "ITMS21 numeric id of the programme.",
        "kod": "Short programme code (e.g. 'PSK', 'PVaI').",
        "nazov_sk": "Full programme name in Slovak.",
        "nazov_en": "Full programme name in English.",
        "skratka": "Programme abbreviation.",
        "suma_eu": "Total EU-fund allocation for the programme, in euro.",
        "suma_sr": "Total Slovak national co-financing allocation, in euro.",
        "suma_spolu": "Total programme allocation (EU + national co-financing), in euro.",
        "kod_cci": "EU Commission's CCI reference code identifying the programme.",
        "typ_programu": "Programme type (e.g. national operational programme, cross-border cooperation).",
        "riadiaci_organ": "Name of the managing authority responsible for the programme.",
        "subjekt_nazov": "Name of the institution acting as the managing authority.",
        "ico": "Slovak company registration number (ICO) of the managing authority.",
        "obec": "Municipality/city of the managing authority's registered address.",
        "created_at": "Record creation timestamp in the source system (epoch milliseconds).",
        "updated_at": "Record last-updated timestamp in the source system (epoch milliseconds).",
    },
}


def _esc(value: str) -> str:
    """Escape a string for embedding in a single-quoted SQL literal."""
    return value.replace("'", "''")


def apply_comments(con: duckdb.DuckDBPyConnection) -> None:
    """Attach English COMMENT ON metadata to every table/column above. Safe to
    re-run on every invocation - COMMENT ON simply overwrites."""
    for table, comment in TABLE_COMMENTS.items():
        con.execute(f"COMMENT ON TABLE {table} IS '{_esc(comment)}'")
    for table, columns in COLUMN_COMMENTS.items():
        for column, comment in columns.items():
            con.execute(f"COMMENT ON COLUMN {table}.{column} IS '{_esc(comment)}'")


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    # One-time rename from the pre-"itms21_"-prefix table name; a no-op once
    # the rename has happened (ALTER TABLE IF EXISTS is idempotent).
    con.execute(f"ALTER TABLE IF EXISTS programs_current RENAME TO {TABLE_CURRENT}")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CURRENT} (
            program_id       INTEGER PRIMARY KEY,
            kod              VARCHAR,
            nazov_sk         VARCHAR,
            nazov_en         VARCHAR,
            skratka          VARCHAR,
            suma_eu          DOUBLE,
            suma_sr          DOUBLE,
            suma_spolu       DOUBLE,
            kod_cci          VARCHAR,
            typ_programu     VARCHAR,
            riadiaci_organ   VARCHAR,
            subjekt_nazov    VARCHAR,
            ico              VARCHAR,
            obec             VARCHAR,
            created_at       BIGINT,
            updated_at       BIGINT
        )
    """)


def upsert_row(con: duckdb.DuckDBPyConnection, row: dict) -> None:
    con.execute(f"""
        INSERT INTO {TABLE_CURRENT} (
            program_id, kod, nazov_sk, nazov_en, skratka,
            suma_eu, suma_sr, suma_spolu, kod_cci, typ_programu,
            riadiaci_organ, subjekt_nazov, ico, obec,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (program_id) DO UPDATE SET
            kod = excluded.kod,
            nazov_sk = excluded.nazov_sk,
            nazov_en = excluded.nazov_en,
            skratka = excluded.skratka,
            suma_eu = excluded.suma_eu,
            suma_sr = excluded.suma_sr,
            suma_spolu = excluded.suma_spolu,
            kod_cci = excluded.kod_cci,
            typ_programu = excluded.typ_programu,
            riadiaci_organ = excluded.riadiaci_organ,
            subjekt_nazov = excluded.subjekt_nazov,
            ico = excluded.ico,
            obec = excluded.obec,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at
    """, [
        row["program_id"], row["kod"], row["nazov_sk"], row["nazov_en"], row["skratka"],
        row["suma_eu"], row["suma_sr"], row["suma_spolu"], row["kod_cci"], row["typ_programu"],
        row["riadiaci_organ"], row["subjekt_nazov"], row["ico"], row["obec"],
        row["created_at"], row["updated_at"],
    ])


def sync_programs() -> tuple[int, int]:
    """Fetch the list and upsert every record: existing ids are fully rewritten,
    new ids are inserted, and ids missing from this response are left as-is.

    Returns (total_in_list, synced_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_table(con)
    apply_comments(con)

    print("Fetching program list...")
    records = fetch_programs()
    print(f"List returned {len(records)} programs.")

    for record in records:
        row = flatten_program(record)
        upsert_row(con, row)

    con.commit()
    con.close()

    return len(records), len(records)


def export_to_json() -> None:
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    df = con.execute(f"SELECT * FROM {TABLE_CURRENT} ORDER BY suma_spolu DESC").fetchdf()
    con.close()

    JSON_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    date_cols = ["created_at", "updated_at"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")

    export = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "programs": json.loads(df.to_json(orient="records", date_format="iso")),
    }

    with open(JSON_OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)

    print(f"Exported {len(df)} programs to {JSON_OUT_PATH}")


def main():
    total, synced = sync_programs()
    print(f"Done. {total} total in list, {synced} synced.")
    export_to_json()


if __name__ == "__main__":
    main()
