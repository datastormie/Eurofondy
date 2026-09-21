"""
Fetches the current program list from api.itms21.sk and, for any program id
not yet stored, fetches full detail and decomposes it into the normalized
itms21_program / itms21_program_graf tables in the slovakia schema. Purely
additive: once a program id is stored it is never re-fetched, updated, or
deleted.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

API_URL = "https://api.itms21.sk/public/v1/program?limit=-1"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/program/id/{id}"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate table inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_PROGRAM = f"{TABLE_PREFIX}program"

MAX_WORKERS = 8           # concurrent detail requests — keep modest to avoid hammering the API
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2


def fetch_programs() -> list[dict]:
    """Call the API and return the list of program records."""
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    return payload["results"]


def fetch_detail(program_id: int) -> dict | None:
    """Call the detail endpoint for one program, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=program_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={program_id} after {RETRY_ATTEMPTS} attempts: {e}")
                return None
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    return None


def _get(d: dict | None, path: str):
    """Walk a dotted path through nested dicts; None if any segment is missing/None."""
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _t(bare_name: str) -> str:
    """Prefix a bare child-table name (as used as a CHILD_TABLES key) with TABLE_PREFIX."""
    return f"{TABLE_PREFIX}{bare_name}".lower()


# --- Full normalized schema -------------------------------------------------
# Mirrors the ITMS21 "program" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).

PROGRAM_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("kodcci", "kodCCI", "VARCHAR"),
    ("nazovde", "nazovDe", "VARCHAR"),
    ("nazoven", "nazovEn", "VARCHAR"),
    ("nazovsk", "nazovSk", "VARCHAR"),
    ("popis", "popis", "VARCHAR"),
    ("skratka", "skratka", "VARCHAR"),
    ("pocetplanovanychvyziev", "pocetPlanovanychVyziev", "INTEGER"),
    ("pocetprojektov", "pocetProjektov", "INTEGER"),
    ("pocetvyhlasenychvyziev", "pocetVyhlasenychVyziev", "INTEGER"),
    ("pocetzonfp", "pocetZonfp", "INTEGER"),
    ("sumaeu", "sumaEu", "DOUBLE"),
    ("sumasr", "sumaSr", "DOUBLE"),
    ("sumaspolu", "sumaSpolu", "DOUBLE"),
    ("riadiaciorgan_href", "riadiaciOrgan.href", "VARCHAR"),
    ("riadiaciorgan_id", "riadiaciOrgan.id", "BIGINT"),
    ("riadiaciorgan_kod", "riadiaciOrgan.kod", "VARCHAR"),
    ("riadiaciorgan_nazov", "riadiaciOrgan.nazov", "VARCHAR"),
    ("riadiaciorgan_subjekt_id", "riadiaciOrgan.subjekt.id", "BIGINT"),
    ("typprogramu_jedenfond", "typProgramu.jedenFond", "BOOLEAN"),
    ("typprogramu_makategoriureg", "typProgramu.maKategoriuReg", "BOOLEAN"),
    ("typprogramu_maopatrenia", "typProgramu.maOpatrenia", "BOOLEAN"),
    ("typprogramu_mapriority", "typProgramu.maPriority", "BOOLEAN"),
    ("typprogramu_typ", "typProgramu.typ", "VARCHAR"),
    ("createdat", "createdAt", "BIGINT"),
    ("updatedat", "updatedAt", "BIGINT"),
]

# table_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "PROGRAM_GRAF": ("graf", [
        ("kod", "kod", "VARCHAR"),
        ("nazovde", "nazovDe", "VARCHAR"),
        ("nazoven", "nazovEn", "VARCHAR"),
        ("nazovsk", "nazovSk", "VARCHAR"),
        ("alokacia", "alokacia", "DOUBLE"),
        ("cerpanie", "cerpanie", "DOUBLE"),
        ("zazmluvnene", "zazmluvnene", "DOUBLE"),
    ]),
}


TABLE_COMMENTS: dict[str, str] = {
    TABLE_PROGRAM: (
        "One row per Operational Programme ('program'), with the full detail "
        "record from the ITMS21 program/id/{id} endpoint. Purely additive: "
        "once an id is stored it is never re-fetched, updated, or deleted."
    ),
    _t("PROGRAM_GRAF"): (
        "Chart data points for the programme, broken down by fund (allocation "
        "vs. contracted vs. drawn amounts per fund). Child rows carrying "
        "program_id back to itms21_program; inserted once when the parent "
        "detail is first fetched, never updated/deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_PROGRAM: {
        "id": "ITMS21 numeric id of the programme.",
        "href": "API URL of this programme's own detail resource.",
        "kod": "Programme code.",
        "kodcci": "EU Commission's CCI reference code identifying the programme.",
        "nazovde": "Programme name in German.",
        "nazoven": "Programme name in English.",
        "nazovsk": "Programme name in Slovak.",
        "popis": "Programme description.",
        "skratka": "Programme abbreviation.",
        "pocetplanovanychvyziev": "Number of planned calls for the programme.",
        "pocetprojektov": "Number of projects funded under the programme.",
        "pocetvyhlasenychvyziev": "Number of calls announced for the programme.",
        "pocetzonfp": "Number of grant applications ('zonfp') submitted under the programme.",
        "sumaeu": "Total EU-fund allocation for the programme, in euro.",
        "sumasr": "Total Slovak national co-financing allocation, in euro.",
        "sumaspolu": "Total programme allocation (EU + national co-financing), in euro.",
        "riadiaciorgan_href": "API URL of the managing authority resource.",
        "riadiaciorgan_id": "Id of the managing authority responsible for the programme.",
        "riadiaciorgan_kod": "Code of the managing authority.",
        "riadiaciorgan_nazov": "Name of the managing authority.",
        "riadiaciorgan_subjekt_id": "Id of the institution acting as the managing authority.",
        "typprogramu_jedenfond": "Whether the programme is funded from a single fund.",
        "typprogramu_makategoriureg": "Whether the programme has a region category assigned.",
        "typprogramu_maopatrenia": "Whether the programme defines measures ('opatrenia').",
        "typprogramu_mapriority": "Whether the programme defines priority axes.",
        "typprogramu_typ": "Programme type (e.g. national operational programme, cross-border cooperation).",
        "createdat": "Record creation timestamp in the source system (epoch milliseconds).",
        "updatedat": "Record last-updated timestamp in the source system (epoch milliseconds).",
    },
    _t("PROGRAM_GRAF"): {
        "program_id": "Id of the parent programme (itms21_program.id).",
        "kod": "Code of the fund this chart data point covers.",
        "nazovde": "Fund name in German.",
        "nazoven": "Fund name in English.",
        "nazovsk": "Fund name in Slovak.",
        "alokacia": "Allocated amount for this fund, in euro.",
        "cerpanie": "Drawn/disbursed amount for this fund, in euro.",
        "zazmluvnene": "Contracted amount for this fund, in euro.",
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


def ensure_full_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create PROGRAM and its PROGRAM_GRAF child table if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in PROGRAM_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_PROGRAM} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                program_id BIGINT,
                {cols_sql}
            )
        """)


def store_full_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Decompose one program's full detail JSON into PROGRAM + PROGRAM_GRAF.
    Only ever called once per program id (gated by get_known_ids in
    sync_programs), so this is a plain INSERT — never re-fetched, never updated."""
    program_id = detail.get("id")

    columns_sql = ", ".join(col for col, _, _ in PROGRAM_COLUMNS)
    placeholders = ", ".join("?" for _ in PROGRAM_COLUMNS)
    values = [_get(detail, path) for _, path, _ in PROGRAM_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_PROGRAM} ({columns_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO NOTHING",
        values,
    )

    for table, (source_field, columns) in CHILD_TABLES.items():
        items = detail.get(source_field) or []
        if not items:
            continue
        columns_sql = ", ".join(col for col, _, _ in columns)
        placeholders = ", ".join("?" for _ in columns)
        rows = [
            [program_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (program_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already stored in the full detail schema, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_PROGRAM}").fetchall()
    return {row[0] for row in rows}


def sync_programs() -> tuple[int, int, int, int]:
    """Fetch the list, then fetch full detail only for ids not yet stored in
    the normalized itms21_program schema (purely additive).

    Returns (total_in_list, total_in_list, detail_fetched_count, detail_failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_full_schema(con)
    apply_comments(con)

    print("Fetching program list...")
    records = fetch_programs()
    print(f"List returned {len(records)} programs.")

    known_ids = get_known_ids(con)
    to_fetch = [record.get("id") for record in records if record.get("id") not in known_ids]
    print(f"{len(to_fetch)} new program(s) to fetch full detail for; "
          f"{len(records) - len(to_fetch)} already stored (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, pid): pid for pid in to_fetch}
            for i, future in enumerate(as_completed(future_to_id), start=1):
                pid = future_to_id[future]
                detail = future.result()
                if detail is None:
                    failed_count += 1
                    continue
                store_full_detail(con, detail)
                fetched_count += 1

                if i % 50 == 0 or i == len(to_fetch):
                    con.commit()
                    print(f"  Progress: {i}/{len(to_fetch)} processed "
                          f"({fetched_count} ok, {failed_count} failed)")

    con.commit()
    con.close()

    return len(records), len(records), fetched_count, failed_count


def main():
    total, synced, detail_fetched, detail_failed = sync_programs()
    print(f"Done. {total} total in list, {synced} synced, "
          f"{detail_fetched} newly fetched detail record(s), {detail_failed} failed.")


if __name__ == "__main__":
    main()
