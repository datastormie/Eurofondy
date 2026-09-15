"""
Fetches full detail for ITMS21 "dodavatelobstaravatel" (public procurement
supplier/contractor) records. Like fetch_intenzitaprojekt.py, there is no
list endpoint here - the set of ids to fetch is read directly from four
sibling tables' foreign-key columns, all populated by other fetch scripts
that already reference a supplier by id without ever having fetched that
supplier's own detail:

  - itms21_doklaductovny.dodavateldodavatelobstaravatel_id       (fetch_uctovnydoklad.py)
  - itms21_verejneobstaravanie_detail.dodavatelobstaravatel_id   (fetch_verejneobstaravanie.py)
  - itms21_zmluvaverejneobstaravanie_dodavatelia.dodavateldodavatelobstaravatel_id (fetch_zmluvyverejnehoobstaravania.py)
  - itms21_zmluvaverejneobstaravanie.hlavnydodavateldodavatelobstaravatel_id       (fetch_zmluvyverejnehoobstaravania.py)

Detail is fetched ONLY for ids not yet stored in itms21_dodavatelobstaravatel;
each id is fetched exactly once and never re-fetched, updated, or deleted
afterward (purely additive incremental sync, same approach as
fetch_aktivitaprojekt.py).

Hard dependency: fetch_uctovnydoklad.py, fetch_verejneobstaravanie.py, and
fetch_zmluvyverejnehoobstaravania.py must all have already run at least once
against this DuckDB file, since their tables are this script's only source of
ids. If any of those tables don't exist yet, this script raises immediately
instead of silently doing nothing.

"dodavatelobstaravatel (detail)" is a flat record (no inner list fields), so
it's stored as a single itms21_dodavatelobstaravatel table with no child
tables.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml), right after
fetch_zmluvyverejnehoobstaravania.py (the last of its three source scripts).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/dodavatelobstaravatel/id/{id}"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate table inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_DODAVATELOBSTARAVATEL = f"{TABLE_PREFIX}dodavatelobstaravatel"

# (source table, source column) pairs to union for the set of ids to fetch.
SOURCE_COLUMNS: list[tuple[str, str]] = [
    (f"{TABLE_PREFIX}doklaductovny", "dodavateldodavatelobstaravatel_id"),
    (f"{TABLE_PREFIX}verejneobstaravanie_detail", "dodavatelobstaravatel_id"),
    (f"{TABLE_PREFIX}zmluvaverejneobstaravanie_dodavatelia", "dodavateldodavatelobstaravatel_id"),
    (f"{TABLE_PREFIX}zmluvaverejneobstaravanie", "hlavnydodavateldodavatelobstaravatel_id"),
]

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2


def fetch_detail(dodavatel_id: int) -> dict | None:
    """Call the detail endpoint for one dodavatelobstaravatel, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=dodavatel_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={dodavatel_id} after {RETRY_ATTEMPTS} attempts: {e}")
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


# Mirrors the ITMS21 "dodavatelobstaravatel" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).
DODAVATELOBSTARAVATEL_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("nazov", "nazov", "VARCHAR"),
    ("ico", "ico", "VARCHAR"),
    ("dic", "dic", "VARCHAR"),
    ("ineidentifikacnecislo", "ineIdentifikacneCislo", "VARCHAR"),
    ("platiteldph", "platitelDph", "BOOLEAN"),
    ("typinehoidentifikatora_id", "typInehoIdentifikatora.id", "BIGINT"),
    ("adresa_ulica", "adresa.ulica", "VARCHAR"),
    ("adresa_cislo", "adresa.cislo", "VARCHAR"),
    ("adresa_psc", "adresa.psc", "VARCHAR"),
    ("adresa_obec", "adresa.obec", "VARCHAR"),
    ("adresa_stat_id", "adresa.stat.id", "BIGINT"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
]


TABLE_COMMENTS: dict[str, str] = {
    TABLE_DODAVATELOBSTARAVATEL: (
        "One row per public procurement supplier/contractor "
        "('dodávateľ/obstarávateľ'), referenced by accounting documents, "
        "procurement details, and procurement contracts. Ids to fetch come "
        "from four sibling tables' foreign-key columns (see the module "
        "docstring), not from a list endpoint of its own. Purely additive: "
        "once an id is stored it is never re-fetched, updated, or deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_DODAVATELOBSTARAVATEL: {
        "id": "ITMS21 numeric id of the supplier/contractor.",
        "href": "API URL of this supplier/contractor's own detail resource.",
        "nazov": "Supplier/contractor name.",
        "ico": "Business identification number (IČO).",
        "dic": "Tax identification number (DIČ).",
        "ineidentifikacnecislo": "Other identification number, used when neither ICO nor DIC applies.",
        "platiteldph": "Whether the supplier/contractor is a registered VAT payer.",
        "typinehoidentifikatora_id": "Id of the type of the 'other identification number', when ineidentifikacnecislo is used.",
        "adresa_ulica": "Street of the supplier/contractor's registered address.",
        "adresa_cislo": "Street/building number of the supplier/contractor's registered address.",
        "adresa_psc": "Postal code of the supplier/contractor's registered address.",
        "adresa_obec": "Municipality of the supplier/contractor's registered address.",
        "adresa_stat_id": "Id of the country of the supplier/contractor's registered address.",
        "createdat": "Record creation timestamp in the source system.",
        "updatedat": "Record last-updated timestamp in the source system.",
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


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM information_schema.tables WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?)",
        [DB_SCHEMA, name],
    ).fetchone() is not None


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in DODAVATELOBSTARAVATEL_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_DODAVATELOBSTARAVATEL} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)


def store_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Insert one dodavatelobstaravatel's detail JSON into
    itms21_dodavatelobstaravatel. Only ever called once per id (gated by
    get_known_ids in sync_dodavatelobstaravatel), so this is a plain INSERT -
    never re-fetched, never updated."""
    columns_sql = ", ".join(col for col, _, _ in DODAVATELOBSTARAVATEL_COLUMNS)
    placeholders = ", ".join("?" for _ in DODAVATELOBSTARAVATEL_COLUMNS)
    values = [_get(detail, path) for _, path, _ in DODAVATELOBSTARAVATEL_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_DODAVATELOBSTARAVATEL} ({columns_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO NOTHING",
        values,
    )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_DODAVATELOBSTARAVATEL}").fetchall()
    return {row[0] for row in rows}


def get_source_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids to fetch, read from the union of the four sibling tables'
    foreign-key columns listed in SOURCE_COLUMNS. Raises if any of those
    tables don't exist yet - this script has no list endpoint of its own and
    depends entirely on their fetch scripts having run first."""
    for table, _ in SOURCE_COLUMNS:
        if not _table_exists(con, table):
            raise RuntimeError(
                f"{DB_SCHEMA}.{table} does not exist. Run fetch_uctovnydoklad.py, "
                "fetch_verejneobstaravanie.py, and fetch_zmluvyverejnehoobstaravania.py "
                "first - together they populate the tables that are the only source "
                "of ids for fetch_dodavatelobstaravatel.py."
            )

    union_sql = " UNION ".join(
        f"SELECT {column} AS id FROM {table} WHERE {column} IS NOT NULL"
        for table, column in SOURCE_COLUMNS
    )
    rows = con.execute(union_sql).fetchall()
    return {row[0] for row in rows}


def sync_dodavatelobstaravatel() -> tuple[int, int, int]:
    """Fetch full details only for ids referenced by the source tables not
    already stored in itms21_dodavatelobstaravatel.

    Returns (total_source_ids, fetched_count, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_table(con)
    apply_comments(con)

    source_ids = get_source_ids(con)
    print(f"Source tables reference {len(source_ids)} distinct dodavatelobstaravatel id(s).")

    known_ids = get_known_ids(con)
    to_fetch = [i for i in source_ids if i not in known_ids]

    print(f"{len(to_fetch)} new dodavatelobstaravatel(s) to fetch; "
          f"{len(source_ids) - len(to_fetch)} already stored (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, did): did for did in to_fetch}
            for i, future in enumerate(as_completed(future_to_id), start=1):
                detail = future.result()
                if detail is None:
                    failed_count += 1
                    continue
                store_detail(con, detail)
                fetched_count += 1

                if i % 50 == 0 or i == len(to_fetch):
                    con.commit()  # periodic commit so progress survives an interruption
                    print(f"  Progress: {i}/{len(to_fetch)} processed "
                          f"({fetched_count} ok, {failed_count} failed)")

    con.commit()
    con.close()

    return len(source_ids), fetched_count, failed_count


def main():
    total, fetched, failed = sync_dodavatelobstaravatel()
    print(f"Done. {total} total ids referenced, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
