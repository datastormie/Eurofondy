"""
Fetches the ITMS21 "aktivitaprojekt" (project activity) list, then fetches
full detail ONLY for ids not yet stored in DuckDB. The list itself is not
persisted - it exists purely to discover ids. Ids already stored are never
re-fetched and never deleted, even if they disappear from the API list
(purely additive incremental sync, same approach as fetch_zonfp.py /
fetch_vyzvy.py / fetch_projects.py).

Unlike those endpoints, "aktivitaprojekt (detail)" is a flat record (no
inner list fields), so it's stored as a single itms21_aktivitaprojekt table
with no child tables.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/aktivitaprojekt"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/aktivitaprojekt/id/{id}"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate table inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_AKTIVITAPROJEKT = f"{TABLE_PREFIX}aktivitaprojekt"

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

LIST_REQUEST_TIMEOUT = 60

# This list has ~110k records - far more than any other endpoint in this repo.
# A single limit=-1 request gets its connection forcibly reset by the server
# before it completes, so the list is fetched in pages instead.
LIST_PAGE_SIZE = 5000


def fetch_list_page(offset: int) -> list[dict]:
    """Fetch one page of the aktivitaprojekt list, with retry on failure."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(
                LIST_URL, params={"offset": offset, "limit": LIST_PAGE_SIZE}, timeout=LIST_REQUEST_TIMEOUT
            )
            resp.raise_for_status()
            return resp.json()["results"]
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                raise
            print(f"  List page fetch failed (offset={offset}, attempt {attempt}/{RETRY_ATTEMPTS}): {e}")
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)


def fetch_list() -> list[dict]:
    """Page through the list endpoint and return all aktivitaprojekt summary
    records (used only to discover ids - the list itself is not stored)."""
    results = []
    offset = 0
    while True:
        page = fetch_list_page(offset)
        if not page:
            break
        results.extend(page)
        print(f"  Fetched {len(results)} aktivitaprojekt so far...")
        if len(page) < LIST_PAGE_SIZE:
            break
        offset += LIST_PAGE_SIZE
    return results


def fetch_detail(aktivitaprojekt_id: int) -> dict | None:
    """Call the detail endpoint for one aktivitaprojekt, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=aktivitaprojekt_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={aktivitaprojekt_id} after {RETRY_ATTEMPTS} attempts: {e}")
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


# Mirrors the ITMS21 "aktivitaprojekt" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).
AKTIVITAPROJEKT_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("nazov", "nazov", "VARCHAR"),
    ("datumzaciatkuplanovany", "datumZaciatkuPlanovany", "BIGINT"),
    ("datumzaciatkuskutocny", "datumZaciatkuSkutocny", "BIGINT"),
    ("datumkoncaplanovany", "datumKoncaPlanovany", "BIGINT"),
    ("datumkoncaskutocny", "datumKoncaSkutocny", "BIGINT"),
    ("projekt_id", "projekt.id", "BIGINT"),
    ("subjekt_id", "subjekt.id", "BIGINT"),
    ("typakcieprogramu_id", "typAkcieProgramu.id", "BIGINT"),
]


TABLE_COMMENTS: dict[str, str] = {
    TABLE_AKTIVITAPROJEKT: (
        "One row per project activity ('aktivita projektu'), a discrete "
        "activity carried out within a funded project. Purely additive: once "
        "an id is stored it is never re-fetched, updated, or deleted, even "
        "if it disappears from a later API list response."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_AKTIVITAPROJEKT: {
        "id": "ITMS21 numeric id of the project activity.",
        "href": "API URL of this project activity's own detail resource.",
        "kod": "Project activity code.",
        "nazov": "Project activity name.",
        "datumzaciatkuplanovany": "Planned start date of the activity (epoch milliseconds).",
        "datumzaciatkuskutocny": "Actual start date of the activity (epoch milliseconds).",
        "datumkoncaplanovany": "Planned end date of the activity (epoch milliseconds).",
        "datumkoncaskutocny": "Actual end date of the activity (epoch milliseconds).",
        "projekt_id": "Id of the parent project this activity belongs to (itms21_projekt.id).",
        "subjekt_id": "Id of the entity/institution responsible for carrying out this activity.",
        "typakcieprogramu_id": "Id of the programme action type this activity is classified under (itms21_program_typakcieprogramu.id).",
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
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in AKTIVITAPROJEKT_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_AKTIVITAPROJEKT} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)


def store_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Insert one aktivitaprojekt's detail JSON into itms21_aktivitaprojekt.
    Only ever called once per id (gated by get_known_ids in
    sync_aktivitaprojekt), so this is a plain INSERT - never re-fetched, never
    updated."""
    columns_sql = ", ".join(col for col, _, _ in AKTIVITAPROJEKT_COLUMNS)
    placeholders = ", ".join("?" for _ in AKTIVITAPROJEKT_COLUMNS)
    values = [_get(detail, path) for _, path, _ in AKTIVITAPROJEKT_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_AKTIVITAPROJEKT} ({columns_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO NOTHING",
        values,
    )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_AKTIVITAPROJEKT}").fetchall()
    return {row[0] for row in rows}


def sync_aktivitaprojekt() -> tuple[int, int, int]:
    """Fetch the list (ids only, not stored), then fetch full details only for
    ids not already stored.

    Returns (total_in_list, fetched_count, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_table(con)
    apply_comments(con)

    print("Fetching aktivitaprojekt list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} aktivitaprojekt.")

    known_ids = get_known_ids(con)

    to_fetch = [item.get("id") for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_fetch)} new aktivitaprojekt(s) to fetch; "
          f"{len(list_items) - len(to_fetch)} already stored (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, aid): aid for aid in to_fetch}
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

    return len(list_items), fetched_count, failed_count


def main():
    total, fetched, failed = sync_aktivitaprojekt()
    print(f"Done. {total} total in list, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
