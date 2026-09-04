"""
Fetches the ITMS21 code-list (ciselniky) reference data and syncs it into two
DuckDB tables, mirroring the source API/table names:

  - CISELNIK: the list of code-list types (one row per `kod`). Existing kods
    are always rewritten with the latest API data (full overwrite of that
    row), new kods are inserted, nothing is ever deleted.
  - CISELNIKY_DETAIL: the items inside each code list. Purely additive
    incremental sync, keyed on (CISELNIK_KOD, ID) - rows already stored are
    never re-fetched, never updated, never deleted, even if they disappear
    from a later API response.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/ciselniky"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/ciselniky/{kod}"

DB_PATH = Path("data/eurofondy.duckdb")  # shared DuckDB file, separate tables inside

TABLE_PREFIX = "itms21_"
TABLE_LIST = f"{TABLE_PREFIX}CISELNIK".lower()
TABLE_DETAIL = f"{TABLE_PREFIX}CISELNIKY_DETAIL".lower()

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2


def fetch_list() -> list[dict]:
    """Call the ciselniky list endpoint and return every code-list type."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(LIST_URL, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                raise
            print(f"  List fetch failed (attempt {attempt}/{RETRY_ATTEMPTS}): {e}")
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)


def fetch_detail(kod: str) -> list[dict] | None:
    """Call the detail endpoint for one code-list kod, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(kod=kod)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED kod={kod} after {RETRY_ATTEMPTS} attempts: {e}")
                return None
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    return None


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM information_schema.tables WHERE lower(table_name) = lower(?)", [name]
    ).fetchone() is not None


def _rename_columns(con: duckdb.DuckDBPyConnection, table: str, columns: list[str]) -> None:
    """Rename every column in `columns` from its old ALL-CAPS form to its
    current lowercase form; matching is case-insensitive, so this is a no-op
    once a column is already lowercase (safe to run on every invocation)."""
    for col in columns:
        con.execute(f"ALTER TABLE {table} RENAME COLUMN {col.upper()} TO {col}")


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    # One-time rename from the pre-"itms21_"-prefix / ALL-CAPS-column tables
    # from earlier runs; every step is idempotent/case-insensitive, so this
    # is safe to run on every invocation.
    con.execute(f"ALTER TABLE IF EXISTS CISELNIK RENAME TO {TABLE_LIST}")
    con.execute(f"ALTER TABLE IF EXISTS CISELNIKY_DETAIL RENAME TO {TABLE_DETAIL}")
    if _table_exists(con, TABLE_LIST):
        _rename_columns(con, TABLE_LIST, ["kod", "nazov", "popis"])
    if _table_exists(con, TABLE_DETAIL):
        _rename_columns(con, TABLE_DETAIL, [
            "ciselnik_kod", "id", "kod", "kodzdroj", "nazovsk", "nazoven", "nazovde",
            "popissk", "popisen", "popisde", "platnostod", "platnostdo",
        ])

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_LIST} (
            kod    VARCHAR PRIMARY KEY,
            nazov  VARCHAR,
            popis  VARCHAR
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_DETAIL} (
            ciselnik_kod  VARCHAR,
            id            BIGINT,
            kod           VARCHAR,
            kodzdroj      VARCHAR,
            nazovsk       VARCHAR,
            nazoven       VARCHAR,
            nazovde       VARCHAR,
            popissk       VARCHAR,
            popisen       VARCHAR,
            popisde       VARCHAR,
            platnostod    BIGINT,
            platnostdo    BIGINT,
            PRIMARY KEY (ciselnik_kod, id)
        )
    """)


def upsert_ciselnik(con: duckdb.DuckDBPyConnection, item: dict) -> None:
    con.execute(f"""
        INSERT INTO {TABLE_LIST} (kod, nazov, popis) VALUES (?, ?, ?)
        ON CONFLICT (kod) DO UPDATE SET
            nazov = excluded.nazov,
            popis = excluded.popis
    """, [item.get("kod"), item.get("nazov"), item.get("popis")])


def get_known_pairs(con: duckdb.DuckDBPyConnection) -> set[tuple[str, int]]:
    """(ciselnik_kod, id) pairs already stored, so detail rows are never re-inserted."""
    rows = con.execute(f"SELECT ciselnik_kod, id FROM {TABLE_DETAIL}").fetchall()
    return {(row[0], row[1]) for row in rows}


def insert_detail_rows(con: duckdb.DuckDBPyConnection, ciselnik_kod: str, items: list[dict],
                        known_pairs: set[tuple[str, int]]) -> int:
    rows = [
        [
            ciselnik_kod, item.get("id"), item.get("kod"), item.get("kodZdroj"),
            item.get("nazovSk"), item.get("nazovEn"), item.get("nazovDe"),
            item.get("popisSk"), item.get("popisEn"), item.get("popisDe"),
            item.get("platnostOd"), item.get("platnostDo"),
        ]
        for item in items
        if (ciselnik_kod, item.get("id")) not in known_pairs
    ]
    if rows:
        con.executemany(f"""
            INSERT INTO {TABLE_DETAIL} (
                ciselnik_kod, id, kod, kodzdroj, nazovsk, nazoven, nazovde,
                popissk, popisen, popisde, platnostod, platnostdo
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
    return len(rows)


def sync_ciselniky() -> tuple[int, int, int]:
    """Fetch the list of code-list types, upsert CISELNIK, then fetch each
    code-list's items and insert only the (kod, id) combinations not yet
    stored in CISELNIKY_DETAIL.

    Returns (ciselnik_count, new_detail_rows, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    ensure_tables(con)

    print("Fetching ciselniky list...")
    ciselniky = fetch_list()
    print(f"List returned {len(ciselniky)} code lists.")

    for item in ciselniky:
        upsert_ciselnik(con, item)
    con.commit()

    known_pairs = get_known_pairs(con)

    new_rows = 0
    failed_count = 0
    kods = [item.get("kod") for item in ciselniky]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_kod = {executor.submit(fetch_detail, kod): kod for kod in kods}
        for i, future in enumerate(as_completed(future_to_kod), start=1):
            kod = future_to_kod[future]
            items = future.result()
            if items is None:
                failed_count += 1
                continue
            new_rows += insert_detail_rows(con, kod, items, known_pairs)

            if i % 20 == 0 or i == len(kods):
                con.commit()  # periodic commit so progress survives an interruption
                print(f"  Progress: {i}/{len(kods)} code lists processed "
                      f"({new_rows} new detail rows so far, {failed_count} failed)")

    con.commit()
    con.close()

    return len(ciselniky), new_rows, failed_count


def main():
    ciselnik_count, new_rows, failed = sync_ciselniky()
    print(f"Done. {ciselnik_count} code lists, {new_rows} new detail rows, {failed} failed.")


if __name__ == "__main__":
    main()
