"""
Fetches the ITMS21 "zmluvaverejneobstaravanie" (public procurement contract)
list, then fetches full detail ONLY for ids not yet stored in DuckDB. Ids
already stored are never re-fetched and never deleted, even if they
disappear from a later API list response (purely additive incremental sync,
same approach as fetch_zop.py / fetch_zonfp.py / fetch_verejneobstaravanie.py).

Unlike every other list endpoint in this repo, this one has no "all
records" mode: the list endpoint only accepts a single-KOD filter
(verejneobstaravanieKOD), so there is no equivalent of a plain
"?limit=-1" call. Discovery therefore means querying the list once per
known verejneobstaravanie KOD - i.e. once per row already stored in
itms21_verejneobstaravanie_detail (written by fetch_verejneobstaravanie.py,
which MUST run before this script in .github/workflows/monthly.yml). Every
contract belongs to exactly one verejneobstaravanie, so the per-KOD list
responses don't overlap. As with detail fetches, these per-KOD list calls
run through a ThreadPoolExecutor to keep the many small requests fast.

Each detail fetched decomposes into a normalized
itms21_zmluvaverejneobstaravanie table plus
itms21_zmluvaverejneobstaravanie_dalsieurl and
itms21_zmluvaverejneobstaravanie_dodavatelia child tables. Every child table
carries zmluvaverejneobstaravanie_id so it can be joined back to the parent.
Rows are inserted once and never updated/deleted, gated by the same "id not
yet known" check.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/zmluvyverejnehoobstaravania"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/zmluvaverejneobstaravanie/id/{id}"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_ZMLUVA = f"{TABLE_PREFIX}zmluvaverejneobstaravanie"
TABLE_VEREJNEOBSTARAVANIE = f"{TABLE_PREFIX}verejneobstaravanie_detail"  # written by fetch_verejneobstaravanie.py

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

LIST_REQUEST_TIMEOUT = 60  # each call is scoped to a single verejneobstaravanie KOD, so responses are small


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM information_schema.tables WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?)",
        [DB_SCHEMA, name],
    ).fetchone() is not None


def get_verejneobstaravanie_kods(con: duckdb.DuckDBPyConnection) -> list[str]:
    """KODs of already-known verejneobstaravanie procedures, read from
    slovakia.itms21_verejneobstaravanie_detail (populated by
    fetch_verejneobstaravanie.py). Raises if that table doesn't exist yet -
    the list endpoint here has no "all records" mode, so this script depends
    entirely on fetch_verejneobstaravanie.py having run first."""
    if not _table_exists(con, TABLE_VEREJNEOBSTARAVANIE):
        raise RuntimeError(
            f"{DB_SCHEMA}.{TABLE_VEREJNEOBSTARAVANIE} does not exist. "
            "Run scripts/fetch_verejneobstaravanie.py first - it populates this table, "
            "which is the only source of verejneobstaravanieKOD values for "
            "fetch_zmluvyverejnehoobstaravania.py."
        )
    rows = con.execute(f"SELECT DISTINCT kod FROM {TABLE_VEREJNEOBSTARAVANIE} WHERE kod IS NOT NULL").fetchall()
    return [row[0] for row in rows]


def fetch_list_for_kod(kod: str) -> list[dict]:
    """Call the list endpoint scoped to one verejneobstaravanie KOD, with
    retry on failure. Returns [] (rather than raising) on final failure -
    whether from a network error or an unexpected response shape - so one
    bad KOD doesn't abort discovery for every other KOD."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(
                LIST_URL,
                params={"limit": -1, "verejneobstaravanieKOD": kod},
                timeout=LIST_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            payload = resp.json()
            return payload["result"]
        except (requests.RequestException, ValueError, KeyError) as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  List fetch failed for verejneobstaravanieKOD={kod} after {RETRY_ATTEMPTS} attempts: {e}")
                return []
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    return []


def fetch_list(kods: list[str]) -> list[dict]:
    """Query the list endpoint once per known verejneobstaravanie KOD,
    concurrently, and return the combined summary records (used only to
    discover ids - the list itself is not stored)."""
    all_items = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_kod = {executor.submit(fetch_list_for_kod, kod): kod for kod in kods}
        for i, future in enumerate(as_completed(future_to_kod), start=1):
            all_items.extend(future.result())
            if i % 500 == 0 or i == len(kods):
                print(f"  Queried {i}/{len(kods)} verejneobstaravanie KODs, {len(all_items)} contract(s) found so far...")
    return all_items


def fetch_detail(zmluva_id: int) -> dict | None:
    """Call the detail endpoint for one zmluvaverejneobstaravanie, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=zmluva_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={zmluva_id} after {RETRY_ATTEMPTS} attempts: {e}")
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
    """Build the itms21_zmluvaverejneobstaravanie_<bare_name> child table name."""
    return f"{TABLE_PREFIX}zmluvaverejneobstaravanie_{bare_name}"


# --- Full normalized schema -------------------------------------------------
# Mirrors the ITMS21 "zmluvaverejneobstaravanie" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).

ZMLUVA_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("nazov", "nazov", "VARCHAR"),
    ("celkovasumazmluvy", "celkovaSumaZmluvy", "DOUBLE"),
    ("cislozmluvy", "cisloZmluvy", "VARCHAR"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
    ("datumplatnosti", "datumPlatnosti", "VARCHAR"),
    ("datumucinnosti", "datumUcinnosti", "VARCHAR"),
    ("hlavnydodavateldodavatelobstaravatel_id", "hlavnyDodavatelDodavatelObstaravatel.id", "BIGINT"),
    ("hlavnydodavatelsubjekt_id", "hlavnyDodavatelSubjekt.id", "BIGINT"),
    ("predmetzmluvy", "predmetZmluvy", "VARCHAR"),
    ("sumabezdph", "sumaBezDph", "DOUBLE"),
    ("urlodkaznazmluvu", "urlOdkazNaZmluvu", "VARCHAR"),
    ("verejneobstaravanie_id", "verejneObstaravanie.id", "BIGINT"),
]

# bare_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "dalsieurl": ("dalsieUrl", [
        ("nazov", "nazov", "VARCHAR"),
        ("url", "url", "VARCHAR"),
    ]),
    "dodavatelia": ("dodavatelia", [
        ("id", "id", "BIGINT"),
        ("href", "href", "VARCHAR"),
        ("hlavnydodavatel", "hlavnyDodavatel", "BOOLEAN"),
        ("dodavateldodavatelobstaravatel_id", "dodavatelDodavatelObstaravatel.id", "BIGINT"),
        ("dodavatelsubjekt_id", "dodavatelSubjekt.id", "BIGINT"),
    ]),
}


TABLE_COMMENTS: dict[str, str] = {
    TABLE_ZMLUVA: (
        "One row per contract awarded under a public procurement procedure "
        "('zmluva verejneho obstaravania'). Purely additive: once an id is "
        "stored it is never re-fetched, updated, or deleted, even if it "
        "disappears from a later API list response."
    ),
    _t("dalsieurl"): (
        "Additional reference URLs for the contract (e.g. links to related documents). Child "
        "rows carrying zmluvaverejneobstaravanie_id back to itms21_zmluvaverejneobstaravanie; "
        "inserted once when the parent detail is first fetched, never updated/deleted."
    ),
    _t("dodavatelia"): (
        "Suppliers (contractors) party to the contract. Child rows carrying "
        "zmluvaverejneobstaravanie_id back to itms21_zmluvaverejneobstaravanie; inserted once "
        "when the parent detail is first fetched, never updated/deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_ZMLUVA: {
        "id": "ITMS21 numeric id of the contract.",
        "href": "API URL of this contract's own detail resource.",
        "kod": "Contract code.",
        "nazov": "Name of the contract.",
        "celkovasumazmluvy": "Total value of the contract, in euro.",
        "cislozmluvy": "Contract number.",
        "createdat": "Record creation timestamp in the source system.",
        "updatedat": "Record last-updated timestamp in the source system.",
        "datumplatnosti": "Date the contract is valid until.",
        "datumucinnosti": "Date the contract took effect.",
        "hlavnydodavateldodavatelobstaravatel_id": (
            "Id of the main supplier as a contracting-authority-side party (dodavatel/obstaravatel)."
        ),
        "hlavnydodavatelsubjekt_id": "Id of the main supplier entity (subjekt).",
        "predmetzmluvy": "Subject matter of the contract.",
        "sumabezdph": "Value of the contract excluding VAT, in euro.",
        "urlodkaznazmluvu": "URL link to the published contract document.",
        "verejneobstaravanie_id": (
            "Id of the parent procurement procedure this contract belongs to "
            "(itms21_verejneobstaravanie_detail.id)."
        ),
    },
    _t("dalsieurl"): {
        "zmluvaverejneobstaravanie_id": "Id of the parent contract (itms21_zmluvaverejneobstaravanie.id).",
        "nazov": "Name/label of the additional URL.",
        "url": "The additional URL itself.",
    },
    _t("dodavatelia"): {
        "zmluvaverejneobstaravanie_id": "Id of the parent contract (itms21_zmluvaverejneobstaravanie.id).",
        "id": "Id of the supplier-party record.",
        "href": "API URL of this supplier-party record's own detail resource.",
        "hlavnydodavatel": "Whether this supplier is the main/lead contractor on the contract.",
        "dodavateldodavatelobstaravatel_id": (
            "Id of the supplier as a contracting-authority-side party (dodavatel/obstaravatel)."
        ),
        "dodavatelsubjekt_id": "Id of the supplier entity (subjekt).",
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
    """Create itms21_zmluvaverejneobstaravanie and its child tables if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in ZMLUVA_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_ZMLUVA} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                zmluvaverejneobstaravanie_id BIGINT,
                {cols_sql}
            )
        """)


def store_full_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Decompose one zmluvaverejneobstaravanie's full detail JSON into
    itms21_zmluvaverejneobstaravanie + all child tables. Only ever called
    once per id (gated by get_known_ids in sync_zmluvaverejneobstaravania),
    so this is a plain INSERT - never re-fetched, never updated."""
    zmluva_id = detail.get("id")

    columns_sql = ", ".join(col for col, _, _ in ZMLUVA_COLUMNS)
    placeholders = ", ".join("?" for _ in ZMLUVA_COLUMNS)
    values = [_get(detail, path) for _, path, _ in ZMLUVA_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_ZMLUVA} ({columns_sql}) VALUES ({placeholders}) "
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
            [zmluva_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (zmluvaverejneobstaravanie_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_ZMLUVA}").fetchall()
    return {row[0] for row in rows}


def sync_zmluvaverejneobstaravania() -> tuple[int, int, int]:
    """Fetch the list (ids only, not stored) once per known
    verejneobstaravanie KOD, then fetch full details only for ids not
    already stored.

    Returns (total_in_list, fetched_count, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_full_schema(con)
    apply_comments(con)

    kods = get_verejneobstaravanie_kods(con)
    print(f"Found {len(kods)} verejneobstaravanie KOD(s) to query for contracts.")

    print("Fetching zmluvaverejneobstaravanie list...")
    list_items = fetch_list(kods)
    print(f"List returned {len(list_items)} zmluvaverejneobstaravanie (across all KODs).")

    known_ids = get_known_ids(con)

    to_fetch = [item.get("id") for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_fetch)} new zmluvaverejneobstaravanie(s) to fetch; "
          f"{len(list_items) - len(to_fetch)} already stored (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, zid): zid for zid in to_fetch}
            for i, future in enumerate(as_completed(future_to_id), start=1):
                detail = future.result()
                if detail is None:
                    failed_count += 1
                    continue
                store_full_detail(con, detail)
                fetched_count += 1

                if i % 50 == 0 or i == len(to_fetch):
                    con.commit()  # periodic commit so progress survives an interruption
                    print(f"  Progress: {i}/{len(to_fetch)} processed "
                          f"({fetched_count} ok, {failed_count} failed)")

    con.commit()
    con.close()

    return len(list_items), fetched_count, failed_count


def main():
    total, fetched, failed = sync_zmluvaverejneobstaravania()
    print(f"Done. {total} total in list, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
