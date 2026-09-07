"""
Fetches the ITMS21 "uctovnydoklad" (accounting document) list, then fetches
full detail ONLY for ids not yet stored in DuckDB. The list itself is not
persisted - it exists purely to discover ids. Ids already stored are never
re-fetched and never deleted, even if they disappear from the API list
(purely additive incremental sync, same approach as fetch_zop.py /
fetch_zonfp.py / fetch_vyzvy.py).

Note the table name mismatch with the API/URL: the DWH spec for this
endpoint names the parent table itms21_doklaductovny (not
itms21_uctovnydoklad), so that's what's used here, while the module/API URL
still say "uctovnydoklad".

Each detail fetched decomposes into a normalized itms21_doklaductovny table
plus itms21_doklaductovny_polozkydokladu, itms21_doklaductovny_projekty, and
itms21_doklaductovny_verejneobstaravania child tables. Every child table
carries doklaductovny_id so it can be joined back to the parent. Rows are
inserted once and never updated/deleted, gated by the same "id not yet known"
check.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/uctovnydoklad?limit=-1"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/uctovnydoklad/id/{id}"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_DOKLADUCTOVNY = f"{TABLE_PREFIX}doklaductovny"

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

LIST_REQUEST_TIMEOUT = 180  # the list endpoint returns every uctovnydoklad in one response (limit=-1)


def fetch_list() -> list[dict]:
    """Call the list endpoint and return all uctovnydoklad summary records
    (used only to discover ids - the list itself is not stored), with retry
    on failure."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(LIST_URL, timeout=LIST_REQUEST_TIMEOUT)
            resp.raise_for_status()
            payload = resp.json()
            return payload["results"]
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                raise
            print(f"  List fetch failed (attempt {attempt}/{RETRY_ATTEMPTS}): {e}")
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)


def fetch_detail(doklad_id: int) -> dict | None:
    """Call the detail endpoint for one uctovnydoklad, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=doklad_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={doklad_id} after {RETRY_ATTEMPTS} attempts: {e}")
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
    """Build the itms21_doklaductovny_<bare_name> child table name."""
    return f"{TABLE_PREFIX}doklaductovny_{bare_name}"


# --- Full normalized schema -------------------------------------------------
# Mirrors the ITMS21 "uctovnydoklad" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).

DOKLADUCTOVNY_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("typ", "typ", "VARCHAR"),
    ("nazov", "nazov", "VARCHAR"),
    ("cislodokladu", "cisloDokladu", "VARCHAR"),
    ("celkovavyskadokladu", "celkovaVyskaDokladu", "DOUBLE"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
    ("datumuhrady", "datumUhrady", "VARCHAR"),
    ("datumvyhotovenia", "datumVyhotovenia", "VARCHAR"),
    ("dodavateldodavatelobstaravatel_id", "dodavatelDodavatelObstaravatel.id", "BIGINT"),
    ("dodavatelsubjekt_id", "dodavatelSubjekt.id", "BIGINT"),
    ("vlastnikdokladu_id", "vlastnikDokladu.id", "BIGINT"),
]

# bare_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "polozkydokladu": ("polozkyDokladu", [
        ("id", "id", "BIGINT"),
        ("href", "href", "VARCHAR"),
        ("nazov", "nazov", "VARCHAR"),
        ("poradovecislo", "poradoveCislo", "INTEGER"),
        ("mnozstvo", "mnozstvo", "DOUBLE"),
        ("jednotkovacena", "jednotkovaCena", "DOUBLE"),
        ("sadzbadph", "sadzbaDph", "DOUBLE"),
        ("dph", "dph", "DOUBLE"),
        ("sumabezdph", "sumaBezDph", "DOUBLE"),
        ("sumaspolu", "sumaSpolu", "DOUBLE"),
        ("sumaopravnena", "sumaOpravnena", "DOUBLE"),
        ("sumaziadana", "sumaZiadana", "DOUBLE"),
    ]),
    "projekty": ("projekty", [("id", "id", "BIGINT")]),
    "verejneobstaravania": ("verejneObstaravania", [("id", "id", "BIGINT")]),
}


TABLE_COMMENTS: dict[str, str] = {
    TABLE_DOKLADUCTOVNY: (
        "One row per accounting document ('účtovný doklad'), e.g. an invoice "
        "backing project expenditure. Purely additive: once an id is stored "
        "it is never re-fetched, updated, or deleted, even if it disappears "
        "from a later API list response."
    ),
    _t("polozkydokladu"): (
        "Line items of an accounting document. Child rows carrying doklaductovny_id back to "
        "itms21_doklaductovny; inserted once when the parent detail is first fetched, never "
        "updated/deleted."
    ),
    _t("projekty"): (
        "Projects this accounting document is linked to. Child rows carrying doklaductovny_id "
        "back to itms21_doklaductovny; inserted once when the parent detail is first fetched, "
        "never updated/deleted."
    ),
    _t("verejneobstaravania"): (
        "Public procurement procedures this accounting document is linked to. Child rows "
        "carrying doklaductovny_id back to itms21_doklaductovny; inserted once when the parent "
        "detail is first fetched, never updated/deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_DOKLADUCTOVNY: {
        "id": "ITMS21 numeric id of the accounting document.",
        "href": "API URL of this accounting document's own detail resource.",
        "typ": "Type of the accounting document.",
        "nazov": "Name/description of the accounting document.",
        "cislodokladu": "Document number.",
        "celkovavyskadokladu": "Total amount of the document, in euro.",
        "createdat": "Record creation timestamp in the source system.",
        "updatedat": "Record last-updated timestamp in the source system.",
        "datumuhrady": "Date the document was settled/paid.",
        "datumvyhotovenia": "Date the document was issued.",
        "dodavateldodavatelobstaravatel_id": "Id of the supplier as a contracting-authority-side party (dodávateľ/obstarávateľ).",
        "dodavatelsubjekt_id": "Id of the supplier entity (subjekt) that issued the document.",
        "vlastnikdokladu_id": "Id of the entity that owns/holds this accounting document.",
    },
    _t("polozkydokladu"): {
        "doklaductovny_id": "Id of the parent accounting document (itms21_doklaductovny.id).",
        "id": "Id of the line item.",
        "href": "API URL of this line item's own detail resource.",
        "nazov": "Name/description of the line item.",
        "poradovecislo": "Ordering position of the line item within the document.",
        "mnozstvo": "Quantity of the line item.",
        "jednotkovacena": "Unit price of the line item, in euro.",
        "sadzbadph": "VAT rate applied to the line item, as a percentage.",
        "dph": "VAT amount of the line item, in euro.",
        "sumabezdph": "Amount of the line item excluding VAT, in euro.",
        "sumaspolu": "Total amount of the line item including VAT, in euro.",
        "sumaopravnena": "Eligible amount of the line item, in euro.",
        "sumaziadana": "Amount of the line item claimed for reimbursement, in euro.",
    },
    _t("projekty"): {
        "doklaductovny_id": "Id of the parent accounting document (itms21_doklaductovny.id).",
        "id": "Id of the linked project (itms21_projekt.id).",
    },
    _t("verejneobstaravania"): {
        "doklaductovny_id": "Id of the parent accounting document (itms21_doklaductovny.id).",
        "id": "Id of the linked public procurement procedure.",
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
    """Create itms21_doklaductovny and its child tables if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in DOKLADUCTOVNY_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_DOKLADUCTOVNY} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                doklaductovny_id BIGINT,
                {cols_sql}
            )
        """)


def store_full_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Decompose one uctovnydoklad's full detail JSON into
    itms21_doklaductovny + all child tables. Only ever called once per id
    (gated by get_known_ids in sync_uctovnydoklad), so this is a plain
    INSERT - never re-fetched, never updated."""
    doklad_id = detail.get("id")

    columns_sql = ", ".join(col for col, _, _ in DOKLADUCTOVNY_COLUMNS)
    placeholders = ", ".join("?" for _ in DOKLADUCTOVNY_COLUMNS)
    values = [_get(detail, path) for _, path, _ in DOKLADUCTOVNY_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_DOKLADUCTOVNY} ({columns_sql}) VALUES ({placeholders}) "
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
            [doklad_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (doklaductovny_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_DOKLADUCTOVNY}").fetchall()
    return {row[0] for row in rows}


def sync_uctovnydoklad() -> tuple[int, int, int]:
    """Fetch the list (ids only, not stored), then fetch full details only for
    ids not already stored.

    Returns (total_in_list, fetched_count, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_full_schema(con)
    apply_comments(con)

    print("Fetching uctovnydoklad list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} uctovnydoklad.")

    known_ids = get_known_ids(con)

    to_fetch = [item.get("id") for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_fetch)} new uctovnydoklad(s) to fetch; "
          f"{len(list_items) - len(to_fetch)} already stored (skipped).")

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
    total, fetched, failed = sync_uctovnydoklad()
    print(f"Done. {total} total in list, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
