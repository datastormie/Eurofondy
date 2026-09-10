"""
Fetches the ITMS21 "strategiaius" (integrated territorial strategy) list.
Sourced entirely from the list endpoint (?limit=-1): the list response
already embeds each strategy's full detail inline, including its
one-to-many specifickyCielProgramu and projektovyZamerIUS references, so
there is no separate per-id detail call here. (The dedicated /id/{id}
detail endpoint for this entity is currently blocked by the ITMS21 API's
WAF for this client anyway - the list endpoint is unaffected and already
carries everything the detail endpoint would.)

Only records whose id isn't already in itms21_strategiaius are stored.
Purely additive: once an id is stored it is never re-fetched, updated, or
deleted, even if it disappears from a later API list response (same overall
approach as fetch_aktivitaprojekt.py / fetch_zop.py, minus the detail call).

Decomposes into itms21_strategiaius (id, kod, nazov, stav) and two child
tables, one per one-to-many nested list field:
itms21_strategiaius_specifickycielprogramu and
itms21_strategiaius_projektovyzamerius. Each child table stores only the
bare id reference - the referenced entity's own detail lives in its own
fetch script's table (fetch_specifickycielprogramu.py /
fetch_projektovyzamerius.py respectively).

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/strategiaius?limit=-1"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_STRATEGIAIUS = f"{TABLE_PREFIX}strategiaius"

RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2
LIST_REQUEST_TIMEOUT = 180  # the list endpoint returns every strategiaius in one response (limit=-1)


def fetch_list() -> list[dict]:
    """Call the list endpoint and return every strategiaius record, full
    detail already embedded, with retry on failure."""
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


def _get(d: dict | None, path: str):
    """Walk a dotted path through nested dicts; None if any segment is missing/None."""
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _t(bare_name: str) -> str:
    """Build the itms21_strategiaius_<bare_name> child table name."""
    return f"{TABLE_PREFIX}strategiaius_{bare_name}"


# Mirrors the ITMS21 "strategiaius" list record's scalar fields.
# column_name -> (dotted path in the raw record, DuckDB type).
STRATEGIAIUS_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("kod", "kod", "VARCHAR"),
    ("nazov", "nazov", "VARCHAR"),
    ("stav", "stav", "VARCHAR"),
]

# bare_name -> (source list field in the raw record, [(column, path-within-item, type), ...])
# Both of these fields are a one-to-many list of references in the raw JSON
# (not a single object), so they're normalized into child tables instead of
# flat FK columns.
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "specifickycielprogramu": ("specifickyCielProgramu", [("id", "id", "BIGINT")]),
    "projektovyzamerius": ("projektovyZamerIUS", [("id", "id", "BIGINT")]),
}


TABLE_COMMENTS: dict[str, str] = {
    TABLE_STRATEGIAIUS: (
        "One row per integrated territorial strategy ('strategia integrovaneho "
        "uzemneho investovania', IUS) record. Sourced entirely from the "
        "strategiaius list endpoint (?limit=-1), which already embeds full "
        "detail per record. Purely additive: once an id is stored it is "
        "never re-fetched, updated, or deleted."
    ),
    _t("specifickycielprogramu"): (
        "Programme specific objectives ('specificky ciel programu') an integrated territorial "
        "strategy is classified under. Child rows carrying strategiaius_id back to "
        "itms21_strategiaius; inserted once when the parent record is first stored, never "
        "updated/deleted."
    ),
    _t("projektovyzamerius"): (
        "Project intents ('projektovy zamer') declared under an integrated territorial "
        "strategy. Child rows carrying strategiaius_id back to itms21_strategiaius; inserted "
        "once when the parent record is first stored, never updated/deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_STRATEGIAIUS: {
        "id": "ITMS21 numeric id of the integrated territorial strategy record.",
        "kod": "Integrated territorial strategy code.",
        "nazov": "Integrated territorial strategy name.",
        "stav": "Status of the integrated territorial strategy record.",
    },
    _t("specifickycielprogramu"): {
        "strategiaius_id": "Id of the parent integrated territorial strategy (itms21_strategiaius.id).",
        "id": "Id of the programme specific objective (itms21_specifickycielprogramu.id).",
    },
    _t("projektovyzamerius"): {
        "strategiaius_id": "Id of the parent integrated territorial strategy (itms21_strategiaius.id).",
        "id": "Id of the project intent (itms21_projektovyzamerius.id).",
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
    """Create itms21_strategiaius and its child tables if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in STRATEGIAIUS_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_STRATEGIAIUS} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                strategiaius_id BIGINT,
                {cols_sql}
            )
        """)


def store_record(con: duckdb.DuckDBPyConnection, record: dict) -> None:
    """Decompose one strategiaius list record into itms21_strategiaius + its
    child tables. Only ever called once per id (gated by get_known_ids in
    sync_strategiaius), so this is a plain INSERT - never re-fetched, never
    updated."""
    strategiaius_id = record.get("id")

    columns_sql = ", ".join(col for col, _, _ in STRATEGIAIUS_COLUMNS)
    placeholders = ", ".join("?" for _ in STRATEGIAIUS_COLUMNS)
    values = [_get(record, path) for _, path, _ in STRATEGIAIUS_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_STRATEGIAIUS} ({columns_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO NOTHING",
        values,
    )

    for table, (source_field, columns) in CHILD_TABLES.items():
        items = record.get(source_field) or []
        if not items:
            continue
        columns_sql = ", ".join(col for col, _, _ in columns)
        placeholders = ", ".join("?" for _ in columns)
        rows = [
            [strategiaius_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (strategiaius_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already stored, so we never re-store them."""
    rows = con.execute(f"SELECT id FROM {TABLE_STRATEGIAIUS}").fetchall()
    return {row[0] for row in rows}


def sync_strategiaius() -> tuple[int, int]:
    """Fetch the full list (already containing full detail per record), then
    store only records whose id isn't already in itms21_strategiaius.

    Returns (total_in_list, newly_stored_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_full_schema(con)
    apply_comments(con)

    print("Fetching strategiaius list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} strategiaius.")

    known_ids = get_known_ids(con)
    to_store = [item for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_store)} new strategiaius(es) to store; "
          f"{len(list_items) - len(to_store)} already stored (skipped).")

    for i, record in enumerate(to_store, start=1):
        store_record(con, record)
        if i % 50 == 0 or i == len(to_store):
            con.commit()  # periodic commit so progress survives an interruption
            print(f"  Progress: {i}/{len(to_store)} stored")

    con.commit()
    con.close()

    return len(list_items), len(to_store)


def main():
    total, stored = sync_strategiaius()
    print(f"Done. {total} total in list, {stored} newly stored.")


if __name__ == "__main__":
    main()
