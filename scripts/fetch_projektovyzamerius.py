"""
Fetches the ITMS21 "projektovyzamerius" (project intent - a preliminary
declaration of intent to submit a project, ahead of the actual zonfp
application) list. Sourced entirely from the list endpoint
(?limit=-1): unlike most other entities in this repo, projektovyzamerius's
list response already embeds each record's full detail (nested strategiaIUS,
ziadatel, and the one-to-many planovanaVyzva/vyzva/projekt/zonfp
references) inline, so there is no separate per-id detail call here. (The
dedicated /id/{id} detail endpoint for this entity is currently blocked by
the ITMS21 API's WAF for this client anyway - the list endpoint is
unaffected and already carries everything the detail endpoint would.)

Only records whose id isn't already in itms21_projektovyzamerius are stored.
Purely additive: once an id is stored it is never re-fetched, updated, or
deleted, even if it disappears from a later API list response (same overall
approach as fetch_aktivitaprojekt.py / fetch_zop.py, minus the detail call).

Decomposes into itms21_projektovyzamerius (id, kod, nazov, stav, plus the
single-object strategiaius_id/ziadatel_id FKs) and four child tables, one
per one-to-many nested list field: itms21_projektovyzamerius_planovanavyzva,
_vyzva, _projekt, _zonfp. Each child table stores only the bare id
reference - the referenced entity's own detail lives in its own fetch
script's table (fetch_planovanavyzvy.py / fetch_vyzvy.py / fetch_projects.py
/ fetch_zonfp.py respectively).

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/projektovyzamerius?limit=-1"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_PROJEKTOVYZAMERIUS = f"{TABLE_PREFIX}projektovyzamerius"

RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2
LIST_REQUEST_TIMEOUT = 180  # the list endpoint returns every projektovyzamerius in one response (limit=-1)


def fetch_list() -> list[dict]:
    """Call the list endpoint and return every projektovyzamerius record,
    full detail already embedded, with retry on failure."""
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
    """Build the itms21_projektovyzamerius_<bare_name> child table name."""
    return f"{TABLE_PREFIX}projektovyzamerius_{bare_name}"


# Mirrors the ITMS21 "projektovyzamerius" list record's scalar/single-object fields.
# column_name -> (dotted path in the raw record, DuckDB type).
PROJEKTOVYZAMERIUS_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("kod", "kod", "VARCHAR"),
    ("nazov", "nazov", "VARCHAR"),
    ("stav", "stav", "VARCHAR"),
    ("strategiaius_id", "strategiaIUS.id", "BIGINT"),
    ("ziadatel_id", "ziadatel.id", "BIGINT"),
]

# bare_name -> (source list field in the raw record, [(column, path-within-item, type), ...])
# Each of these fields is a one-to-many list of references in the raw JSON
# (not a single object), so they're normalized into child tables instead of
# flat FK columns.
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "planovanavyzva": ("planovanaVyzva", [("id", "id", "BIGINT")]),
    "vyzva": ("vyzva", [("id", "id", "BIGINT")]),
    "projekt": ("projekt", [("id", "id", "BIGINT")]),
    "zonfp": ("zonfp", [("id", "id", "BIGINT")]),
}


TABLE_COMMENTS: dict[str, str] = {
    TABLE_PROJEKTOVYZAMERIUS: (
        "One row per project intent ('projektový zámer'), a preliminary "
        "declaration of intent to submit a project, made ahead of (and "
        "optionally linked to) an actual call/application. Sourced entirely "
        "from the projektovyzamerius list endpoint (?limit=-1), which "
        "already embeds full detail per record. Purely additive: once an "
        "id is stored it is never re-fetched, updated, or deleted."
    ),
    _t("planovanavyzva"): (
        "Planned calls ('planovana vyzva') a project intent targets. Child rows carrying "
        "projektovyzamerius_id back to itms21_projektovyzamerius; inserted once when the "
        "parent record is first stored, never updated/deleted."
    ),
    _t("vyzva"): (
        "Calls ('vyzva') a project intent targets. Child rows carrying projektovyzamerius_id "
        "back to itms21_projektovyzamerius; inserted once when the parent record is first "
        "stored, never updated/deleted."
    ),
    _t("projekt"): (
        "Projects a project intent turned into, if any. Child rows carrying "
        "projektovyzamerius_id back to itms21_projektovyzamerius; inserted once when the "
        "parent record is first stored, never updated/deleted."
    ),
    _t("zonfp"): (
        "Funding applications ('zonfp') a project intent turned into, if any. Child rows "
        "carrying projektovyzamerius_id back to itms21_projektovyzamerius; inserted once "
        "when the parent record is first stored, never updated/deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_PROJEKTOVYZAMERIUS: {
        "id": "ITMS21 numeric id of the project intent.",
        "kod": "Project intent code.",
        "nazov": "Project intent name.",
        "stav": "Current status of the project intent (e.g. approved/rejected).",
        "strategiaius_id": "Id of the integrated territorial strategy ('strategia IUS') this intent falls under (itms21_strategiaius.id).",
        "ziadatel_id": "Id of the applicant entity declaring the intent.",
    },
    _t("planovanavyzva"): {
        "projektovyzamerius_id": "Id of the parent project intent (itms21_projektovyzamerius.id).",
        "id": "Id of the planned call (itms21_planovanavyzva.id).",
    },
    _t("vyzva"): {
        "projektovyzamerius_id": "Id of the parent project intent (itms21_projektovyzamerius.id).",
        "id": "Id of the call (itms21_vyzva.id).",
    },
    _t("projekt"): {
        "projektovyzamerius_id": "Id of the parent project intent (itms21_projektovyzamerius.id).",
        "id": "Id of the project (itms21_projekt.id).",
    },
    _t("zonfp"): {
        "projektovyzamerius_id": "Id of the parent project intent (itms21_projektovyzamerius.id).",
        "id": "Id of the funding application (itms21_zonfp.id).",
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
    """Create itms21_projektovyzamerius and its child tables if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in PROJEKTOVYZAMERIUS_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_PROJEKTOVYZAMERIUS} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                projektovyzamerius_id BIGINT,
                {cols_sql}
            )
        """)


def store_record(con: duckdb.DuckDBPyConnection, record: dict) -> None:
    """Decompose one projektovyzamerius list record into
    itms21_projektovyzamerius + its child tables. Only ever called once per
    id (gated by get_known_ids in sync_projektovyzamerius), so this is a
    plain INSERT - never re-fetched, never updated."""
    pvz_id = record.get("id")

    columns_sql = ", ".join(col for col, _, _ in PROJEKTOVYZAMERIUS_COLUMNS)
    placeholders = ", ".join("?" for _ in PROJEKTOVYZAMERIUS_COLUMNS)
    values = [_get(record, path) for _, path, _ in PROJEKTOVYZAMERIUS_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_PROJEKTOVYZAMERIUS} ({columns_sql}) VALUES ({placeholders}) "
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
            [pvz_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (projektovyzamerius_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already stored, so we never re-store them."""
    rows = con.execute(f"SELECT id FROM {TABLE_PROJEKTOVYZAMERIUS}").fetchall()
    return {row[0] for row in rows}


def sync_projektovyzamerius() -> tuple[int, int]:
    """Fetch the full list (already containing full detail per record), then
    store only records whose id isn't already in itms21_projektovyzamerius.

    Returns (total_in_list, newly_stored_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_full_schema(con)
    apply_comments(con)

    print("Fetching projektovyzamerius list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} projektovyzamerius.")

    known_ids = get_known_ids(con)
    to_store = [item for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_store)} new projektovyzamerius to store; "
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
    total, stored = sync_projektovyzamerius()
    print(f"Done. {total} total in list, {stored} newly stored.")


if __name__ == "__main__":
    main()
