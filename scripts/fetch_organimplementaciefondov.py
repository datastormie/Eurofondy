"""
Fetches the ITMS21 "organimplementaciefondov" (fund implementation body) list
and syncs it into a single DuckDB table: organimplementaciefondov already
known are always rewritten with the latest API data (full overwrite of that
row), new ones are inserted, and ones that no longer appear in the API
response are left untouched (the table is never truncated, so nothing is
ever deleted).

Like "priorita" and "typakcieprogramu", the list call itself already returns
full records (no separate per-id detail endpoint) - but unlike them, this one
supports limit=-1 directly (confirmed with a bare curl call, which returned
all 48 rows in one response), so there's no need for the "discover size via a
bare call, then request limit={size}" two-step dance.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from pathlib import Path

import duckdb
import requests

API_URL = "https://api.itms21.sk/public/v1/organimplementaciefondov?limit=-1"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate table inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_ORGANIMPLEMENTACIEFONDOV = f"{TABLE_PREFIX}organimplementaciefondov"

RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2
REQUEST_TIMEOUT = 30


def fetch_organimplementaciefondov() -> list[dict]:
    """Fetch every organimplementaciefondov record in one request (the list
    endpoint supports limit=-1 directly), with retry on failure."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(API_URL, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()["results"]
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


# Mirrors the ITMS21 "organimplementaciefondov" list endpoint field-by-field.
# column_name -> (dotted path in the raw JSON, DuckDB type).
ORGANIMPLEMENTACIEFONDOV_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("kod", "kod", "VARCHAR"),
    ("subjekt_id", "subjekt.id", "BIGINT"),
    ("nazov", "nazov", "VARCHAR"),
]


TABLE_COMMENTS: dict[str, str] = {
    TABLE_ORGANIMPLEMENTACIEFONDOV: (
        "One row per fund implementation body ('orgán implementácie fondov'), "
        "an institution responsible for implementing part of a programme's "
        "funds. Existing rows are fully rewritten with the latest API data "
        "on every sync; nothing is ever deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_ORGANIMPLEMENTACIEFONDOV: {
        "id": "ITMS21 numeric id of the fund implementation body.",
        "kod": "Fund implementation body code.",
        "subjekt_id": "Id of the legal entity (itms21_subjekt.id) that acts as this implementation body.",
        "nazov": "Fund implementation body name.",
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
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in ORGANIMPLEMENTACIEFONDOV_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_ORGANIMPLEMENTACIEFONDOV} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)


def upsert_row(con: duckdb.DuckDBPyConnection, item: dict) -> None:
    columns_sql = ", ".join(col for col, _, _ in ORGANIMPLEMENTACIEFONDOV_COLUMNS)
    placeholders = ", ".join("?" for _ in ORGANIMPLEMENTACIEFONDOV_COLUMNS)
    update_sql = ", ".join(
        f"{col} = excluded.{col}" for col, _, _ in ORGANIMPLEMENTACIEFONDOV_COLUMNS if col != "id"
    )
    values = [_get(item, path) for _, path, _ in ORGANIMPLEMENTACIEFONDOV_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_ORGANIMPLEMENTACIEFONDOV} ({columns_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {update_sql}",
        values,
    )


def sync_organimplementaciefondov() -> int:
    """Fetch every organimplementaciefondov and upsert it: existing ids are
    fully rewritten, new ids are inserted, and nothing is ever deleted.

    Returns the number of organimplementaciefondov synced.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_table(con)
    apply_comments(con)

    print("Fetching organimplementaciefondov list...")
    items = fetch_organimplementaciefondov()
    print(f"List returned {len(items)} organimplementaciefondov.")

    for item in items:
        upsert_row(con, item)

    con.commit()
    con.close()

    return len(items)


def main():
    synced = sync_organimplementaciefondov()
    print(f"Done. {synced} organimplementaciefondov synced.")


if __name__ == "__main__":
    main()
