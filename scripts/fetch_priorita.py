"""
Fetches the ITMS21 "priorita" (programme priority) list and syncs it into a
single DuckDB table: priorita already known are always rewritten with the
latest API data (full overwrite of that row), new priorita are inserted, and
priorita that no longer appear in the API response are left untouched (the
table is never truncated, so nothing is ever deleted).

Unlike the other endpoints, the list call itself already returns full records
(no separate per-id detail endpoint), and it doesn't support limit=-1 - the
total count has to be discovered first (a bare call returns size with an
empty results list), then requested explicitly via limit={size}.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

from pathlib import Path

import duckdb
import requests

API_URL = "https://api.itms21.sk/public/v1/priorita"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate table inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_PRIORITA = f"{TABLE_PREFIX}program_priorita"

REQUEST_TIMEOUT = 30


def fetch_priorita() -> list[dict]:
    """Discover the total count via a bare call, then fetch every priorita
    record in one request using that count as the limit."""
    resp = requests.get(API_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    size = resp.json()["size"]

    resp = requests.get(API_URL, params={"limit": size}, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()["results"]


def _get(d: dict | None, path: str):
    """Walk a dotted path through nested dicts; None if any segment is missing/None."""
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


# Mirrors the ITMS21 "priorita" list endpoint field-by-field.
# column_name -> (dotted path in the raw JSON, DuckDB type).
PRIORITA_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("nazovsk", "nazovSk", "VARCHAR"),
    ("nazoven", "nazovEn", "VARCHAR"),
    ("nazovde", "nazovDe", "VARCHAR"),
    ("program_id", "program.id", "BIGINT"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
]


TABLE_COMMENTS: dict[str, str] = {
    TABLE_PRIORITA: (
        "One row per programme priority axis ('priorita'), a top-level "
        "structural division of an Operational Programme under which "
        "specific objectives and measures are grouped. Existing rows are "
        "fully rewritten with the latest API data on every sync; nothing is "
        "ever deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_PRIORITA: {
        "id": "ITMS21 numeric id of the priority axis.",
        "href": "API URL of this priority axis's own detail resource.",
        "kod": "Priority axis code.",
        "nazovsk": "Priority axis name in Slovak.",
        "nazoven": "Priority axis name in English.",
        "nazovde": "Priority axis name in German.",
        "program_id": "Id of the parent programme this priority axis belongs to (itms21_programs_current.program_id).",
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


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in PRIORITA_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_PRIORITA} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)


def upsert_row(con: duckdb.DuckDBPyConnection, item: dict) -> None:
    columns_sql = ", ".join(col for col, _, _ in PRIORITA_COLUMNS)
    placeholders = ", ".join("?" for _ in PRIORITA_COLUMNS)
    update_sql = ", ".join(f"{col} = excluded.{col}" for col, _, _ in PRIORITA_COLUMNS if col != "id")
    values = [_get(item, path) for _, path, _ in PRIORITA_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_PRIORITA} ({columns_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {update_sql}",
        values,
    )


def sync_priorita() -> int:
    """Fetch every priorita and upsert it: existing ids are fully rewritten,
    new ids are inserted, and nothing is ever deleted.

    Returns the number of priorita synced.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_table(con)
    apply_comments(con)

    print("Fetching priorita list...")
    items = fetch_priorita()
    print(f"List returned {len(items)} priorita.")

    for item in items:
        upsert_row(con, item)

    con.commit()
    con.close()

    return len(items)


def main():
    synced = sync_priorita()
    print(f"Done. {synced} priorita synced.")


if __name__ == "__main__":
    main()
