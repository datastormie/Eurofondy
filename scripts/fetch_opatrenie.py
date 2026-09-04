"""
Fetches the ITMS21 "opatrenie" (programme measure) list and syncs it into a
single DuckDB table: opatrenie already known are always rewritten with the
latest API data (full overwrite of that row), new ones are inserted, and
ones that no longer appear in the API response are left untouched (the
table is never truncated, so nothing is ever deleted).

Like "priorita", the list call itself already returns full records (no
separate per-id detail endpoint), and it doesn't support limit=-1 - the
total count has to be discovered first (a bare call returns size with an
empty results list), then requested explicitly via limit={size}.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

from pathlib import Path

import duckdb
import requests

API_URL = "https://api.itms21.sk/public/v1/opatrenie"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate table inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_OPATRENIE = f"{TABLE_PREFIX}program_opatrenie"

REQUEST_TIMEOUT = 30


def fetch_opatrenie() -> list[dict]:
    """Discover the total count via a bare call, then fetch every opatrenie
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


# Mirrors the ITMS21 "opatrenie" list endpoint field-by-field.
# column_name -> (dotted path in the raw JSON, DuckDB type).
OPATRENIE_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("nazovsk", "nazovSk", "VARCHAR"),
    ("nazoven", "nazovEn", "VARCHAR"),
    ("nazovde", "nazovDe", "VARCHAR"),
    ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
]


TABLE_COMMENTS: dict[str, str] = {
    TABLE_OPATRENIE: (
        "One row per programme measure ('opatrenie'), a funding sub-category "
        "nested under a programme's specific objective, under which calls "
        "for proposals ('vyzva') are announced. Existing rows are fully "
        "rewritten with the latest API data on every sync; nothing is ever "
        "deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_OPATRENIE: {
        "id": "ITMS21 numeric id of the measure.",
        "href": "API URL of this measure's own detail resource.",
        "kod": "Measure code.",
        "nazovsk": "Measure name in Slovak.",
        "nazoven": "Measure name in English.",
        "nazovde": "Measure name in German.",
        "specifickycielprogramu_id": "Id of the parent specific objective this measure belongs to (itms21_program_specifickycielprogramu.id).",
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
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in OPATRENIE_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_OPATRENIE} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)


def upsert_row(con: duckdb.DuckDBPyConnection, item: dict) -> None:
    columns_sql = ", ".join(col for col, _, _ in OPATRENIE_COLUMNS)
    placeholders = ", ".join("?" for _ in OPATRENIE_COLUMNS)
    update_sql = ", ".join(f"{col} = excluded.{col}" for col, _, _ in OPATRENIE_COLUMNS if col != "id")
    values = [_get(item, path) for _, path, _ in OPATRENIE_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_OPATRENIE} ({columns_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {update_sql}",
        values,
    )


def sync_opatrenie() -> int:
    """Fetch every opatrenie and upsert it: existing ids are fully rewritten,
    new ids are inserted, and nothing is ever deleted.

    Returns the number of opatrenie synced.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_table(con)
    apply_comments(con)

    print("Fetching opatrenie list...")
    items = fetch_opatrenie()
    print(f"List returned {len(items)} opatrenie.")

    for item in items:
        upsert_row(con, item)

    con.commit()
    con.close()

    return len(items)


def main():
    synced = sync_opatrenie()
    print(f"Done. {synced} opatrenie synced.")


if __name__ == "__main__":
    main()
