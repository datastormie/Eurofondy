"""
Fetches the ITMS21 "typakcieprogramu" (programme action type) list and syncs
it into a single DuckDB table: typakcieprogramu already known are always
rewritten with the latest API data (full overwrite of that row), new ones
are inserted, and ones that no longer appear in the API response are left
untouched (the table is never truncated, so nothing is ever deleted).

Like "priorita" and "opatrenie", the list call itself already returns full
records (no separate per-id detail endpoint), and it doesn't support
limit=-1 - the total count has to be discovered first (a bare call returns
size with an empty results list), then requested explicitly via
limit={size}.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

from pathlib import Path

import duckdb
import requests

API_URL = "https://api.itms21.sk/public/v1/typakcieprogramu"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate table inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_TYPAKCIEPROGRAMU = f"{TABLE_PREFIX}program_typakcieprogramu"

REQUEST_TIMEOUT = 30


def fetch_typakcieprogramu() -> list[dict]:
    """Discover the total count via a bare call, then fetch every
    typakcieprogramu record in one request using that count as the limit."""
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


# Mirrors the ITMS21 "typakcieprogramu" list endpoint field-by-field.
# column_name -> (dotted path in the raw JSON, DuckDB type).
TYPAKCIEPROGRAMU_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("nazovsk", "nazovSk", "VARCHAR"),
    ("nazoven", "nazovEn", "VARCHAR"),
    ("nazovde", "nazovDe", "VARCHAR"),
    ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
    ("opatrenie_id", "opatrenie.id", "BIGINT"),
    ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
]


TABLE_COMMENTS: dict[str, str] = {
    TABLE_TYPAKCIEPROGRAMU: (
        "One row per programme action type ('typ akcie programu'), a "
        "classification of the kind of intervention a measure/specific "
        "objective funds within a given region category. Existing rows are "
        "fully rewritten with the latest API data on every sync; nothing is "
        "ever deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_TYPAKCIEPROGRAMU: {
        "id": "ITMS21 numeric id of the programme action type.",
        "href": "API URL of this programme action type's own detail resource.",
        "kod": "Programme action type code.",
        "nazovsk": "Programme action type name in Slovak.",
        "nazoven": "Programme action type name in English.",
        "nazovde": "Programme action type name in German.",
        "kategoriaregionov_id": "Id of the region category this action type applies to (e.g. less-developed vs. more-developed region).",
        "opatrenie_id": "Id of the parent measure this action type belongs to (itms21_program_opatrenie.id), when applicable.",
        "specifickycielprogramu_id": "Id of the parent specific objective this action type belongs to (itms21_program_specifickycielprogramu.id), when applicable.",
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
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in TYPAKCIEPROGRAMU_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_TYPAKCIEPROGRAMU} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)


def upsert_row(con: duckdb.DuckDBPyConnection, item: dict) -> None:
    columns_sql = ", ".join(col for col, _, _ in TYPAKCIEPROGRAMU_COLUMNS)
    placeholders = ", ".join("?" for _ in TYPAKCIEPROGRAMU_COLUMNS)
    update_sql = ", ".join(f"{col} = excluded.{col}" for col, _, _ in TYPAKCIEPROGRAMU_COLUMNS if col != "id")
    values = [_get(item, path) for _, path, _ in TYPAKCIEPROGRAMU_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_TYPAKCIEPROGRAMU} ({columns_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {update_sql}",
        values,
    )


def sync_typakcieprogramu() -> int:
    """Fetch every typakcieprogramu and upsert it: existing ids are fully
    rewritten, new ids are inserted, and nothing is ever deleted.

    Returns the number of typakcieprogramu synced.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_table(con)
    apply_comments(con)

    print("Fetching typakcieprogramu list...")
    items = fetch_typakcieprogramu()
    print(f"List returned {len(items)} typakcieprogramu.")

    for item in items:
        upsert_row(con, item)

    con.commit()
    con.close()

    return len(items)


def main():
    synced = sync_typakcieprogramu()
    print(f"Done. {synced} typakcieprogramu synced.")


if __name__ == "__main__":
    main()
