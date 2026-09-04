"""
Fetches the ITMS21 "specifickycielprogramu" (programme specific objective)
list and syncs it into two DuckDB tables: specifickycielprogramu already
known are always rewritten with the latest API data (full overwrite of that
row, including its kategoriaRegionov child rows), new ones are inserted, and
ones that no longer appear in the API response are left untouched (the
tables are never truncated, so nothing is ever deleted).

Like "priorita", the list call itself already returns full records (no
separate per-id detail endpoint), and it doesn't support limit=-1 - the total
count has to be discovered first (a bare call returns size with an empty
results list), then requested explicitly via offset=0&limit={size}.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

from pathlib import Path

import duckdb
import requests

API_URL = "https://api.itms21.sk/public/v1/specifickycielprogramu"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_SCP = f"{TABLE_PREFIX}program_specifickycielprogramu"
TABLE_SCP_KATEGORIAREGIONOV = f"{TABLE_PREFIX}program_specifickycielprogramu_kategoriaregionov"

REQUEST_TIMEOUT = 30


def fetch_specifickycielprogramu() -> list[dict]:
    """Discover the total count via a bare call, then fetch every record in
    one request using that count as the limit."""
    resp = requests.get(API_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    size = resp.json()["size"]

    resp = requests.get(API_URL, params={"offset": 0, "limit": size}, timeout=REQUEST_TIMEOUT)
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


# Mirrors the ITMS21 "specifickycielprogramu" list endpoint field-by-field.
# column_name -> (dotted path in the raw JSON, DuckDB type).
SCP_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("nazovsk", "nazovSk", "VARCHAR"),
    ("nazoven", "nazovEn", "VARCHAR"),
    ("nazovde", "nazovDe", "VARCHAR"),
    ("fond_id", "fond.id", "BIGINT"),
    ("priorita_id", "priorita.id", "BIGINT"),
    ("program_id", "program.id", "BIGINT"),
    ("technickaasistencia", "technickaAsistencia", "BOOLEAN"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
]


TABLE_COMMENTS: dict[str, str] = {
    TABLE_SCP: (
        "One row per programme specific objective ('specificky ciel "
        "programu'), a funding goal nested under a priority axis, under "
        "which measures ('opatrenie') are grouped. Existing rows (and their "
        "kategoriaregionov child rows) are fully rewritten with the latest "
        "API data on every sync; nothing is ever deleted."
    ),
    TABLE_SCP_KATEGORIAREGIONOV: (
        "Child rows listing which region categories (e.g. less-developed vs. "
        "more-developed region) a specific objective applies to. Fully "
        "refreshed alongside its parent row on every sync."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_SCP: {
        "id": "ITMS21 numeric id of the specific objective.",
        "href": "API URL of this specific objective's own detail resource.",
        "kod": "Specific objective code.",
        "nazovsk": "Specific objective name in Slovak.",
        "nazoven": "Specific objective name in English.",
        "nazovde": "Specific objective name in German.",
        "fond_id": "Id of the EU fund (e.g. ERDF, ESF+, Cohesion Fund) financing this specific objective.",
        "priorita_id": "Id of the parent priority axis this specific objective belongs to (itms21_program_priorita.id).",
        "program_id": "Id of the parent programme this specific objective belongs to (itms21_programs_current.program_id).",
        "technickaasistencia": "Whether this specific objective funds technical assistance rather than a substantive intervention.",
        "createdat": "Record creation timestamp in the source system.",
        "updatedat": "Record last-updated timestamp in the source system.",
    },
    TABLE_SCP_KATEGORIAREGIONOV: {
        "specifickycielprogramu_id": "Id of the parent specific objective (itms21_program_specifickycielprogramu.id).",
        "id": "Id of the region category that applies to this specific objective.",
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


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in SCP_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_SCP} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_SCP_KATEGORIAREGIONOV} (
            specifickycielprogramu_id BIGINT,
            id BIGINT
        )
    """)


def upsert_row(con: duckdb.DuckDBPyConnection, item: dict) -> None:
    scp_id = item.get("id")

    columns_sql = ", ".join(col for col, _, _ in SCP_COLUMNS)
    placeholders = ", ".join("?" for _ in SCP_COLUMNS)
    update_sql = ", ".join(f"{col} = excluded.{col}" for col, _, _ in SCP_COLUMNS if col != "id")
    values = [_get(item, path) for _, path, _ in SCP_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_SCP} ({columns_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {update_sql}",
        values,
    )

    # Full refresh of the child rows on every upsert, since the parent row
    # itself is also fully rewritten each run.
    con.execute(f"DELETE FROM {TABLE_SCP_KATEGORIAREGIONOV} WHERE specifickycielprogramu_id = ?", [scp_id])
    kat_rows = [[scp_id, kat.get("id")] for kat in item.get("kategoriaRegionov") or []]
    if kat_rows:
        con.executemany(
            f"INSERT INTO {TABLE_SCP_KATEGORIAREGIONOV} (specifickycielprogramu_id, id) VALUES (?, ?)",
            kat_rows,
        )


def sync_specifickycielprogramu() -> int:
    """Fetch every specifickycielprogramu and upsert it: existing ids are
    fully rewritten (including their kategoriaRegionov child rows), new ids
    are inserted, and nothing is ever deleted.

    Returns the number of specifickycielprogramu synced.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_tables(con)
    apply_comments(con)

    print("Fetching specifickycielprogramu list...")
    items = fetch_specifickycielprogramu()
    print(f"List returned {len(items)} specifickycielprogramu.")

    for item in items:
        upsert_row(con, item)

    con.commit()
    con.close()

    return len(items)


def main():
    synced = sync_specifickycielprogramu()
    print(f"Done. {synced} specifickycielprogramu synced.")


if __name__ == "__main__":
    main()
