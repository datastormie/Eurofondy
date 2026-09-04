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

DB_PATH = Path("data/eurofondy.duckdb")  # shared DuckDB file, separate tables inside

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
    ensure_tables(con)

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
