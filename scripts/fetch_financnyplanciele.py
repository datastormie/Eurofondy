"""
Fetches the ITMS21 "financnyplanciele" (specific objective financial plan)
records and syncs them into two DuckDB tables: itms21_program_financneciele
(one row per specific objective's financial plan) and its child
itms21_program_financneciele_clenenie (the plan broken down further by a
"delenieFP" funding sub-split).

Same shape as fetch_financnyplanpriority.py: there is no standalone
list/detail call for this entity - the API only supports filtering by a
parent id (specifickyCielProgramuId), so this script reads its set of ids to
iterate from slovakia.itms21_program_specifickycielprogramu (populated by
fetch_specifickycielprogramu.py), then calls the list endpoint once per
specific objective id with limit=-1. That per-id response already embeds
full nested detail (including the financnyPlanCieleClenenie child rows), so
there is no separate per-id detail call.

Hard dependency: fetch_specifickycielprogramu.py must have already run at
least once against this DuckDB file, since
itms21_program_specifickycielprogramu is this script's only source of ids. If
that table doesn't exist yet, this script raises immediately instead of
silently doing nothing.

Unlike the archetype-#2/#3 "gate on the id we're about to fetch" pattern, this
script cannot gate network calls on the specific objective id itself: DuckDB
commits each statement as it executes (there is no held-open transaction for
con.commit() to flush - verified this empirically, an uncommitted INSERT still
survives a hard process kill), so if a specific objective's several
financnyplanciele rows were inserted one at a time and the process died
partway through, marking the whole specific objective as "done" the moment
any one row existed would silently and permanently drop the rest. So instead
this follows fetch_ciselniky.py's approach for the same
one-parent-many-children shape: every specific objective id is queried again
on every run (cheap - there are only about a hundred), and rows are
de-duplicated on insert at the finest available grain -
itms21_program_financneciele by its own id (get_known_ids, plus an
ON CONFLICT (id) DO NOTHING backstop), itms21_program_financneciele_clenenie
by the (financneciele_id, id) pair (get_known_clenenie_pairs) since it has no
primary key of its own. That makes a partial/interrupted run harmless: a
re-run simply inserts whatever didn't make it in last time and skips the
rest.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml), right after
fetch_specifickycielprogramu.py.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/financnyplanciele"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

# Table names (the "program_" infix, and "financneciele" dropping "plan" from
# the URL entity name "financnyplanciele") match the DWH attribute mapping
# supplied for this endpoint - not an accidental divergence from
# fetch_financnyplanpriority.py's itms21_financnyplanpriority naming, which
# follows its own supplied mapping instead.
TABLE_PREFIX = "itms21_"
TABLE_FINANCNECIELE = f"{TABLE_PREFIX}program_financneciele"
TABLE_FINANCNECIELE_CLENENIE = f"{TABLE_PREFIX}program_financneciele_clenenie"
SOURCE_TABLE_SCP = f"{TABLE_PREFIX}program_specifickycielprogramu"

MAX_WORKERS = 8
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2
REQUEST_TIMEOUT = 30


def fetch_for_scp(scp_id: int) -> list[dict] | None:
    """Call the list endpoint filtered to one specifickycielprogramu id
    (limit=-1), with basic retry on failure. Returns None (never an empty
    list vs. failure ambiguity) only when every attempt raises."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(
                LIST_URL, params={"limit": -1, "specifickyCielProgramuId": scp_id}, timeout=REQUEST_TIMEOUT
            )
            resp.raise_for_status()
            return resp.json()["results"]
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED specifickyCielProgramuId={scp_id} after {RETRY_ATTEMPTS} attempts: {e}")
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


# Mirrors the ITMS21 "financnyplanciele" list endpoint field-by-field.
# column_name -> (dotted path in the raw JSON, DuckDB type).
# specifickycielprogramu_id is not part of the JSON - it's the query filter
# used to fetch the record, added here so results can be joined back to
# their specific objective.
FINANCNECIELE_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("plansumaeu", "planSumaEU", "DOUBLE"),
    ("plansumanarodne", "planSumaNarodne", "DOUBLE"),
    ("plansumasukromne", "planSumaSukromne", "DOUBLE"),
    ("plansumaverejne", "planSumaVerejne", "DOUBLE"),
    ("zakladprevypocet", "zakladPreVypocet", "VARCHAR"),
]

# Mirrors one item of the nested "financnyPlanCieleClenenie" list.
# financneciele_id is the parent FK, added the same way as every other child
# table in this repo.
FINANCNECIELE_CLENENIE_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("deleniefp_id", "delenieFP.id", "BIGINT"),
    ("deleniefp_fond_id", "delenieFP.fond.id", "BIGINT"),
    ("deleniefp_poradie", "delenieFP.poradie", "INTEGER"),
    ("plansumaeu", "planSumaEU", "DOUBLE"),
    ("plansumanarodne", "planSumaNarodne", "DOUBLE"),
    ("plansumasukromne", "planSumaSukromne", "DOUBLE"),
    ("plansumaverejne", "planSumaVerejne", "DOUBLE"),
]


TABLE_COMMENTS: dict[str, str] = {
    TABLE_FINANCNECIELE: (
        "One row per specific objective's financial plan ('financny plan "
        "cielov'). Ids to iterate come from itms21_program_specifickycielprogramu "
        "(populated by fetch_specifickycielprogramu.py). Purely additive: once "
        "a specific objective id has rows stored here it is never queried "
        "again, and existing rows are never updated or deleted."
    ),
    TABLE_FINANCNECIELE_CLENENIE: (
        "Further breakdown of a specific objective financial plan row by a "
        "'delenieFP' funding sub-split. Child rows carrying financneciele_id "
        "back to itms21_program_financneciele; inserted once alongside their "
        "parent, never updated or deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_FINANCNECIELE: {
        "id": "ITMS21 numeric id of the specific objective financial plan record.",
        "specifickycielprogramu_id": "Id of the parent specific objective (itms21_program_specifickycielprogramu.id); the query filter used to fetch this record, not part of the API's own JSON.",
        "plansumaeu": "Planned EU contribution amount, in euro.",
        "plansumanarodne": "Planned national (public + private) contribution amount, in euro.",
        "plansumasukromne": "Planned private contribution amount, in euro.",
        "plansumaverejne": "Planned public national contribution amount, in euro.",
        "zakladprevypocet": "Basis used for calculating this financial plan row (e.g. 'COV' for total eligible cost).",
    },
    TABLE_FINANCNECIELE_CLENENIE: {
        "financneciele_id": "Id of the parent specific objective financial plan row (itms21_program_financneciele.id).",
        "id": "ITMS21 numeric id of this breakdown row.",
        "deleniefp_id": "Id of the funding sub-split ('delenie financneho planu') this breakdown row belongs to, when applicable.",
        "deleniefp_fond_id": "Id of the fund associated with the funding sub-split.",
        "deleniefp_poradie": "Display order of the funding sub-split within its parent.",
        "plansumaeu": "Planned EU contribution amount for this breakdown, in euro.",
        "plansumanarodne": "Planned national (public + private) contribution amount for this breakdown, in euro.",
        "plansumasukromne": "Planned private contribution amount for this breakdown, in euro.",
        "plansumaverejne": "Planned public national contribution amount for this breakdown, in euro.",
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


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM information_schema.tables WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?)",
        [DB_SCHEMA, name],
    ).fetchone() is not None


def ensure_full_schema(con: duckdb.DuckDBPyConnection) -> None:
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in FINANCNECIELE_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_FINANCNECIELE} (
            specifickycielprogramu_id BIGINT,
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in FINANCNECIELE_CLENENIE_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_FINANCNECIELE_CLENENIE} (
            financneciele_id BIGINT,
            {cols_sql}
        )
    """)


def store_records(
    con: duckdb.DuckDBPyConnection,
    scp_id: int,
    records: list[dict],
    known_ids: set[int],
    known_clenenie_pairs: set[tuple[int, int]],
) -> tuple[int, int]:
    """Insert financnyplanciele records (and their clenenie child rows)
    returned for one specifickycielprogramu id, skipping anything already
    stored (known_ids / known_clenenie_pairs are snapshotted once per sync
    run - see sync_financnyplanciele - which is safe because a given
    financnyplanciele/clenenie id only ever appears under one specific
    objective, so no two concurrently-processed specific objective ids can
    race on the same row).

    Returns (new_parent_rows, new_clenenie_rows).
    """
    columns_sql = ", ".join(col for col, _, _ in FINANCNECIELE_COLUMNS)
    placeholders = ", ".join("?" for _ in FINANCNECIELE_COLUMNS)
    clenenie_columns_sql = ", ".join(col for col, _, _ in FINANCNECIELE_CLENENIE_COLUMNS)
    clenenie_placeholders = ", ".join("?" for _ in FINANCNECIELE_CLENENIE_COLUMNS)

    new_parent_rows = 0
    new_clenenie_rows = 0

    for record in records:
        record_id = record.get("id")
        if record_id not in known_ids:
            values = [scp_id] + [_get(record, path) for _, path, _ in FINANCNECIELE_COLUMNS]
            con.execute(
                f"INSERT INTO {TABLE_FINANCNECIELE} (specifickycielprogramu_id, {columns_sql}) VALUES (?, {placeholders}) "
                f"ON CONFLICT (id) DO NOTHING",
                values,
            )
            new_parent_rows += 1

        clenenie_items = [
            item for item in (record.get("financnyPlanCieleClenenie") or [])
            if (record_id, item.get("id")) not in known_clenenie_pairs
        ]
        if clenenie_items:
            rows = [
                [record_id] + [_get(item, path) for _, path, _ in FINANCNECIELE_CLENENIE_COLUMNS]
                for item in clenenie_items
            ]
            con.executemany(
                f"INSERT INTO {TABLE_FINANCNECIELE_CLENENIE} "
                f"(financneciele_id, {clenenie_columns_sql}) VALUES (?, {clenenie_placeholders})",
                rows,
            )
            new_clenenie_rows += len(clenenie_items)

    return new_parent_rows, new_clenenie_rows


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """financnyplanciele ids already stored, so they're never re-inserted."""
    rows = con.execute(f"SELECT id FROM {TABLE_FINANCNECIELE}").fetchall()
    return {row[0] for row in rows}


def get_known_clenenie_pairs(con: duckdb.DuckDBPyConnection) -> set[tuple[int, int]]:
    """(financneciele_id, id) pairs already stored, so clenenie rows are
    never re-inserted - same shape as fetch_ciselniky.py's get_known_pairs,
    needed because this child table has no primary key."""
    rows = con.execute(f"SELECT financneciele_id, id FROM {TABLE_FINANCNECIELE_CLENENIE}").fetchall()
    return {(row[0], row[1]) for row in rows}


def get_source_scp_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Specific objective ids to iterate, read from
    itms21_program_specifickycielprogramu (populated by
    fetch_specifickycielprogramu.py). Raises if that table doesn't exist yet
    - this script has no list endpoint of its own and depends entirely on
    fetch_specifickycielprogramu.py having run first."""
    if not _table_exists(con, SOURCE_TABLE_SCP):
        raise RuntimeError(
            f"{DB_SCHEMA}.{SOURCE_TABLE_SCP} does not exist. "
            "Run scripts/fetch_specifickycielprogramu.py first - it populates this table, "
            "which is the only source of ids for fetch_financnyplanciele.py."
        )
    rows = con.execute(f"SELECT DISTINCT id FROM {SOURCE_TABLE_SCP} WHERE id IS NOT NULL").fetchall()
    return {row[0] for row in rows}


def sync_financnyplanciele() -> tuple[int, int, int, int]:
    """Query every specific objective id every run (cheap - there are only
    about a hundred) and insert only the financnyplanciele/clenenie rows not
    already stored.

    Returns (total_scp, new_parent_rows, new_clenenie_rows, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_full_schema(con)
    apply_comments(con)

    scp_ids = get_source_scp_ids(con)
    print(f"{SOURCE_TABLE_SCP} has {len(scp_ids)} distinct specific objective id(s) to query.")

    known_ids = get_known_ids(con)
    known_clenenie_pairs = get_known_clenenie_pairs(con)

    new_parent_rows = 0
    new_clenenie_rows = 0
    failed_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(fetch_for_scp, sid): sid for sid in scp_ids}
        for i, future in enumerate(as_completed(future_to_id), start=1):
            scp_id = future_to_id[future]
            records = future.result()
            if records is None:
                failed_count += 1
                continue
            parent_added, clenenie_added = store_records(con, scp_id, records, known_ids, known_clenenie_pairs)
            new_parent_rows += parent_added
            new_clenenie_rows += clenenie_added

            if i % 50 == 0 or i == len(scp_ids):
                con.commit()  # periodic commit so progress survives an interruption
                print(f"  Progress: {i}/{len(scp_ids)} specific objectives processed "
                      f"({new_parent_rows} new rows, {failed_count} failed)")

    con.commit()
    con.close()

    return len(scp_ids), new_parent_rows, new_clenenie_rows, failed_count


def main():
    total, new_parent, new_clenenie, failed = sync_financnyplanciele()
    print(f"Done. {total} specific objectives queried, {new_parent} new financnyplanciele rows, "
          f"{new_clenenie} new clenenie rows, {failed} failed.")


if __name__ == "__main__":
    main()
