"""
Fetches the ITMS21 "ukazovatelprojektovy" (project indicator) list, then
fetches full detail ONLY for ids not yet stored in DuckDB. The list itself is
not persisted - it exists purely to discover ids. Ids already stored are
never re-fetched and never deleted, even if they disappear from the API list
(purely additive incremental sync, same approach as fetch_zop.py /
fetch_zonfp.py / fetch_vyzvy.py).

Each detail fetched decomposes into a normalized itms21_ukazovatelprojektovy
table plus itms21_ukazovatelprojektovy_casplnenia and
itms21_ukazovatelprojektovy_fondy child tables (both single-column id lists,
same shape as PROJEKT_INTENZITY/PROJEKT_KATEGORIAREGIONOV in
fetch_projects.py). Every child table carries ukazovatelprojektovy_id so it
can be joined back to the parent. Rows are inserted once and never
updated/deleted, gated by the same "id not yet known" check.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/ukazovatelprojektovy?limit=-1"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/ukazovatelprojektovy/id/{id}"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_UKAZOVATELPROJEKTOVY = f"{TABLE_PREFIX}ukazovatelprojektovy"

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

LIST_REQUEST_TIMEOUT = 180  # the list endpoint returns every ukazovatelprojektovy in one response (limit=-1)


def fetch_list() -> list[dict]:
    """Call the list endpoint and return all ukazovatelprojektovy summary
    records (used only to discover ids - the list itself is not stored),
    with retry on failure."""
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


def fetch_detail(ukazovatel_id: int) -> dict | None:
    """Call the detail endpoint for one ukazovatelprojektovy, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=ukazovatel_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={ukazovatel_id} after {RETRY_ATTEMPTS} attempts: {e}")
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
    """Build the itms21_ukazovatelprojektovy_<bare_name> child table name."""
    return f"{TABLE_PREFIX}ukazovatelprojektovy_{bare_name}"


# --- Full normalized schema -------------------------------------------------
# Mirrors the ITMS21 "ukazovatelprojektovy" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).

UKAZOVATELPROJEKTOVY_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
    ("definiciade", "definiciaDe", "VARCHAR"),
    ("definiciaen", "definiciaEn", "VARCHAR"),
    ("definiciask", "definiciaSk", "VARCHAR"),
    ("evidenciapodlapohlavia", "evidenciaPodlaPohlavia", "BOOLEAN"),
    ("mernajednotka_id", "mernaJednotka.id", "BIGINT"),
    ("nazovde", "nazovDe", "VARCHAR"),
    ("nazoven", "nazovEn", "VARCHAR"),
    ("nazovsk", "nazovSk", "VARCHAR"),
    ("platnost", "platnost", "BOOLEAN"),
    ("typ", "typ", "VARCHAR"),
    ("typvypoctu", "typVypoctu", "VARCHAR"),
]

# bare_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "casplnenia": ("casPlnenia", [("id", "id", "BIGINT")]),
    "fondy": ("fondy", [("id", "id", "BIGINT")]),
}


TABLE_COMMENTS: dict[str, str] = {
    TABLE_UKAZOVATELPROJEKTOVY: (
        "One row per project indicator ('ukazovateľ projektový'), a metric "
        "definition that projects report progress against. Purely additive: "
        "once an id is stored it is never re-fetched, updated, or deleted, "
        "even if it disappears from a later API list response."
    ),
    _t("casplnenia"): (
        "Points in time ('čas plnenia') at which this indicator is measured/reported. "
        "Child rows carrying ukazovatelprojektovy_id back to itms21_ukazovatelprojektovy; "
        "inserted once when the parent detail is first fetched, never updated/deleted."
    ),
    _t("fondy"): (
        "Funds this indicator applies to. Child rows carrying ukazovatelprojektovy_id back to "
        "itms21_ukazovatelprojektovy; inserted once when the parent detail is first fetched, "
        "never updated/deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_UKAZOVATELPROJEKTOVY: {
        "id": "ITMS21 numeric id of the project indicator.",
        "href": "API URL of this project indicator's own detail resource.",
        "kod": "Project indicator code.",
        "createdat": "Record creation timestamp in the source system.",
        "updatedat": "Record last-updated timestamp in the source system.",
        "definiciade": "Indicator definition, in German.",
        "definiciaen": "Indicator definition, in English.",
        "definiciask": "Indicator definition, in Slovak.",
        "evidenciapodlapohlavia": "Whether the indicator is tracked broken down by sex.",
        "mernajednotka_id": "Id of the unit of measure this indicator is reported in.",
        "nazovde": "Indicator name, in German.",
        "nazoven": "Indicator name, in English.",
        "nazovsk": "Indicator name, in Slovak.",
        "platnost": "Whether the indicator is currently valid/active.",
        "typ": "Type of the indicator.",
        "typvypoctu": "Calculation method of the indicator.",
    },
    _t("casplnenia"): {
        "ukazovatelprojektovy_id": "Id of the parent project indicator (itms21_ukazovatelprojektovy.id).",
        "id": "Id of the point-in-time / milestone at which the indicator is measured.",
    },
    _t("fondy"): {
        "ukazovatelprojektovy_id": "Id of the parent project indicator (itms21_ukazovatelprojektovy.id).",
        "id": "Id of the fund this indicator applies to.",
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
    """Create itms21_ukazovatelprojektovy and its child tables if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in UKAZOVATELPROJEKTOVY_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_UKAZOVATELPROJEKTOVY} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                ukazovatelprojektovy_id BIGINT,
                {cols_sql}
            )
        """)


def store_full_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Decompose one ukazovatelprojektovy's full detail JSON into
    itms21_ukazovatelprojektovy + all child tables. Only ever called once per
    id (gated by get_known_ids in sync_ukazovatelprojektovy), so this is a
    plain INSERT - never re-fetched, never updated."""
    ukazovatel_id = detail.get("id")

    columns_sql = ", ".join(col for col, _, _ in UKAZOVATELPROJEKTOVY_COLUMNS)
    placeholders = ", ".join("?" for _ in UKAZOVATELPROJEKTOVY_COLUMNS)
    values = [_get(detail, path) for _, path, _ in UKAZOVATELPROJEKTOVY_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_UKAZOVATELPROJEKTOVY} ({columns_sql}) VALUES ({placeholders}) "
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
            [ukazovatel_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (ukazovatelprojektovy_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_UKAZOVATELPROJEKTOVY}").fetchall()
    return {row[0] for row in rows}


def sync_ukazovatelprojektovy() -> tuple[int, int, int]:
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

    print("Fetching ukazovatelprojektovy list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} ukazovatelprojektovy.")

    known_ids = get_known_ids(con)

    to_fetch = [item.get("id") for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_fetch)} new ukazovatelprojektovy to fetch; "
          f"{len(list_items) - len(to_fetch)} already stored (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, uid): uid for uid in to_fetch}
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
    total, fetched, failed = sync_ukazovatelprojektovy()
    print(f"Done. {total} total in list, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
