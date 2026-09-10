"""
Fetches full detail for ITMS21 "polozkarozpocetprojekt" (project budget line
item) ids, then decomposes each into DuckDB. This endpoint has no list of its
own to page through - ids are discovered entirely from itms21_projekt_polozkyrozpoctu.id
(written by fetch_projects.py), so fetch_projects.py MUST run before this
script in .github/workflows/monthly.yml (it already does - fetch_projects.py
runs early in that file).

Other candidate sources were tried and dropped after live verification
against the real API: itms21_zonfp_polozkyrozpoctu.id (requested budget items
from the grant-application stage - ~92% turned out to be 404s, since most
never get promoted into this post-contract endpoint's id space) and this
table's own podpolozkyrozpoctu.id (a budget sub-item's id does NOT double as
a valid top-level id - 0% hit rate across all samples). itms21_projekt_polozkyrozpoctu.id
tested 100% valid across a live sample, same as the itms21_zop_vydavky.polozkarozpoctu_id
source it replaced (also 100% valid, but redundant with this one - a payment
request's expense item and the project's own budget line list point at the
same underlying ids). Revisit the dropped sources only if a future need
justifies the wasted-request cost of re-adding them.

Ids already stored in itms21_polozkarozpocetprojekt are never re-fetched and
never deleted, even if they disappear from a later reading of the source
table (purely additive incremental sync, same approach as
fetch_intenzitaprojekt.py - the other script in this repo with no list
endpoint of its own).

Each detail fetched decomposes into a normalized itms21_polozkarozpocetprojekt
table plus one itms21_polozkarozpocetprojekt_podpolozkyrozpoctu child table.
The child table carries polozkarozpocetprojekt_id so it can be joined back to
the parent. Rows are inserted once and never updated/deleted, gated by the
same "id not yet known" check.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/polozkarozpoctuprojekt/id/{id}"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_POLOZKA = f"{TABLE_PREFIX}polozkarozpocetprojekt"
SOURCE_TABLE_PROJEKT_POLOZKYROZPOCTU = f"{TABLE_PREFIX}projekt_polozkyrozpoctu"  # written by fetch_projects.py

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2


def fetch_detail(polozka_id: int) -> dict | None:
    """Call the detail endpoint for one polozkarozpocetprojekt, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=polozka_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={polozka_id} after {RETRY_ATTEMPTS} attempts: {e}")
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
    """Build the itms21_polozkarozpocetprojekt_<bare_name> child table name."""
    return f"{TABLE_PREFIX}polozkarozpocetprojekt_{bare_name}"


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM information_schema.tables WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?)",
        [DB_SCHEMA, name],
    ).fetchone() is not None


# --- Full normalized schema -------------------------------------------------
# Mirrors the ITMS21 "polozkarozpocetprojekt" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).

POLOZKA_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("aktivita_id", "aktivita.id", "BIGINT"),
    ("intenzita_id", "intenzita.id", "BIGINT"),
    ("skupinavydavkov_kod", "skupinaVydavkov.kod", "VARCHAR"),
    ("skupinavydavkov_nazovsk", "skupinaVydavkov.nazovSk", "VARCHAR"),
    ("skupinavydavkov_nazoven", "skupinaVydavkov.nazovEn", "VARCHAR"),
    ("skupinavydavkov_nazovde", "skupinaVydavkov.nazovDe", "VARCHAR"),
    ("skupinavydavkov_typakcieprogramu_id", "skupinaVydavkov.typAkcieProgramu.id", "BIGINT"),
    ("specifickyciel_id", "specifickyCiel.id", "BIGINT"),
    ("subjekt_id", "subjekt.id", "BIGINT"),
    ("vratenasuma", "vratenaSuma", "DOUBLE"),
    ("zazmluvnenasuma", "zazmluvnenaSuma", "DOUBLE"),
]

# bare_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "podpolozkyrozpoctu": ("podpolozkyRozpoctu", [
        ("id", "id", "BIGINT"),
        ("nazov", "nazov", "VARCHAR"),
        ("poradovecislo", "poradoveCislo", "INTEGER"),
        ("mernajednotka_id", "mernaJednotka.id", "BIGINT"),
        ("mnozstvo", "mnozstvo", "DOUBLE"),
        ("jednotkovacena", "jednotkovaCena", "DOUBLE"),
        ("zazmluvnenasuma", "zazmluvnenaSuma", "DOUBLE"),
        ("sumazopnarokovana", "sumaZopNarokovana", "DOUBLE"),
        ("vratenasuma", "vratenaSuma", "DOUBLE"),
    ]),
}


TABLE_COMMENTS: dict[str, str] = {
    TABLE_POLOZKA: (
        "One row per project budget line item ('polozka rozpoctu projektu'). "
        "Purely additive: once an id is stored it is never re-fetched, "
        "updated, or deleted. Has no list endpoint of its own - ids are "
        "discovered from itms21_projekt_polozkyrozpoctu.id."
    ),
    _t("podpolozkyrozpoctu"): (
        "Budget sub-items ('podpolozky rozpoctu') of a project budget line item. Child rows "
        "carrying polozkarozpocetprojekt_id back to itms21_polozkarozpocetprojekt; inserted once "
        "when the parent detail is first fetched, never updated/deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_POLOZKA: {
        "id": "ITMS21 numeric id of the project budget line item.",
        "href": "API URL of this project budget line item's own detail resource.",
        "aktivita_id": "Id of the project activity this budget item is linked to (itms21_aktivitaprojekt.id).",
        "intenzita_id": "Id of the aid intensity applied to this budget item.",
        "skupinavydavkov_kod": "Code of the expenditure group this budget item belongs to.",
        "skupinavydavkov_nazovsk": "Name of the expenditure group, in Slovak.",
        "skupinavydavkov_nazoven": "Name of the expenditure group, in English.",
        "skupinavydavkov_nazovde": "Name of the expenditure group, in German.",
        "skupinavydavkov_typakcieprogramu_id": (
            "Id of the programme action type the expenditure group is classified under "
            "(itms21_program_typakcieprogramu.id)."
        ),
        "specifickyciel_id": "Id of the specific objective this budget item is linked to.",
        "subjekt_id": "Id of the entity (subjekt) this budget item belongs to.",
        "vratenasuma": "Amount returned/refunded against this budget item, in euro.",
        "zazmluvnenasuma": "Contracted amount of this budget item, in euro.",
    },
    _t("podpolozkyrozpoctu"): {
        "polozkarozpocetprojekt_id": "Id of the parent budget line item (itms21_polozkarozpocetprojekt.id).",
        "id": "Id of the budget sub-item.",
        "nazov": "Name/description of the budget sub-item.",
        "poradovecislo": "Ordering position of the sub-item within the budget line item.",
        "mernajednotka_id": "Id of the unit of measure the sub-item's quantity is expressed in.",
        "mnozstvo": "Quantity of the budget sub-item.",
        "jednotkovacena": "Unit price of the budget sub-item, in euro.",
        "zazmluvnenasuma": "Contracted amount of the budget sub-item, in euro.",
        "sumazopnarokovana": "Amount of the budget sub-item claimed in a payment request (ZoP), in euro.",
        "vratenasuma": "Amount returned/refunded against the budget sub-item, in euro.",
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
    """Create itms21_polozkarozpocetprojekt and its child table if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in POLOZKA_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_POLOZKA} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                polozkarozpocetprojekt_id BIGINT,
                {cols_sql}
            )
        """)


def store_full_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Decompose one polozkarozpocetprojekt's full detail JSON into
    itms21_polozkarozpocetprojekt + its child table. Only ever called once
    per id (gated by get_known_ids in sync_polozkarozpocetprojekt), so this
    is a plain INSERT - never re-fetched, never updated."""
    polozka_id = detail.get("id")

    columns_sql = ", ".join(col for col, _, _ in POLOZKA_COLUMNS)
    placeholders = ", ".join("?" for _ in POLOZKA_COLUMNS)
    values = [_get(detail, path) for _, path, _ in POLOZKA_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_POLOZKA} ({columns_sql}) VALUES ({placeholders}) "
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
            [polozka_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (polozkarozpocetprojekt_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_POLOZKA}").fetchall()
    return {row[0] for row in rows}


def get_source_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Candidate ids to fetch, read from itms21_projekt_polozkyrozpoctu.id
    (populated by fetch_projects.py). Raises if that table doesn't exist yet -
    this script has no list endpoint of its own and depends entirely on
    fetch_projects.py having run first."""
    if not _table_exists(con, SOURCE_TABLE_PROJEKT_POLOZKYROZPOCTU):
        raise RuntimeError(
            f"{DB_SCHEMA}.{SOURCE_TABLE_PROJEKT_POLOZKYROZPOCTU} does not exist. "
            "Run scripts/fetch_projects.py first - it populates this table, which is the only "
            "source of ids for fetch_polozkarozpocetprojekt.py."
        )

    rows = con.execute(
        f"SELECT DISTINCT id FROM {SOURCE_TABLE_PROJEKT_POLOZKYROZPOCTU} WHERE id IS NOT NULL"
    ).fetchall()
    return {row[0] for row in rows}


def sync_polozkarozpocetprojekt() -> tuple[int, int, int]:
    """Fetch full details only for ids not already stored.

    Returns (total_source_ids, fetched_count, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_full_schema(con)
    apply_comments(con)

    source_ids = get_source_ids(con)
    print(f"Found {len(source_ids)} candidate polozkarozpocetprojekt id(s) in itms21_projekt_polozkyrozpoctu.")

    known_ids = get_known_ids(con)
    to_fetch = [i for i in source_ids if i not in known_ids]

    print(f"{len(to_fetch)} new polozkarozpocetprojekt(s) to fetch; "
          f"{len(source_ids) - len(to_fetch)} already stored (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, pid): pid for pid in to_fetch}
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

    return len(source_ids), fetched_count, failed_count


def main():
    total, fetched, failed = sync_polozkarozpocetprojekt()
    print(f"Done. {total} total source id(s), {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
