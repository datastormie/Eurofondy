"""
Fetches full detail for ITMS21 "intenzitaprojekt" (project aid intensity)
records. Unlike most other fetch scripts, there is no list endpoint here -
the set of ids to fetch is read directly from slovakia.itms21_projekt_intenzity
(the PROJEKT_INTENZITY child table populated by fetch_projects.py from each
project's "intenzity" field). Detail is fetched ONLY for ids not yet stored
in itms21_intenzita_projekt; each id is fetched exactly once and never
re-fetched, updated, or deleted afterward (purely additive incremental sync,
same approach as fetch_aktivitaprojekt.py).

Hard dependency: fetch_projects.py must have already run at least once
against this DuckDB file, since itms21_projekt_intenzity is this script's
only source of ids. If that table doesn't exist yet, this script raises
immediately instead of silently doing nothing. This dependency is also
encoded in .github/workflows/monthly.yml, which runs this script right after
fetch_projects.py.

"intenzitaprojekt (detail)" is a flat record (no inner list fields), so it's
stored as a single itms21_intenzita_projekt table with no child tables.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/intenzitaprojekt/id/{id}"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate table inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_INTENZITA_PROJEKT = f"{TABLE_PREFIX}intenzita_projekt"
SOURCE_TABLE_PROJEKT_INTENZITY = f"{TABLE_PREFIX}projekt_intenzity"

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2


def fetch_detail(intenzita_id: int) -> dict | None:
    """Call the detail endpoint for one intenzitaprojekt, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=intenzita_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={intenzita_id} after {RETRY_ATTEMPTS} attempts: {e}")
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


# Mirrors the ITMS21 "intenzitaprojekt" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).
INTENZITA_PROJEKT_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("nazov", "nazov", "VARCHAR"),
    ("cerpanieeupd", "cerpanieEUpd", "DOUBLE"),
    ("cerpanieeuvys", "cerpanieEUvys", "DOUBLE"),
    ("cerpanieeuzop", "cerpanieEUzop", "DOUBLE"),
    ("cerpanieropd", "cerpanieROpd", "DOUBLE"),
    ("cerpanierovys", "cerpanieROvys", "DOUBLE"),
    ("cerpanierozop", "cerpanieROzop", "DOUBLE"),
    ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
    ("podieleu", "podielEu", "DOUBLE"),
    ("podielpr", "podielPr", "DOUBLE"),
    ("podielsr", "podielSr", "DOUBLE"),
    ("podielvz", "podielVz", "DOUBLE"),
    ("priorita_id", "priorita.id", "BIGINT"),
    ("specifickyciel_id", "specifickyCiel.id", "BIGINT"),
    ("subjekt_id", "subjekt.id", "BIGINT"),
    ("sumazazmluvnena", "sumaZazmluvnena", "DOUBLE"),
]


TABLE_COMMENTS: dict[str, str] = {
    TABLE_INTENZITA_PROJEKT: (
        "One row per project aid intensity record ('intenzita projektu'), "
        "describing the EU/national/own-source funding split for a project. "
        "Ids to fetch come from slovakia.itms21_projekt_intenzity (populated "
        "by fetch_projects.py), not from a list endpoint of its own. Purely "
        "additive: once an id is stored it is never re-fetched, updated, or "
        "deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_INTENZITA_PROJEKT: {
        "id": "ITMS21 numeric id of the aid intensity record; matches itms21_projekt_intenzity.id.",
        "href": "API URL of this aid intensity record's own detail resource.",
        "nazov": "Aid intensity record name.",
        "cerpanieeupd": "EU funds drawn amount, PD stage, in euro.",
        "cerpanieeuvys": "EU funds drawn amount, VYS (vysúčtovanie/settlement) stage, in euro.",
        "cerpanieeuzop": "EU funds drawn amount, ZOP (žiadosť o platbu/payment request) stage, in euro.",
        "cerpanieropd": "State budget (RO) funds drawn amount, PD stage, in euro.",
        "cerpanierovys": "State budget (RO) funds drawn amount, VYS (vysúčtovanie/settlement) stage, in euro.",
        "cerpanierozop": "State budget (RO) funds drawn amount, ZOP (žiadosť o platbu/payment request) stage, in euro.",
        "kategoriaregionov_id": "Id of the region category (kategória regiónov) this intensity applies to.",
        "podieleu": "Share of funding covered by the EU, as a fraction/percentage.",
        "podielpr": "Share of funding covered by the beneficiary's own resources, as a fraction/percentage.",
        "podielsr": "Share of funding covered by the state budget (SR), as a fraction/percentage.",
        "podielvz": "Share of funding covered by other own sources (vlastné zdroje), as a fraction/percentage.",
        "priorita_id": "Id of the priority (priorita) this intensity is classified under.",
        "specifickyciel_id": "Id of the specific objective (špecifický cieľ) this intensity is classified under.",
        "subjekt_id": "Id of the entity/institution this aid intensity record applies to.",
        "sumazazmluvnena": "Contracted amount (suma zazmluvnená) associated with this intensity, in euro.",
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


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in INTENZITA_PROJEKT_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_INTENZITA_PROJEKT} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)


def store_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Insert one intenzitaprojekt's detail JSON into itms21_intenzita_projekt.
    Only ever called once per id (gated by get_known_ids in
    sync_intenzitaprojekt), so this is a plain INSERT - never re-fetched,
    never updated."""
    columns_sql = ", ".join(col for col, _, _ in INTENZITA_PROJEKT_COLUMNS)
    placeholders = ", ".join("?" for _ in INTENZITA_PROJEKT_COLUMNS)
    values = [_get(detail, path) for _, path, _ in INTENZITA_PROJEKT_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_INTENZITA_PROJEKT} ({columns_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO NOTHING",
        values,
    )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_INTENZITA_PROJEKT}").fetchall()
    return {row[0] for row in rows}


def get_source_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids to fetch, read from slovakia.itms21_projekt_intenzity (populated by
    fetch_projects.py). Raises if that table doesn't exist yet - this script
    has no list endpoint of its own and depends entirely on fetch_projects.py
    having run first."""
    if not _table_exists(con, SOURCE_TABLE_PROJEKT_INTENZITY):
        raise RuntimeError(
            f"{DB_SCHEMA}.{SOURCE_TABLE_PROJEKT_INTENZITY} does not exist. "
            "Run scripts/fetch_projects.py first - it populates this table, "
            "which is the only source of ids for fetch_intenzitaprojekt.py."
        )
    rows = con.execute(f"SELECT DISTINCT id FROM {SOURCE_TABLE_PROJEKT_INTENZITY} WHERE id IS NOT NULL").fetchall()
    return {row[0] for row in rows}


def sync_intenzitaprojekt() -> tuple[int, int, int]:
    """Fetch full details only for ids in itms21_projekt_intenzity not already
    stored in itms21_intenzita_projekt.

    Returns (total_source_ids, fetched_count, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_table(con)
    apply_comments(con)

    source_ids = get_source_ids(con)
    print(f"{SOURCE_TABLE_PROJEKT_INTENZITY} has {len(source_ids)} distinct id(s).")

    known_ids = get_known_ids(con)
    to_fetch = [i for i in source_ids if i not in known_ids]

    print(f"{len(to_fetch)} new intenzitaprojekt(s) to fetch; "
          f"{len(source_ids) - len(to_fetch)} already stored (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, iid): iid for iid in to_fetch}
            for i, future in enumerate(as_completed(future_to_id), start=1):
                detail = future.result()
                if detail is None:
                    failed_count += 1
                    continue
                store_detail(con, detail)
                fetched_count += 1

                if i % 50 == 0 or i == len(to_fetch):
                    con.commit()  # periodic commit so progress survives an interruption
                    print(f"  Progress: {i}/{len(to_fetch)} processed "
                          f"({fetched_count} ok, {failed_count} failed)")

    con.commit()
    con.close()

    return len(source_ids), fetched_count, failed_count


def main():
    total, fetched, failed = sync_intenzitaprojekt()
    print(f"Done. {total} total ids in {SOURCE_TABLE_PROJEKT_INTENZITY}, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
