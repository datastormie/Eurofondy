"""
Fetches the ITMS21 "planovanavyzva" (planned call for proposals) list, then
fetches full detail ONLY for ids not yet stored in DuckDB. The list itself is
not persisted - it exists purely to discover ids. Ids already stored are
never re-fetched and never deleted, even if they disappear from the API list
(purely additive incremental sync, same approach as fetch_vyzvy.py).

Each detail fetched decomposes into a normalized itms21_planovanavyzva table
plus itms21_planovanavyzva_* child tables (one row per inner list item),
mirroring the ITMS21 "planovanavyzva (detail)" field-by-field mapping. Every
child table carries planovanavyzva_id so it can be joined back to
itms21_planovanavyzva. Rows are inserted once and never updated/deleted,
gated by the same "id not yet known" check.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/planovanavyzva?limit=-1"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/planovanavyzva/id/{id}"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_PLANOVANAVYZVA = f"{TABLE_PREFIX}PLANOVANAVYZVA".lower()

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

LIST_REQUEST_TIMEOUT = 180  # the list endpoint returns every planovanavyzva in one response (limit=-1)


def fetch_list() -> list[dict]:
    """Call the list endpoint and return all planovanavyzva summary records
    (used only to discover ids - the list itself is not stored), with retry
    on failure."""
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


def fetch_detail(planovanavyzva_id: int) -> dict | None:
    """Call the detail endpoint for one planovanavyzva, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=planovanavyzva_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={planovanavyzva_id} after {RETRY_ATTEMPTS} attempts: {e}")
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
    """Prefix a bare child-table name (as used as a CHILD_TABLES key) with TABLE_PREFIX."""
    return f"{TABLE_PREFIX}{bare_name}".lower()


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM information_schema.tables WHERE lower(table_name) = lower(?)", [name]
    ).fetchone() is not None


def _rename_columns(con: duckdb.DuckDBPyConnection, table: str, columns: list[str]) -> None:
    """Rename every column in `columns` from its old ALL-CAPS form to its
    current lowercase form; matching is case-insensitive, so this is a no-op
    once a column is already lowercase (safe to run on every invocation)."""
    for col in columns:
        con.execute(f"ALTER TABLE {table} RENAME COLUMN {col.upper()} TO {col}")


# --- Full normalized schema -------------------------------------------------
# Mirrors the ITMS21 "planovanavyzva" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).

PLANOVANAVYZVA_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("nazovsk", "nazovSk", "VARCHAR"),
    ("nazoven", "nazovEn", "VARCHAR"),
    ("nazovde", "nazovDe", "VARCHAR"),
    ("programskratka", "programSkratka", "VARCHAR"),
    ("programoveobdobie", "programoveObdobie", "VARCHAR"),
    ("mrk", "mrk", "VARCHAR"),
    ("schema", "schema", "BOOLEAN"),
    ("dvekola", "dveKola", "BOOLEAN"),
    ("typ", "typ", "VARCHAR"),
    ("typ1kolo", "typ1Kolo", "VARCHAR"),
    ("typ2kolo", "typ2Kolo", "VARCHAR"),
    ("druh", "druh", "VARCHAR"),
    ("zameranieprojektu", "zameranieProjektu", "VARCHAR"),
    ("vyhlasena", "vyhlasena", "BOOLEAN"),
    ("zrusena", "zrusena", "BOOLEAN"),
    ("obsahujemiestorealizaciezahranicie", "obsahujeMiestoRealizacieZahranicie", "BOOLEAN"),
    ("sumaeu", "sumaEu", "DOUBLE"),
    ("sumasr", "sumaSr", "DOUBLE"),
    ("datumvyhlasenia1kolo", "datumVyhlasenia1Kolo", "BIGINT"),
    ("datumvyhlasenia2kolo", "datumVyhlasenia2Kolo", "BIGINT"),
    ("datumuzavretia1kolo", "datumUzavretia1Kolo", "BIGINT"),
    ("datumuzavretia2kolo", "datumUzavretia2Kolo", "BIGINT"),
    ("trvanie1kolo", "trvanie1Kolo", "INTEGER"),
    ("trvanie2kolo", "trvanie2Kolo", "INTEGER"),
    ("poskytovatel_id", "poskytovatel.id", "BIGINT"),
    ("program_id", "program.id", "BIGINT"),
    ("vyhlasovatel_id", "vyhlasovatel.id", "BIGINT"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
]

# table_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "PLANOVANAVYZVA_DOKUMENT": ("dokument", [
        ("nazov", "nazov", "VARCHAR"),
        ("uuid", "uuid", "VARCHAR"),
    ]),
    "PLANOVANAVYZVA_EXTERNYZDROJ": ("externyZdroj", [
        ("nazov", "nazov", "VARCHAR"),
        ("url", "url", "VARCHAR"),
    ]),
    "PLANOVANAVYZVA_KATEGORIAREGIONOV": ("kategoriaRegionov", [("id", "id", "BIGINT")]),
    "PLANOVANAVYZVA_MIESTOREALIZACIE": ("miestoRealizacie", [("id", "id", "BIGINT")]),
    "PLANOVANAVYZVA_OPATRENIE": ("opatrenie", [("id", "id", "BIGINT")]),
    "PLANOVANAVYZVA_PROJEKTOVYZAMERIUS": ("projektovyZamerIUS", [("id", "id", "BIGINT")]),
    "PLANOVANAVYZVA_SPECIFICKYCIELPROGRAMU": ("specifickyCielProgramu", [("id", "id", "BIGINT")]),
    "PLANOVANAVYZVA_TYPAKCIEPROGRAMU": ("typAkcieProgramu", [("id", "id", "BIGINT")]),
    "PLANOVANAVYZVA_ZIADATEL": ("ziadatel", [("id", "id", "BIGINT")]),
    "PLANOVANAVYZVA_VYZVA": ("vyzva", [("id", "id", "BIGINT")]),
}


TABLE_COMMENTS: dict[str, str] = {
    TABLE_PLANOVANAVYZVA: (
        "One row per planned/forecast call for proposals ('planovana "
        "vyzva'), published ahead of time on a programme's indicative call "
        "schedule before the actual call is opened. Purely additive: once "
        "an id is stored it is never re-fetched, updated, or deleted."
    ),
    _t("PLANOVANAVYZVA_DOKUMENT"): "Documents (e.g. draft call templates) attached to a planned call.",
    _t("PLANOVANAVYZVA_EXTERNYZDROJ"): "External web links referenced by a planned call.",
    _t("PLANOVANAVYZVA_KATEGORIAREGIONOV"): "Region categories a planned call applies to.",
    _t("PLANOVANAVYZVA_MIESTOREALIZACIE"): "Eligible places of implementation for a planned call.",
    _t("PLANOVANAVYZVA_OPATRENIE"): "Measures a planned call is planned to be issued under.",
    _t("PLANOVANAVYZVA_PROJEKTOVYZAMERIUS"): "Project intentions ('projektovy zamer IUS') linked to a planned call.",
    _t("PLANOVANAVYZVA_SPECIFICKYCIELPROGRAMU"): "Specific objectives a planned call is planned to contribute to.",
    _t("PLANOVANAVYZVA_TYPAKCIEPROGRAMU"): "Programme action types expected to be eligible under a planned call.",
    _t("PLANOVANAVYZVA_ZIADATEL"): "Eligible applicant types/categories for a planned call.",
    _t("PLANOVANAVYZVA_VYZVA"): "Actual call(s) for proposals ('vyzva') opened from this planned call, once published.",
}
for _child_table in CHILD_TABLES:
    TABLE_COMMENTS[_t(_child_table)] += (
        " Child rows carrying planovanavyzva_id back to itms21_planovanavyzva; "
        "inserted once when the parent detail is first fetched, never updated/deleted."
    )

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_PLANOVANAVYZVA: {
        "id": "ITMS21 numeric id of the planned call.",
        "href": "API URL of this planned call's own detail resource.",
        "kod": "Planned call code.",
        "nazovsk": "Planned call name in Slovak.",
        "nazoven": "Planned call name in English.",
        "nazovde": "Planned call name in German.",
        "programskratka": "Abbreviation of the parent programme.",
        "programoveobdobie": "Programming period the planned call belongs to (e.g. '2021-2027').",
        "mrk": "Marginalized Roma community ('marginalizovana romska komunita', MRK) indicator/code for the planned call, when it targets this focus area.",
        "schema": "Whether the call operates under a (state aid) scheme.",
        "dvekola": "Whether the call is planned to run in two rounds ('dve kola').",
        "typ": "Type of the planned call.",
        "typ1kolo": "Type of the first round, when the call has two rounds.",
        "typ2kolo": "Type of the second round, when the call has two rounds.",
        "druh": "Kind/category of the planned call.",
        "zameranieprojektu": "Project focus/orientation targeted by the call.",
        "vyhlasena": "Whether the call has already been formally announced/opened (linked via itms21_planovanavyzva_vyzva).",
        "zrusena": "Whether the planned call has been cancelled.",
        "obsahujemiestorealizaciezahranicie": "Whether the call allows a place of implementation abroad.",
        "sumaeu": "Planned EU-fund allocation for the call, in euro.",
        "sumasr": "Planned Slovak national co-financing allocation for the call, in euro.",
        "datumvyhlasenia1kolo": "Planned announcement date of the first round (epoch milliseconds).",
        "datumvyhlasenia2kolo": "Planned announcement date of the second round (epoch milliseconds).",
        "datumuzavretia1kolo": "Planned closing date of the first round (epoch milliseconds).",
        "datumuzavretia2kolo": "Planned closing date of the second round (epoch milliseconds).",
        "trvanie1kolo": "Planned duration of the first round, in days.",
        "trvanie2kolo": "Planned duration of the second round, in days.",
        "poskytovatel_id": "Id of the provider/managing body responsible for the call.",
        "program_id": "Id of the parent programme this planned call belongs to.",
        "vyhlasovatel_id": "Id of the entity expected to announce the call.",
        "createdat": "Record creation timestamp in the source system.",
        "updatedat": "Record last-updated timestamp in the source system.",
    },
    _t("PLANOVANAVYZVA_DOKUMENT"): {
        "planovanavyzva_id": "Id of the parent planned call (itms21_planovanavyzva.id).",
        "nazov": "Document name.",
        "uuid": "Document's file identifier (uuid), used to build its download URL.",
    },
    _t("PLANOVANAVYZVA_EXTERNYZDROJ"): {
        "planovanavyzva_id": "Id of the parent planned call (itms21_planovanavyzva.id).",
        "nazov": "External source name.",
        "url": "External source URL.",
    },
    _t("PLANOVANAVYZVA_KATEGORIAREGIONOV"): {
        "planovanavyzva_id": "Id of the parent planned call (itms21_planovanavyzva.id).",
        "id": "Id of the region category this planned call applies to.",
    },
    _t("PLANOVANAVYZVA_MIESTOREALIZACIE"): {
        "planovanavyzva_id": "Id of the parent planned call (itms21_planovanavyzva.id).",
        "id": "Id of the eligible place of implementation.",
    },
    _t("PLANOVANAVYZVA_OPATRENIE"): {
        "planovanavyzva_id": "Id of the parent planned call (itms21_planovanavyzva.id).",
        "id": "Id of the measure this planned call is planned to be issued under (itms21_program_opatrenie.id).",
    },
    _t("PLANOVANAVYZVA_PROJEKTOVYZAMERIUS"): {
        "planovanavyzva_id": "Id of the parent planned call (itms21_planovanavyzva.id).",
        "id": "Id of the linked project intention ('projektovy zamer IUS').",
    },
    _t("PLANOVANAVYZVA_SPECIFICKYCIELPROGRAMU"): {
        "planovanavyzva_id": "Id of the parent planned call (itms21_planovanavyzva.id).",
        "id": "Id of the specific objective this planned call is planned to contribute to (itms21_program_specifickycielprogramu.id).",
    },
    _t("PLANOVANAVYZVA_TYPAKCIEPROGRAMU"): {
        "planovanavyzva_id": "Id of the parent planned call (itms21_planovanavyzva.id).",
        "id": "Id of the eligible programme action type (itms21_program_typakcieprogramu.id).",
    },
    _t("PLANOVANAVYZVA_ZIADATEL"): {
        "planovanavyzva_id": "Id of the parent planned call (itms21_planovanavyzva.id).",
        "id": "Id of the eligible applicant type/category.",
    },
    _t("PLANOVANAVYZVA_VYZVA"): {
        "planovanavyzva_id": "Id of the parent planned call (itms21_planovanavyzva.id).",
        "id": "Id of the actual call for proposals opened from this planned call (itms21_vyzva.id).",
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


def migrate_table_prefix(con: duckdb.DuckDBPyConnection) -> None:
    """One-time rename of pre-"itms21_"-prefix tables (and their ALL-CAPS
    columns) from earlier runs; every step is idempotent/case-insensitive,
    so this is safe to run on every invocation."""
    bare_names = ["PLANOVANAVYZVA"] + list(CHILD_TABLES.keys())
    for name in bare_names:
        con.execute(f"ALTER TABLE IF EXISTS {name} RENAME TO {_t(name)}")
        # Also catch a table already prefixed but with the old ALL-CAPS name
        # (e.g. from a run of this script before lowercasing was added).
        con.execute(f"ALTER TABLE IF EXISTS {TABLE_PREFIX}{name} RENAME TO {_t(name)}")

    if _table_exists(con, TABLE_PLANOVANAVYZVA):
        _rename_columns(con, TABLE_PLANOVANAVYZVA, [col for col, _, _ in PLANOVANAVYZVA_COLUMNS])

    for table, (_, columns) in CHILD_TABLES.items():
        full = _t(table)
        if _table_exists(con, full):
            con.execute(f"ALTER TABLE {full} RENAME COLUMN PLANOVANAVYZVA_ID TO planovanavyzva_id")
            _rename_columns(con, full, [col for col, _, _ in columns])


def ensure_full_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create itms21_planovanavyzva and every itms21_planovanavyzva_* child table if missing."""
    migrate_table_prefix(con)

    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in PLANOVANAVYZVA_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_PLANOVANAVYZVA} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                planovanavyzva_id BIGINT,
                {cols_sql}
            )
        """)


def store_full_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Decompose one planovanavyzva's full detail JSON into ITMS21_PLANOVANAVYZVA
    + all child tables. Only ever called once per planovanavyzva id (gated by
    get_known_ids in sync_planovanavyzvy), so this is a plain INSERT - never
    re-fetched, never updated."""
    planovanavyzva_id = detail.get("id")

    columns_sql = ", ".join(col for col, _, _ in PLANOVANAVYZVA_COLUMNS)
    placeholders = ", ".join("?" for _ in PLANOVANAVYZVA_COLUMNS)
    values = [_get(detail, path) for _, path, _ in PLANOVANAVYZVA_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_PLANOVANAVYZVA} ({columns_sql}) VALUES ({placeholders}) "
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
            [planovanavyzva_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (planovanavyzva_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_PLANOVANAVYZVA}").fetchall()
    return {row[0] for row in rows}


def sync_planovanavyzvy() -> tuple[int, int, int]:
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

    print("Fetching planovanavyzva list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} planovanavyzvy.")

    known_ids = get_known_ids(con)

    to_fetch = [item.get("id") for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_fetch)} new planovanavyzva(s) to fetch; "
          f"{len(list_items) - len(to_fetch)} already stored (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, pvid): pvid for pvid in to_fetch}
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
    total, fetched, failed = sync_planovanavyzvy()
    print(f"Done. {total} total in list, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
