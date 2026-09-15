"""
Fetches the ITMS21 "subjekt" (legal entity / beneficiary) list, then fetches
full detail ONLY for ids not yet stored in DuckDB. The list itself is not
persisted - it exists purely to discover ids. Ids already stored are never
re-fetched and never deleted, even if they disappear from a later API list
(purely additive incremental sync, same approach as fetch_zop.py /
fetch_zonfp.py / fetch_vyzvy.py / fetch_ukazovatelprojektovy.py).

Each detail fetched decomposes into a normalized itms21_subjekt table plus
itms21_subjekt_naslednik and itms21_subjekt_predchodca child tables, both
carrying subjekt_id back to the parent. These record this entity's legal
succession history (e.g. mergers, splits, renamings): a "naslednik" row means
this subjekt is the predecessor (predchodca) side of that transition, a
"predchodca" row means this subjekt is the successor (naslednik) side - the
JSON fields are identical in both, only which list they came from differs.
Rows are inserted once and never updated/deleted, gated by the same "id not
yet known" check as the parent.

The list endpoint supports limit=-1 directly (confirmed with
https://api.itms21.sk/public/v1/subjekt?limit=-1, which returns the full
result set - about 12,900 rows - in one response), so this uses that
directly rather than the "discover size via a bare call, then request
limit={size}" two-step dance that priorita/opatrenie/typakcieprogramu need
because they don't support limit=-1.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/subjekt?limit=-1"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/subjekt/id/{id}"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_SUBJEKT = f"{TABLE_PREFIX}subjekt"

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

LIST_REQUEST_TIMEOUT = 180  # the list endpoint returns every subjekt in one response (limit=-1)


def fetch_list() -> list[dict]:
    """Call the list endpoint and return all subjekt summary records (used
    only to discover ids - the list itself is not stored), with retry on
    failure."""
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


def fetch_detail(subjekt_id: int) -> dict | None:
    """Call the detail endpoint for one subjekt, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=subjekt_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={subjekt_id} after {RETRY_ATTEMPTS} attempts: {e}")
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
    """Build the itms21_subjekt_<bare_name> child table name."""
    return f"{TABLE_PREFIX}subjekt_{bare_name}"


# --- Full normalized schema -------------------------------------------------
# Mirrors the ITMS21 "subjekt" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).

SUBJEKT_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("nazov", "nazov", "VARCHAR"),
    ("ico", "ico", "VARCHAR"),
    ("dic", "dic", "VARCHAR"),
    ("icz", "icz", "VARCHAR"),
    ("icdph", "icDph", "VARCHAR"),
    ("ineidentifikacnecislo", "ineIdentifikacneCislo", "VARCHAR"),
    ("platiteldph", "platitelDph", "BOOLEAN"),
    ("podlaparagrafu", "podlaParagrafu", "VARCHAR"),
    ("pravnaforma_id", "pravnaForma.id", "BIGINT"),
    ("typinehoidentifikatora_id", "typInehoIdentifikatora.id", "BIGINT"),
    ("adresa_ulica", "adresa.ulica", "VARCHAR"),
    ("adresa_cislo", "adresa.cislo", "VARCHAR"),
    ("adresa_psc", "adresa.psc", "VARCHAR"),
    ("adresa_obec", "adresa.obec", "VARCHAR"),
    ("adresa_stat_id", "adresa.stat.id", "BIGINT"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
]

# bare_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
# "naslednik" and "predchodca" items share the identical shape - both describe
# one legal-succession transition, just from opposite ends.
_SUCCESSION_COLUMNS: list[tuple[str, str, str]] = [
    ("naslednik_id", "naslednik.id", "BIGINT"),
    ("predchodca_id", "predchodca.id", "BIGINT"),
    ("platnostnaslednikaod", "platnostNaslednikaOd", "BIGINT"),
    ("platnostpredchodcudo", "platnostPredchodcuDo", "BIGINT"),
    ("typnaslednika", "typNaslednika", "VARCHAR"),
]

CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "naslednik": ("naslednik", _SUCCESSION_COLUMNS),
    "predchodca": ("predchodca", _SUCCESSION_COLUMNS),
}


TABLE_COMMENTS: dict[str, str] = {
    TABLE_SUBJEKT: (
        "One row per legal entity ('subjekt') known to ITMS21 - programme "
        "bodies, beneficiaries, partners, etc. Purely additive: once an id "
        "is stored it is never re-fetched, updated, or deleted, even if it "
        "disappears from a later API list response."
    ),
    _t("naslednik"): (
        "Legal-succession transitions (e.g. merger, split, renaming) in "
        "which this subjekt is the predecessor ('predchodca') side. Child "
        "rows carrying subjekt_id back to itms21_subjekt; inserted once when "
        "the parent detail is first fetched, never updated/deleted."
    ),
    _t("predchodca"): (
        "Legal-succession transitions (e.g. merger, split, renaming) in "
        "which this subjekt is the successor ('naslednik') side. Same shape "
        "as itms21_subjekt_naslednik; child rows carrying subjekt_id back to "
        "itms21_subjekt, inserted once when the parent detail is first "
        "fetched, never updated/deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_SUBJEKT: {
        "id": "ITMS21 numeric id of the legal entity.",
        "href": "API URL of this entity's own detail resource.",
        "nazov": "Entity name.",
        "ico": "Business identification number (IČO).",
        "dic": "Tax identification number (DIČ).",
        "icz": "Healthcare provider identification number (IČZ), when applicable.",
        "icdph": "VAT identification number (IČ DPH), when the entity is VAT-registered.",
        "ineidentifikacnecislo": "Other identification number, used when none of ICO/DIC/ICZ/ICDPH applies.",
        "platiteldph": "Whether the entity is a registered VAT payer.",
        "podlaparagrafu": "Paragraph/section of the VAT act under which the entity is registered, when applicable.",
        "pravnaforma_id": "Id of the entity's legal form (e.g. joint-stock company, budgetary organization).",
        "typinehoidentifikatora_id": "Id of the type of the 'other identification number', when ineidentifikacnecislo is used.",
        "adresa_ulica": "Street of the entity's registered address.",
        "adresa_cislo": "Street/building number of the entity's registered address.",
        "adresa_psc": "Postal code of the entity's registered address.",
        "adresa_obec": "Municipality of the entity's registered address.",
        "adresa_stat_id": "Id of the country of the entity's registered address.",
        "createdat": "Record creation timestamp in the source system.",
        "updatedat": "Record last-updated timestamp in the source system.",
    },
    _t("naslednik"): {
        "subjekt_id": "Id of the subjekt this row was reported under (itms21_subjekt.id); here this subjekt is the predecessor side of the transition.",
        "naslednik_id": "Id of the successor entity (itms21_subjekt.id) in this legal transition.",
        "predchodca_id": "Id of the predecessor entity (itms21_subjekt.id) in this legal transition; normally equal to subjekt_id.",
        "platnostnaslednikaod": "Start of the successor's validity in this transition (epoch milliseconds).",
        "platnostpredchodcudo": "End of the predecessor's validity in this transition (epoch milliseconds).",
        "typnaslednika": "Type of the legal succession (e.g. merger, split, renaming).",
    },
    _t("predchodca"): {
        "subjekt_id": "Id of the subjekt this row was reported under (itms21_subjekt.id); here this subjekt is the successor side of the transition.",
        "naslednik_id": "Id of the successor entity (itms21_subjekt.id) in this legal transition; normally equal to subjekt_id.",
        "predchodca_id": "Id of the predecessor entity (itms21_subjekt.id) in this legal transition.",
        "platnostnaslednikaod": "Start of the successor's validity in this transition (epoch milliseconds).",
        "platnostpredchodcudo": "End of the predecessor's validity in this transition (epoch milliseconds).",
        "typnaslednika": "Type of the legal succession (e.g. merger, split, renaming).",
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
    """Create itms21_subjekt and its child tables if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in SUBJEKT_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_SUBJEKT} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                subjekt_id BIGINT,
                {cols_sql}
            )
        """)


def store_full_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Decompose one subjekt's full detail JSON into itms21_subjekt + all
    child tables. Only ever called once per id (gated by get_known_ids in
    sync_subjekt), so this is a plain INSERT - never re-fetched, never
    updated."""
    subjekt_id = detail.get("id")

    columns_sql = ", ".join(col for col, _, _ in SUBJEKT_COLUMNS)
    placeholders = ", ".join("?" for _ in SUBJEKT_COLUMNS)
    values = [_get(detail, path) for _, path, _ in SUBJEKT_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_SUBJEKT} ({columns_sql}) VALUES ({placeholders}) "
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
            [subjekt_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (subjekt_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_SUBJEKT}").fetchall()
    return {row[0] for row in rows}


def sync_subjekt() -> tuple[int, int, int]:
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

    print("Fetching subjekt list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} subjekt.")

    known_ids = get_known_ids(con)

    to_fetch = [item.get("id") for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_fetch)} new subjekt to fetch; "
          f"{len(list_items) - len(to_fetch)} already stored (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, sid): sid for sid in to_fetch}
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
    total, fetched, failed = sync_subjekt()
    print(f"Done. {total} total in list, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
