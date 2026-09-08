"""
Fetches the ITMS21 "verejneobstaravanie" (public procurement procedure) list,
then fetches full detail ONLY for ids not yet stored in DuckDB. The list
itself is not persisted - it exists purely to discover ids. Ids already
stored are never re-fetched and never deleted, even if they disappear from
the API list (purely additive incremental sync, same approach as
fetch_uctovnydoklad.py / fetch_zonfp.py / fetch_vyzvy.py).

Note the table name mismatch with the API/URL: the DWH spec for this
endpoint names the parent table itms21_verejneobstaravanie_detail (not
itms21_verejneobstaravanie), so that's what's used here, while the
module/API URL still say "verejneobstaravanie".

Each detail fetched decomposes into a normalized itms21_verejneobstaravanie_detail
table plus itms21_verejneobstaravanie_detail_doplnujucepredmetydoplnkovyslovnik,
_doplnujucepredmetyhlavnyslovnik, _hlavnypredmetdoplnkovyslovnik, _naslednicivo,
_programy, _projekty and _uctovnedoklady child tables. Every child table
carries verejneobstaravanie_detail_id so it can be joined back to the parent.
Rows are inserted once and never updated/deleted, gated by the same "id not
yet known" check.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/verejneobstaravanie?limit=-1"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/verejneobstaravanie/id/{id}"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_VEREJNEOBSTARAVANIE = f"{TABLE_PREFIX}verejneobstaravanie_detail"

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

LIST_REQUEST_TIMEOUT = 180  # the list endpoint returns every verejneobstaravanie in one response (limit=-1)


def fetch_list() -> list[dict]:
    """Call the list endpoint and return all verejneobstaravanie summary
    records (used only to discover ids - the list itself is not stored),
    with retry on failure."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(LIST_URL, timeout=LIST_REQUEST_TIMEOUT)
            resp.raise_for_status()
            payload = resp.json()
            return payload["result"]
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                raise
            print(f"  List fetch failed (attempt {attempt}/{RETRY_ATTEMPTS}): {e}")
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)


def fetch_detail(vo_id: int) -> dict | None:
    """Call the detail endpoint for one verejneobstaravanie, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=vo_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={vo_id} after {RETRY_ATTEMPTS} attempts: {e}")
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
    """Build the itms21_verejneobstaravanie_detail_<bare_name> child table name."""
    return f"{TABLE_PREFIX}verejneobstaravanie_detail_{bare_name}"


# --- Full normalized schema -------------------------------------------------
# Mirrors the ITMS21 "verejneobstaravanie (detail)" field-by-field mapping.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).

VEREJNEOBSTARAVANIE_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("nazov", "nazov", "VARCHAR"),
    ("stav", "stav", "VARCHAR"),
    ("centralneobstaravanie", "centralneObstaravanie", "BOOLEAN"),
    ("cislovestnika", "cisloVestnika", "VARCHAR"),
    ("cislozverejneniavovestniku", "cisloZverejneniaVoVestniku", "VARCHAR"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
    ("datumzverejnenia", "datumZverejnenia", "VARCHAR"),
    ("dodavatelobstaravatel_id", "dodavatelObstaravatel.id", "BIGINT"),
    ("druhzakazky_id", "druhZakazky.id", "BIGINT"),
    ("hlavnypredmethlavnyslovnik_id", "hlavnyPredmetHlavnySlovnik.id", "BIGINT"),
    ("kriteriumnavyhodnotenieponuk", "kriteriumNaVyhodnoteniePonuk", "VARCHAR"),
    ("lehotanapredkladanieponuk", "lehotaNaPredkladaniePonuk", "VARCHAR"),
    ("lehotanapredkladanieziadostioucast", "lehotaNaPredkladanieZiadostiOUcast", "VARCHAR"),
    ("metodavo_id", "metodaVO.id", "BIGINT"),
    ("obmedzeniepoctuuchadzacov", "obmedzeniePoctuUchadzacov", "BOOLEAN"),
    ("obstaravatelsubjekt_id", "obstaravatelSubjekt.id", "BIGINT"),
    ("pocetprijatychponuk", "pocetPrijatychPonuk", "INTEGER"),
    ("pocetvylucenychponuk", "pocetVylucenychPonuk", "INTEGER"),
    ("postupobstaravania_id", "postupObstaravania.id", "BIGINT"),
    ("predchadzajuceoznamenie", "predchadzajuceOznamenie", "BOOLEAN"),
    ("predpokladanahodnotazakazky", "predpokladanaHodnotaZakazky", "DOUBLE"),
    ("trvaniezakazkyhodnota", "trvanieZakazkyHodnota", "DOUBLE"),
    ("trvaniezakazkymernajednotka", "trvanieZakazkyMernaJednotka", "VARCHAR"),
    ("urlodkazoznamenie", "urlOdkazOznamenie", "VARCHAR"),
    ("zadavaniezakazkyvmeneinychobstaravatelov", "zadavanieZakazkyVMeneInychObstaravatelov", "BOOLEAN"),
    ("zadavatel_id", "zadavatel.id", "BIGINT"),
    ("zakazkarozdelenanaviaccasti", "zakazkaRozdelenaNaViacCasti", "BOOLEAN"),
    ("zverejnenevovestnikueu", "zverejneneVoVestnikuEU", "BOOLEAN"),
]

# bare_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "doplnujucepredmetydoplnkovyslovnik": ("doplnujucePredmetyDoplnkovySlovnik", [("id", "id", "BIGINT")]),
    "doplnujucepredmetyhlavnyslovnik": ("doplnujucePredmetyHlavnySlovnik", [("id", "id", "BIGINT")]),
    "hlavnypredmetdoplnkovyslovnik": ("hlavnyPredmetDoplnkovySlovnik", [("id", "id", "BIGINT")]),
    "naslednicivo": ("nasledniciVO", [
        ("id", "id", "BIGINT"),
        ("subjekt_id", "subjekt.id", "BIGINT"),
    ]),
    "programy": ("programy", [("id", "id", "BIGINT")]),
    "projekty": ("projekty", [("id", "id", "BIGINT")]),
    "uctovnedoklady": ("uctovneDoklady", [("id", "id", "BIGINT")]),
}


TABLE_COMMENTS: dict[str, str] = {
    TABLE_VEREJNEOBSTARAVANIE: (
        "One row per public procurement procedure ('verejne obstaravanie'). "
        "Purely additive: once an id is stored it is never re-fetched, "
        "updated, or deleted, even if it disappears from a later API list "
        "response."
    ),
    _t("doplnujucepredmetydoplnkovyslovnik"): (
        "Additional subject-matter codes of the procurement, from the CPV supplementary "
        "vocabulary. Child rows carrying verejneobstaravanie_detail_id back to "
        "itms21_verejneobstaravanie_detail; inserted once when the parent detail is first "
        "fetched, never updated/deleted."
    ),
    _t("doplnujucepredmetyhlavnyslovnik"): (
        "Additional subject-matter codes of the procurement, from the CPV main vocabulary. "
        "Child rows carrying verejneobstaravanie_detail_id back to "
        "itms21_verejneobstaravanie_detail; inserted once when the parent detail is first "
        "fetched, never updated/deleted."
    ),
    _t("hlavnypredmetdoplnkovyslovnik"): (
        "Main subject-matter code(s) of the procurement, from the CPV supplementary "
        "vocabulary. Child rows carrying verejneobstaravanie_detail_id back to "
        "itms21_verejneobstaravanie_detail; inserted once when the parent detail is first "
        "fetched, never updated/deleted."
    ),
    _t("naslednicivo"): (
        "Successor procurement procedures linked to this one (e.g. a follow-on procedure). "
        "Child rows carrying verejneobstaravanie_detail_id back to "
        "itms21_verejneobstaravanie_detail; inserted once when the parent detail is first "
        "fetched, never updated/deleted."
    ),
    _t("programy"): (
        "Programmes this procurement procedure is linked to. Child rows carrying "
        "verejneobstaravanie_detail_id back to itms21_verejneobstaravanie_detail; inserted "
        "once when the parent detail is first fetched, never updated/deleted."
    ),
    _t("projekty"): (
        "Projects this procurement procedure is linked to. Child rows carrying "
        "verejneobstaravanie_detail_id back to itms21_verejneobstaravanie_detail; inserted "
        "once when the parent detail is first fetched, never updated/deleted."
    ),
    _t("uctovnedoklady"): (
        "Accounting documents linked to this procurement procedure. Child rows carrying "
        "verejneobstaravanie_detail_id back to itms21_verejneobstaravanie_detail; inserted "
        "once when the parent detail is first fetched, never updated/deleted."
    ),
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_VEREJNEOBSTARAVANIE: {
        "id": "ITMS21 numeric id of the procurement procedure.",
        "href": "API URL of this procurement procedure's own detail resource.",
        "kod": "Procurement procedure code.",
        "nazov": "Name of the procurement procedure.",
        "stav": "Current status of the procurement procedure.",
        "centralneobstaravanie": "Whether this is a centralized procurement procedure.",
        "cislovestnika": "Number of the public procurement bulletin (vestnik) issue.",
        "cislozverejneniavovestniku": "Publication number within the bulletin.",
        "createdat": "Record creation timestamp in the source system.",
        "updatedat": "Record last-updated timestamp in the source system.",
        "datumzverejnenia": "Date the procurement notice was published.",
        "dodavatelobstaravatel_id": "Id of the supplier as a contracting-authority-side party (dodávateľ/obstarávateľ).",
        "druhzakazky_id": "Id of the contract type (goods/works/services).",
        "hlavnypredmethlavnyslovnik_id": "Id of the main subject-matter code (CPV main vocabulary).",
        "kriteriumnavyhodnotenieponuk": "Criterion used to evaluate bids.",
        "lehotanapredkladanieponuk": "Deadline for submitting bids.",
        "lehotanapredkladanieziadostioucast": "Deadline for submitting requests to participate.",
        "metodavo_id": "Id of the procurement method used.",
        "obmedzeniepoctuuchadzacov": "Whether the number of candidates is limited.",
        "obstaravatelsubjekt_id": "Id of the contracting-authority entity (subjekt).",
        "pocetprijatychponuk": "Number of bids received.",
        "pocetvylucenychponuk": "Number of bids excluded.",
        "postupobstaravania_id": "Id of the procurement procedure type used.",
        "predchadzajuceoznamenie": "Whether a prior information notice was published.",
        "predpokladanahodnotazakazky": "Estimated value of the contract, in euro.",
        "trvaniezakazkyhodnota": "Duration of the contract, in units given by trvaniezakazkymernajednotka.",
        "trvaniezakazkymernajednotka": "Unit of measure for the contract duration (e.g. months, days).",
        "urlodkazoznamenie": "URL link to the published procurement notice.",
        "zadavaniezakazkyvmeneinychobstaravatelov": "Whether the contract is awarded on behalf of other contracting authorities.",
        "zadavatel_id": "Id of the entity awarding the contract.",
        "zakazkarozdelenanaviaccasti": "Whether the contract is divided into multiple lots.",
        "zverejnenevovestnikueu": "Whether the notice was also published in the EU Official Journal.",
    },
    _t("doplnujucepredmetydoplnkovyslovnik"): {
        "verejneobstaravanie_detail_id": "Id of the parent procurement procedure (itms21_verejneobstaravanie_detail.id).",
        "id": "Id of the additional subject-matter code (CPV supplementary vocabulary).",
    },
    _t("doplnujucepredmetyhlavnyslovnik"): {
        "verejneobstaravanie_detail_id": "Id of the parent procurement procedure (itms21_verejneobstaravanie_detail.id).",
        "id": "Id of the additional subject-matter code (CPV main vocabulary).",
    },
    _t("hlavnypredmetdoplnkovyslovnik"): {
        "verejneobstaravanie_detail_id": "Id of the parent procurement procedure (itms21_verejneobstaravanie_detail.id).",
        "id": "Id of the main subject-matter code (CPV supplementary vocabulary).",
    },
    _t("naslednicivo"): {
        "verejneobstaravanie_detail_id": "Id of the parent procurement procedure (itms21_verejneobstaravanie_detail.id).",
        "id": "Id of the successor procurement procedure.",
        "subjekt_id": "Id of the entity (subjekt) associated with the successor procedure.",
    },
    _t("programy"): {
        "verejneobstaravanie_detail_id": "Id of the parent procurement procedure (itms21_verejneobstaravanie_detail.id).",
        "id": "Id of the linked programme.",
    },
    _t("projekty"): {
        "verejneobstaravanie_detail_id": "Id of the parent procurement procedure (itms21_verejneobstaravanie_detail.id).",
        "id": "Id of the linked project (itms21_projekt.id).",
    },
    _t("uctovnedoklady"): {
        "verejneobstaravanie_detail_id": "Id of the parent procurement procedure (itms21_verejneobstaravanie_detail.id).",
        "id": "Id of the linked accounting document (itms21_doklaductovny.id).",
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
    """Create itms21_verejneobstaravanie_detail and its child tables if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in VEREJNEOBSTARAVANIE_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_VEREJNEOBSTARAVANIE} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                verejneobstaravanie_detail_id BIGINT,
                {cols_sql}
            )
        """)


def store_full_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Decompose one verejneobstaravanie's full detail JSON into
    itms21_verejneobstaravanie_detail + all child tables. Only ever called
    once per id (gated by get_known_ids in sync_verejneobstaravanie), so this
    is a plain INSERT - never re-fetched, never updated."""
    vo_id = detail.get("id")

    columns_sql = ", ".join(col for col, _, _ in VEREJNEOBSTARAVANIE_COLUMNS)
    placeholders = ", ".join("?" for _ in VEREJNEOBSTARAVANIE_COLUMNS)
    values = [_get(detail, path) for _, path, _ in VEREJNEOBSTARAVANIE_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_VEREJNEOBSTARAVANIE} ({columns_sql}) VALUES ({placeholders}) "
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
            [vo_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (verejneobstaravanie_detail_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_VEREJNEOBSTARAVANIE}").fetchall()
    return {row[0] for row in rows}


def sync_verejneobstaravanie() -> tuple[int, int, int]:
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

    print("Fetching verejneobstaravanie list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} verejneobstaravanie.")

    known_ids = get_known_ids(con)

    to_fetch = [item.get("id") for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_fetch)} new verejneobstaravanie(s) to fetch; "
          f"{len(list_items) - len(to_fetch)} already stored (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, vid): vid for vid in to_fetch}
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
    total, fetched, failed = sync_verejneobstaravanie()
    print(f"Done. {total} total in list, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
