"""
Fetches the ITMS21 "zop" (žiadosť o platbu / payment request) list, then
fetches full detail ONLY for ids not yet stored in DuckDB. The list itself is
not persisted - it exists purely to discover ids. Ids already stored are
never re-fetched and never deleted, even if they disappear from the API list
(purely additive incremental sync, same approach as fetch_vyzvy.py /
fetch_zonfp.py). This endpoint is large (~23000 records as of writing), so
the incremental skip is what keeps subsequent runs fast.

Each detail fetched decomposes into a normalized itms21_zop table plus
itms21_zop_predkladanazasubjekty and itms21_zop_vydavky child tables, and
three itms21_zop_vydavky_* grandchild tables (one level deeper, nested inside
each vydavky item). Every child/grandchild table carries zop_id (and
grandchildren also carry vydavky_id) so they can be joined back to
itms21_zop. Rows are inserted once and never updated/deleted, gated by the
same "id not yet known" check.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/zop?limit=-1"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/zop/id/{id}"

DB_PATH = Path("data/eurofondy.duckdb")  # shared DuckDB file, separate tables inside

TABLE_PREFIX = "itms21_"
TABLE_ZOP = f"{TABLE_PREFIX}zop"

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

LIST_REQUEST_TIMEOUT = 180  # the list endpoint returns every zop in one response (limit=-1)


def fetch_list() -> list[dict]:
    """Call the list endpoint and return all zop summary records (used only
    to discover ids - the list itself is not stored), with retry on failure."""
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


def fetch_detail(zop_id: int) -> dict | None:
    """Call the detail endpoint for one zop, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=zop_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={zop_id} after {RETRY_ATTEMPTS} attempts: {e}")
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
    """Build the itms21_zop_<bare_name> child table name."""
    return f"{TABLE_PREFIX}zop_{bare_name}"


# --- Full normalized schema -------------------------------------------------
# Mirrors the ITMS21 "zop" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).

ZOP_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("typ", "typ", "VARCHAR"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
    ("datumprijatia", "datumPrijatia", "VARCHAR"),
    ("datumuhrady", "datumUhrady", "VARCHAR"),
    ("hlavnycezhranicnypartner_id", "hlavnyCezhranicnyPartner.id", "BIGINT"),
    ("narokovanasuma", "narokovanaSuma", "DOUBLE"),
    ("neuhradena", "neuhradena", "BOOLEAN"),
    ("predfinancovanie_id", "predfinancovanie.id", "BIGINT"),
    ("predkladanaza_id", "predkladanaZa.id", "BIGINT"),
    ("prijimatel_id", "prijimatel.id", "BIGINT"),
    ("projekt_id", "projekt.id", "BIGINT"),
    ("vyplacasapartnerovi", "vyplacaSaPartnerovi", "BOOLEAN"),
    ("zopjezaverecna", "zopJeZaverecna", "BOOLEAN"),
    ("zoppredlozenazaviacsubjektov", "zopPredlozenaZaViacSubjektov", "BOOLEAN"),
]

# bare_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "predkladanazasubjekty": ("predkladanaZaSubjekty", [
        ("platisapriamosubjektu", "platiSaPriamoSubjektu", "BOOLEAN"),
        ("subjekt_id", "subjekt.id", "BIGINT"),
        ("typsubjektunaprojekte", "typSubjektuNaProjekte", "VARCHAR"),
    ]),
    "vydavky": ("vydavky", [
        ("id", "id", "BIGINT"),
        ("datumuhrady", "datumUhrady", "BIGINT"),
        ("dokladpolozka_id", "dokladPolozka.id", "VARCHAR"),
        ("dph", "dph", "VARCHAR"),
        ("druhvydavku", "druhVydavku", "VARCHAR"),
        ("ekonomickaklasifikacia", "ekonomickaKlasifikacia", "VARCHAR"),
        ("funkcnaklasifikacia", "funkcnaKlasifikacia", "VARCHAR"),
        ("investicnaakciaprijimatela", "investicnaAkciaPrijimatela", "VARCHAR"),
        ("klasifikaciajednorazovehotitulu", "klasifikaciaJednorazovehoTitulu", "VARCHAR"),
        ("nazov", "nazov", "VARCHAR"),
        ("polozkarozpoctu_id", "polozkaRozpoctu.id", "BIGINT"),
        ("poradovecislo", "poradoveCislo", "INTEGER"),
        ("sumaziadananapreplatenie", "sumaZiadanaNaPreplatenie", "DOUBLE"),
        ("uctovnydoklad_id", "uctovnyDoklad.id", "BIGINT"),
        ("vyskabezdph", "vyskaBezDph", "VARCHAR"),
    ]),
}


def ensure_full_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create itms21_zop, its child tables, and the itms21_zop_vydavky_*
    grandchild tables nested inside each vydavky item, if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in ZOP_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_ZOP} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                zop_id BIGINT,
                {cols_sql}
            )
        """)

    # Three grandchild tables nested one level deeper than a "vydavky" row.
    # Each carries both zop_id and vydavky_id so it can be joined back to
    # either the top-level zop or the specific expense it belongs to.
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {_t("vydavky_sumaneziadananapreplatenie")} (
            zop_id BIGINT,
            vydavky_id BIGINT,
            druhneziadanejsumy_id BIGINT,
            druhneziadanejsumy_kod VARCHAR,
            druhneziadanejsumy_kodzdroj VARCHAR,
            druhneziadanejsumy_nazovsk VARCHAR,
            druhneziadanejsumy_popissk VARCHAR,
            sumaneziadana DOUBLE
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {_t("vydavky_verejneobstaravanie")} (
            zop_id BIGINT,
            vydavky_id BIGINT,
            id BIGINT
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {_t("vydavky_zmluvaverejneobstaravanie")} (
            zop_id BIGINT,
            vydavky_id BIGINT,
            id BIGINT
        )
    """)


def store_full_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Decompose one zop's full detail JSON into itms21_zop + all
    child/grandchild tables. Only ever called once per zop id (gated by
    get_known_ids in sync_zop), so this is a plain INSERT - never re-fetched,
    never updated."""
    zop_id = detail.get("id")

    columns_sql = ", ".join(col for col, _, _ in ZOP_COLUMNS)
    placeholders = ", ".join("?" for _ in ZOP_COLUMNS)
    values = [_get(detail, path) for _, path, _ in ZOP_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_ZOP} ({columns_sql}) VALUES ({placeholders}) "
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
            [zop_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (zop_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )

    sumaneziadana_rows = []
    verejneobstaravanie_rows = []
    zmluvaverejneobstaravanie_rows = []
    for vydavok in detail.get("vydavky") or []:
        vydavky_id = vydavok.get("id")
        for sn in vydavok.get("sumaNeziadanaNaPreplatenie") or []:
            sumaneziadana_rows.append([
                zop_id, vydavky_id,
                _get(sn, "druhNeziadanejSumy.id"), _get(sn, "druhNeziadanejSumy.kod"),
                _get(sn, "druhNeziadanejSumy.kodZdroj"), _get(sn, "druhNeziadanejSumy.nazovSk"),
                _get(sn, "druhNeziadanejSumy.popisSk"), sn.get("sumaNeziadana"),
            ])
        for vo in vydavok.get("verejneObstaravanie") or []:
            verejneobstaravanie_rows.append([zop_id, vydavky_id, vo.get("id")])
        for zvo in vydavok.get("zmluvaVerejneObstaravanie") or []:
            zmluvaverejneobstaravanie_rows.append([zop_id, vydavky_id, zvo.get("id")])

    if sumaneziadana_rows:
        con.executemany(
            f"INSERT INTO {_t('vydavky_sumaneziadananapreplatenie')} ("
            "zop_id, vydavky_id, druhneziadanejsumy_id, druhneziadanejsumy_kod, "
            "druhneziadanejsumy_kodzdroj, druhneziadanejsumy_nazovsk, druhneziadanejsumy_popissk, "
            "sumaneziadana) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            sumaneziadana_rows,
        )
    if verejneobstaravanie_rows:
        con.executemany(
            f"INSERT INTO {_t('vydavky_verejneobstaravanie')} (zop_id, vydavky_id, id) VALUES (?, ?, ?)",
            verejneobstaravanie_rows,
        )
    if zmluvaverejneobstaravanie_rows:
        con.executemany(
            f"INSERT INTO {_t('vydavky_zmluvaverejneobstaravanie')} (zop_id, vydavky_id, id) VALUES (?, ?, ?)",
            zmluvaverejneobstaravanie_rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_ZOP}").fetchall()
    return {row[0] for row in rows}


def sync_zop() -> tuple[int, int, int]:
    """Fetch the list (ids only, not stored), then fetch full details only for
    ids not already stored.

    Returns (total_in_list, fetched_count, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    ensure_full_schema(con)

    print("Fetching zop list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} zop.")

    known_ids = get_known_ids(con)

    to_fetch = [item.get("id") for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_fetch)} new zop(s) to fetch; {len(list_items) - len(to_fetch)} already stored (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, zid): zid for zid in to_fetch}
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
    total, fetched, failed = sync_zop()
    print(f"Done. {total} total in list, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
