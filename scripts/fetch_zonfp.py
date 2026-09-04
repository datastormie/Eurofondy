"""
Fetches the ITMS21 "zonfp" (grant application / žiadosť o NFP) list, then
fetches full detail ONLY for ids not yet stored in DuckDB. The list itself is
not persisted - it exists purely to discover ids. Ids already stored are
never re-fetched and never deleted, even if they disappear from the API list
(purely additive incremental sync, same approach as fetch_vyzvy.py /
fetch_projects.py). This endpoint is large (~9000 records as of writing), so
the incremental skip is what keeps subsequent runs fast.

Each detail fetched decomposes into a normalized itms21_zonfp table plus
~30 itms21_zonfp_* child tables (one row per inner list item), mirroring the
ITMS21 "zonfp (detail)" field-by-field mapping. Every child table carries
zonfp_id so it can be joined back to itms21_zonfp. Rows are inserted once
and never updated/deleted, gated by the same "id not yet known" check.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/zonfp?limit=-1"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/zonfp/id/{id}"

DB_PATH = Path("data/eurofondy.duckdb")  # shared DuckDB file, separate tables inside

TABLE_PREFIX = "itms21_"
TABLE_ZONFP = f"{TABLE_PREFIX}zonfp"

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

LIST_REQUEST_TIMEOUT = 180  # the list endpoint returns every zonfp in one response (limit=-1)


def fetch_list() -> list[dict]:
    """Call the list endpoint and return all zonfp summary records (used only
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


def fetch_detail(zonfp_id: int) -> dict | None:
    """Call the detail endpoint for one zonfp, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=zonfp_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={zonfp_id} after {RETRY_ATTEMPTS} attempts: {e}")
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
    """Build the itms21_zonfp_<bare_name> child table name."""
    return f"{TABLE_PREFIX}zonfp_{bare_name}"


# --- Full normalized schema -------------------------------------------------
# Mirrors the ITMS21 "zonfp" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).

ZONFP_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("nazov", "nazov", "VARCHAR"),
    ("akronym", "akronym", "VARCHAR"),
    ("mrk", "mrk", "VARCHAR"),
    ("zameranieprojektu", "zameranieProjektu", "VARCHAR"),
    ("popis", "popis", "VARCHAR"),
    ("ucel", "ucel", "VARCHAR"),
    ("stav", "stav", "VARCHAR"),
    ("datumvytvorenia", "datumVytvorenia", "VARCHAR"),
    ("datumpredlozenia", "datumPredlozenia", "VARCHAR"),
    ("datumregistracie", "datumRegistracie", "BIGINT"),
    ("datumcasodoslania", "datumCasOdoslania", "BIGINT"),
    ("datumodoslania", "datumOdoslania", "VARCHAR"),
    ("datumschvalenia", "datumSchvalenia", "BIGINT"),
    ("datumzamietnutia", "datumZamietnutia", "BIGINT"),
    ("datumziadanyzaciatkuhlavnychaktivit", "datumZiadanyZaciatkuHlavnychAktivit", "BIGINT"),
    ("datumziadanykoncahlavnychaktivit", "datumZiadanyKoncaHlavnychAktivit", "BIGINT"),
    ("datumschvalenyzaciatkuhlavnychaktivit", "datumSchvalenyZaciatkuHlavnychAktivit", "BIGINT"),
    ("datumschvalenykoncahlavnychaktivit", "datumSchvalenyKoncaHlavnychAktivit", "BIGINT"),
    ("dlzkaziadanacelkovaprojektu", "dlzkaZiadanaCelkovaProjektu", "INTEGER"),
    ("dlzkaziadanacelkovahlavnychaktivit", "dlzkaZiadanaCelkovaHlavnychAktivit", "INTEGER"),
    ("dlzkaschvalenacelkovaprojektu", "dlzkaSchvalenaCelkovaProjektu", "INTEGER"),
    ("dlzkaschvalenacelkovahlavnychaktivit", "dlzkaSchvalenaCelkovaHlavnychAktivit", "INTEGER"),
    ("percetnoziadane", "percetnoZiadane", "DOUBLE"),
    ("percetnoschvalene", "percetnoSchvalene", "DOUBLE"),
    ("pocetbodovhodnoteniacelkovy", "pocetBodovHodnoteniaCelkovy", "DOUBLE"),
    ("sumaziadananfp", "sumaZiadanaNFP", "DOUBLE"),
    ("sumaziadanavz", "sumaZiadanaVZ", "DOUBLE"),
    ("sumaziadanacelkova", "sumaZiadanaCelkova", "DOUBLE"),
    ("sumaschvalenanfp", "sumaSchvalenaNFP", "DOUBLE"),
    ("sumaschvalenavz", "sumaSchvalenaVZ", "DOUBLE"),
    ("sumaschvalenacelkova", "sumaSchvalenaCelkova", "DOUBLE"),
    ("schvalenavyskanfp", "schvalenaVyskaNfp", "DOUBLE"),
    ("schvalena", "schvalena", "BOOLEAN"),
    ("neschvalena", "neschvalena", "BOOLEAN"),
    ("vylucenazfinancovania", "vylucenaZFinancovania", "BOOLEAN"),
    ("osobitnyproceskonania", "osobitnyProcesKonania", "BOOLEAN"),
    ("obsahujemiestorealizaciezahranicie", "obsahujeMiestoRealizacieZahranicie", "BOOLEAN"),
    ("makategoriuregionov", "maKategoriuRegionov", "BOOLEAN"),
    ("udrzatelnyrozvojmiest", "udrzatelnyRozvojMiest", "BOOLEAN"),
    ("posudzovaneobdobie_datumuzavierky", "posudzovaneObdobie.datumUzavierky", "BIGINT"),
    ("posudzovaneobdobie_poradovecislo", "posudzovaneObdobie.poradoveCislo", "INTEGER"),
    ("program_id", "program.id", "BIGINT"),
    ("projekt_id", "projekt.id", "BIGINT"),
    ("vyzva_id", "vyzva.id", "BIGINT"),
    ("ziadatel_id", "ziadatel.id", "BIGINT"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
]

# bare_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "aktivity": ("aktivity", [("id", "id", "BIGINT")]),
    "aktivityschvalene": ("aktivitySchvalene", [("id", "id", "BIGINT")]),
    "cielovaskupina": ("cielovaSkupina", [("id", "id", "BIGINT")]),
    "dokumenthodnotenia": ("dokumentHodnotenia", [
        ("nazov", "nazov", "VARCHAR"),
        ("uuid", "uuid", "VARCHAR"),
    ]),
    "formapodpory": ("formaPodpory", [
        ("formapodpory_id", "formaPodpory.id", "BIGINT"),
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "hodnotitel": ("hodnotitel", [
        ("osoba_meno", "osoba.meno", "VARCHAR"),
        ("osoba_menouplne", "osoba.menoUplne", "VARCHAR"),
        ("osoba_priezvisko", "osoba.priezvisko", "VARCHAR"),
        ("osoba_titulpred", "osoba.titulPred", "VARCHAR"),
        ("osoba_titulza", "osoba.titulZa", "VARCHAR"),
        ("program_id", "program.id", "BIGINT"),
    ]),
    "hospodarskacinnost": ("hospodarskaCinnost", [
        ("hospodarskacinnost_id", "hospodarskaCinnost.id", "BIGINT"),
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "kategoriaregionov": ("kategoriaRegionov", [("id", "id", "BIGINT")]),
    "makroregionalnastrategiaastrategiapremorskeoblasti": (
        "makroregionalnaStrategiaAStrategiaPreMorskeOblasti", [
            ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
            ("makroregionalnastrategiaastrategiapremorskeoblasti_id",
             "makroregionalnaStrategiaAStrategiaPreMorskeOblasti.id", "BIGINT"),
            ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ]),
    "miestorealizacie": ("miestoRealizacie", [("id", "id", "BIGINT")]),
    "miestorealizaciefull": ("miestoRealizacieFull", [
        ("lokalita", "lokalita", "VARCHAR"),
        ("nuts2_id", "nuts2.id", "BIGINT"),
        ("nuts3_id", "nuts3.id", "BIGINT"),
        ("nuts4_id", "nuts4.id", "BIGINT"),
        ("nuts5_id", "nuts5.id", "BIGINT"),
        ("obecmimoeu", "obecMimoEu", "VARCHAR"),
        ("okresmimoeu", "okresMimoEU", "VARCHAR"),
        ("samospravnykrajmimoeu", "samospravnyKrajMimoEU", "VARCHAR"),
        ("stat_id", "stat.id", "BIGINT"),
    ]),
    "opatrenie": ("opatrenie", [("id", "id", "BIGINT")]),
    "organizacnezlozky": ("organizacneZlozky", [
        ("id", "id", "BIGINT"),
        ("nazov", "nazov", "VARCHAR"),
        ("adresa_ulica", "adresa.ulica", "VARCHAR"),
        ("adresa_cislo", "adresa.cislo", "VARCHAR"),
        ("adresa_psc", "adresa.psc", "VARCHAR"),
        ("adresa_obec", "adresa.obec", "VARCHAR"),
        ("adresa_stat_id", "adresa.stat.id", "BIGINT"),
    ]),
    "partner": ("partner", [("id", "id", "BIGINT")]),
    "polozkyrozpoctu": ("polozkyRozpoctu", [("id", "id", "BIGINT")]),
    "polozkyrozpoctuschvalene": ("polozkyRozpoctuSchvalene", [("id", "id", "BIGINT")]),
    "predchodca": ("predchodca", [
        ("naslednik_id", "naslednik.id", "BIGINT"),
        ("platnostnaslednikaod", "platnostNaslednikaOd", "BIGINT"),
        ("platnostpredchodcudo", "platnostPredchodcuDo", "BIGINT"),
        ("predchodca_id", "predchodca.id", "BIGINT"),
    ]),
    "projektovyzamerius": ("projektovyZamerIUS", [("id", "id", "BIGINT")]),
    "rodovarovnost": ("rodovaRovnost", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("rodovarovnost_id", "rodovaRovnost.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "sekundarnytematickyokruh": ("sekundarnyTematickyOkruh", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("sekundarnytematickyokruh_id", "sekundarnyTematickyOkruh.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "specifickycielprogramu": ("specifickyCielProgramu", [("id", "id", "BIGINT")]),
    "typakcie": ("typAkcie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("typakcie_id", "typAkcie.id", "BIGINT"),
    ]),
    "typintervencie": ("typIntervencie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("typintervencie_id", "typIntervencie.id", "BIGINT"),
    ]),
    "ukazovatelschvalenyvysledku": ("ukazovatelSchvalenyVysledku", [
        ("cielovahodnotaspolu", "cielovaHodnotaSpolu", "DOUBLE"),
        ("vychodiskovahodnotaspolu", "vychodiskovaHodnotaSpolu", "DOUBLE"),
        ("ukazovatelprojektovysc_id", "ukazovatelProjektovySC.id", "BIGINT"),
        ("ukazovatelprojektovysc_kategoriaregionov_id", "ukazovatelProjektovySC.kategoriaRegionov.id", "BIGINT"),
        ("ukazovatelprojektovysc_specifickycielprogramu_id",
         "ukazovatelProjektovySC.specifickyCielProgramu.id", "BIGINT"),
        ("ukazovatelprojektovysc_ukazovatelprojektovy_id",
         "ukazovatelProjektovySC.ukazovatelProjektovy.id", "BIGINT"),
    ]),
    "ukazovatelschvalenyvystupu": ("ukazovatelSchvalenyVystupu", [
        ("cielovahodnotaspolu", "cielovaHodnotaSpolu", "DOUBLE"),
        ("vychodiskovahodnotaspolu", "vychodiskovaHodnotaSpolu", "DOUBLE"),
        ("ukazovatelprojektovysc_id", "ukazovatelProjektovySC.id", "BIGINT"),
        ("ukazovatelprojektovysc_kategoriaregionov_id", "ukazovatelProjektovySC.kategoriaRegionov.id", "BIGINT"),
        ("ukazovatelprojektovysc_specifickycielprogramu_id",
         "ukazovatelProjektovySC.specifickyCielProgramu.id", "BIGINT"),
        ("ukazovatelprojektovysc_ukazovatelprojektovy_id",
         "ukazovatelProjektovySC.ukazovatelProjektovy.id", "BIGINT"),
    ]),
    "ukazovatelziadanyvysledku": ("ukazovatelZiadanyVysledku", [
        ("cielovahodnotaspolu", "cielovaHodnotaSpolu", "DOUBLE"),
        ("vychodiskovahodnotaspolu", "vychodiskovaHodnotaSpolu", "DOUBLE"),
        ("ukazovatelprojektovysc_id", "ukazovatelProjektovySC.id", "BIGINT"),
        ("ukazovatelprojektovysc_kategoriaregionov_id", "ukazovatelProjektovySC.kategoriaRegionov.id", "BIGINT"),
        ("ukazovatelprojektovysc_specifickycielprogramu_id",
         "ukazovatelProjektovySC.specifickyCielProgramu.id", "BIGINT"),
        ("ukazovatelprojektovysc_ukazovatelprojektovy_id",
         "ukazovatelProjektovySC.ukazovatelProjektovy.id", "BIGINT"),
    ]),
    "ukazovatelziadanyvystupu": ("ukazovatelZiadanyVystupu", [
        ("cielovahodnotaspolu", "cielovaHodnotaSpolu", "DOUBLE"),
        ("vychodiskovahodnotaspolu", "vychodiskovaHodnotaSpolu", "DOUBLE"),
        ("ukazovatelprojektovysc_id", "ukazovatelProjektovySC.id", "BIGINT"),
        ("ukazovatelprojektovysc_kategoriaregionov_id", "ukazovatelProjektovySC.kategoriaRegionov.id", "BIGINT"),
        ("ukazovatelprojektovysc_specifickycielprogramu_id",
         "ukazovatelProjektovySC.specifickyCielProgramu.id", "BIGINT"),
        ("ukazovatelprojektovysc_ukazovatelprojektovy_id",
         "ukazovatelProjektovySC.ukazovatelProjektovy.id", "BIGINT"),
    ]),
    "urcitatema": ("urcitaTema", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("urcitatema_id", "urcitaTema.id", "BIGINT"),
    ]),
    "uzemnymechanizmusazameranie": ("uzemnyMechanizmusAZameranie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("uzemnymechanizmusazameranie_id", "uzemnyMechanizmusAZameranie.id", "BIGINT"),
    ]),
    "vykonavanie": ("vykonavanie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("vykonavanie_id", "vykonavanie.id", "BIGINT"),
    ]),
}


def ensure_full_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create itms21_zonfp and every itms21_zonfp_* child table if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in ZONFP_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_ZONFP} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                zonfp_id BIGINT,
                {cols_sql}
            )
        """)


def store_full_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Decompose one zonfp's full detail JSON into itms21_zonfp + all child
    tables. Only ever called once per zonfp id (gated by get_known_ids in
    sync_zonfp), so this is a plain INSERT - never re-fetched, never updated."""
    zonfp_id = detail.get("id")

    columns_sql = ", ".join(col for col, _, _ in ZONFP_COLUMNS)
    placeholders = ", ".join("?" for _ in ZONFP_COLUMNS)
    values = [_get(detail, path) for _, path, _ in ZONFP_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_ZONFP} ({columns_sql}) VALUES ({placeholders}) "
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
            [zonfp_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (zonfp_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_ZONFP}").fetchall()
    return {row[0] for row in rows}


def sync_zonfp() -> tuple[int, int, int]:
    """Fetch the list (ids only, not stored), then fetch full details only for
    ids not already stored.

    Returns (total_in_list, fetched_count, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    ensure_full_schema(con)

    print("Fetching zonfp list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} zonfp.")

    known_ids = get_known_ids(con)

    to_fetch = [item.get("id") for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_fetch)} new zonfp(s) to fetch; {len(list_items) - len(to_fetch)} already stored (skipped).")

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
    total, fetched, failed = sync_zonfp()
    print(f"Done. {total} total in list, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
