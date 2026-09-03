"""
Fetches the project list from api.itms21.sk, detects which project ids are
not yet stored in DuckDB, and fetches full details ONLY for those new ids.
Projects already stored are never re-fetched and never deleted, even if they
disappear from the API list — the table only ever grows (purely additive
incremental sync).

Each detail fetched this way is stored twice, from the same API response:
  - a flat summary row in `projects_current`, which feeds docs/project_data.json
    for the projects.html page (unchanged from before).
  - the full nested detail, decomposed into a normalized PROJEKT table plus
    ~30 PROJEKT_* child/grandchild tables (one row per inner list item, e.g.
    PROJEKT_FINANCNYPLAN, PROJEKT_AKTIVITY, PROJEKT_ZMENAPROJEKT_DOKUMENT...).
    Every one of those tables carries a PROJECT_ID (and, where the item is
    nested two levels deep, its immediate parent row id) so they can all be
    joined back to PROJEKT. This richer schema is NOT used by the website —
    it exists purely so every inner attribute of a project is queryable in
    DuckDB. Rows are inserted once and never updated/deleted, gated by the
    same "id not yet known" check as the simple table above.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests

LIST_URL = "https://api.itms21.sk/public/v1/projekt?limit=-1"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/projekt/id/{id}"

DB_PATH = Path("data/eurofondy.duckdb")  # shared DuckDB file, separate table inside
JSON_OUT_PATH = Path("docs/project_data.json")

TABLE_CURRENT = "projects_current"

MAX_WORKERS = 8           # concurrent detail requests — keep modest to avoid hammering the API
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

LIST_REQUEST_TIMEOUT = 180  # the list endpoint returns every project in one response (limit=-1)


def fetch_list() -> list[dict]:
    """Call the list endpoint and return all project summary records, with
    retry on failure — this single response can be large/slow enough to time out."""
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


def fetch_detail(project_id: int) -> dict | None:
    """Call the detail endpoint for one project, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=project_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={project_id} after {RETRY_ATTEMPTS} attempts: {e}")
                return None
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    return None


def extract_eu_sr(financny_plan: list[dict]) -> tuple[float, float]:
    """Split the financial plan into EU vs national (SR) contribution totals."""
    eu = 0.0
    sr = 0.0
    for item in financny_plan or []:
        zdroj_name = (item.get("zdroj") or {}).get("nazovSk", "")
        suma = item.get("suma") or 0
        if "EÚ" in zdroj_name or "EU" in zdroj_name:
            eu += suma
        elif "ŠR" in zdroj_name or "SR" in zdroj_name:
            sr += suma
    return eu, sr


def flatten_project(d: dict) -> dict:
    """Flatten one detailed project record into a single flat row."""
    program = d.get("program") or {}
    prijimatel = d.get("prijimatel") or {}
    eu, sr = extract_eu_sr(d.get("financnyPlan", []))

    return {
        "project_id": d.get("id"),
        "kod": d.get("kod"),
        "nazov": d.get("nazov"),
        "program_skratka": program.get("skratka"),
        "program_nazov": program.get("nazovSk"),
        "prijimatel_nazov": prijimatel.get("nazov"),
        "prijimatel_ico": prijimatel.get("ico"),
        "stav": d.get("stav"),
        "vrealizacii": bool(d.get("vrealizacii")),
        "ukonceny": bool(d.get("ukonceny")),
        "suma_eu": eu,
        "suma_sr": sr,
        "suma_spolu": eu + sr,
        "celkova_zazmluvnena_suma": d.get("celkovaZazmluvnenaSuma"),
        "poskytnute_prostriedky": d.get("poskytnuteProstriedky"),
        "planovany_zaciatok": d.get("planovanaRealizaciaZaciatok"),
        "planovany_koniec": d.get("planovanaRealizaciaKoniec"),
        "created_at": d.get("createdAt"),
        "updated_at": d.get("updatedAt"),
    }


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CURRENT} (
            project_id                  INTEGER PRIMARY KEY,
            kod                         VARCHAR,
            nazov                       VARCHAR,
            program_skratka             VARCHAR,
            program_nazov               VARCHAR,
            prijimatel_nazov            VARCHAR,
            prijimatel_ico              VARCHAR,
            stav                        VARCHAR,
            vrealizacii                 BOOLEAN,
            ukonceny                    BOOLEAN,
            suma_eu                     DOUBLE,
            suma_sr                     DOUBLE,
            suma_spolu                  DOUBLE,
            celkova_zazmluvnena_suma    DOUBLE,
            poskytnute_prostriedky      DOUBLE,
            planovany_zaciatok          BIGINT,
            planovany_koniec            BIGINT,
            created_at                  BIGINT,
            updated_at                  BIGINT
        )
    """)


def _get(d: dict | None, path: str):
    """Walk a dotted path through nested dicts; None if any segment is missing/None."""
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


# --- Full normalized schema -------------------------------------------------
# Mirrors the ITMS21 "projekt" detail endpoint field-by-field (see module
# docstring). column_name -> (dotted path in the raw detail JSON, DuckDB type).

TABLE_PROJEKT = "PROJEKT"

PROJEKT_COLUMNS: list[tuple[str, str, str]] = [
    ("ID", "id", "BIGINT"),
    ("HREF", "href", "VARCHAR"),
    ("KOD", "kod", "VARCHAR"),
    ("NAZOV", "nazov", "VARCHAR"),
    ("AKRONYM", "akronym", "VARCHAR"),
    ("STAV", "stav", "VARCHAR"),
    ("MRK", "mrk", "VARCHAR"),
    ("ZAMERANIEPROJEKTU", "zameranieProjektu", "VARCHAR"),
    ("UCEL", "ucel", "VARCHAR"),
    ("POPIS", "popis", "VARCHAR"),
    ("POPISKAPACITYPRIJIMATELA", "popisKapacityPrijimatela", "VARCHAR"),
    ("POPISSITUACIEPOREALIZACII", "popisSituaciePoRealizacii", "VARCHAR"),
    ("POPISSPOSOBUREALIZACIE", "popisSposobuRealizacie", "VARCHAR"),
    ("POPISVYCHODISKOVEJSITUACIE", "popisVychodiskovejSituacie", "VARCHAR"),
    ("CELKOVAZAZMLUVNENASUMA", "celkovaZazmluvnenaSuma", "DOUBLE"),
    ("CELKOVAZAZMLUVNENASUMAPOVODNA", "celkovaZazmluvnenaSumaPovodna", "DOUBLE"),
    ("ZAZMLUVNENASUMANFP", "zazmluvnenaSumaNfp", "DOUBLE"),
    ("POSKYTNUTEPROSTRIEDKY", "poskytnuteProstriedky", "DOUBLE"),
    ("DATUMZACIATKUHLAVNYCHAKTIVIT", "datumZaciatkuHlavnychAktivit", "BIGINT"),
    ("DATUMKONCAHLAVNYCHAKTIVIT", "datumKoncaHlavnychAktivit", "BIGINT"),
    ("DLZKACELKOVAHLAVNYCHAKTIVIT", "dlzkaCelkovaHlavnychAktivit", "INTEGER"),
    ("DLZKACELKOVAPROJEKTU", "dlzkaCelkovaProjektu", "INTEGER"),
    ("PLANOVANAREALIZACIAZACIATOK", "planovanaRealizaciaZaciatok", "BIGINT"),
    ("PLANOVANAREALIZACIAKONIEC", "planovanaRealizaciaKoniec", "BIGINT"),
    ("SKUTOCNAREALIZACIAZACIATOK", "skutocnaRealizaciaZaciatok", "BIGINT"),
    ("SKUTOCNAREALIZACIAKONIEC", "skutocnaRealizaciaKoniec", "BIGINT"),
    ("JEOTVORENAZMENA", "jeOtvorenaZmena", "BOOLEAN"),
    ("MAKATEGORIUREGIONOV", "maKategoriuRegionov", "BOOLEAN"),
    ("MIMORIADNEUKONCENYNEPRISPEL", "mimoriadneUkoncenyNeprispel", "BOOLEAN"),
    ("MIMORIADNEUKONCENYPRISPEL", "mimoriadneUkoncenyPrispel", "BOOLEAN"),
    ("OBSAHUJEMIESTOREALIZACIEZAHRANICIE", "obsahujeMiestoRealizacieZahranicie", "BOOLEAN"),
    ("REALIZACIAAKTIVITPOZASTAVENA", "realizaciaAktivitPozastavena", "BOOLEAN"),
    ("UDRZATELNYROZVOJMIEST", "udrzatelnyRozvojMiest", "BOOLEAN"),
    ("UKONCENY", "ukonceny", "BOOLEAN"),
    ("VREALIZACII", "vrealizacii", "BOOLEAN"),
    ("VYLUCENYZFINANCOVANIA", "vylucenyZFinancovania", "BOOLEAN"),
    ("PRIJIMATEL_ID", "prijimatel.id", "BIGINT"),
    ("PROGRAM_ID", "program.id", "BIGINT"),
    ("VYZVA_ID", "vyzva.id", "BIGINT"),
    ("ZONFP_ID", "zonfp.id", "BIGINT"),
    ("POSKYTOVATELORGAN_ID", "poskytovatelOrgan.id", "BIGINT"),
    ("POSKYTOVATELORGAN_KOD", "poskytovatelOrgan.kod", "VARCHAR"),
    ("POSKYTOVATELORGAN_NAZOV", "poskytovatelOrgan.nazov", "VARCHAR"),
    ("POSKYTOVATELSUBJEKT_ID", "poskytovatelSubjekt.id", "BIGINT"),
    ("VYHLASOVATELORGAN_ID", "vyhlasovatelOrgan.id", "BIGINT"),
    ("VYHLASOVATELORGAN_KOD", "vyhlasovatelOrgan.kod", "VARCHAR"),
    ("VYHLASOVATELORGAN_NAZOV", "vyhlasovatelOrgan.nazov", "VARCHAR"),
    ("ZMLUVAPROJEKT_ID", "zmluvaProjekt.id", "BIGINT"),
    ("ZMLUVAPROJEKT_CISLO", "zmluvaProjekt.cislo", "VARCHAR"),
    ("ZMLUVAPROJEKT_DATUMPLATNOSTI", "zmluvaProjekt.datumPlatnosti", "BIGINT"),
    ("ZMLUVAPROJEKT_DATUMUCINNOSTI", "zmluvaProjekt.datumUcinnosti", "BIGINT"),
    ("ZMLUVAPROJEKT_URL", "zmluvaProjekt.url", "VARCHAR"),
    ("CREATEDAT", "createdAt", "BIGINT"),
    ("UPDATEDAT", "updatedAt", "BIGINT"),
]

# table_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "PROJEKT_AKTIVITY": ("aktivity", [("ID", "id", "BIGINT")]),
    "PROJEKT_CIELOVASKUPINA": ("cielovaSkupina", [("ID", "id", "BIGINT")]),
    "PROJEKT_DODAVATEL": ("dodavatel", [
        ("DIC", "dic", "VARCHAR"),
        ("ICO", "ico", "VARCHAR"),
        ("INEIDENTIFIKACNECISLO", "ineIdentifikacneCislo", "VARCHAR"),
        ("NAZOV", "nazov", "VARCHAR"),
    ]),
    "PROJEKT_FINANCNYPLAN": ("financnyPlan", [
        ("SUMA", "suma", "DOUBLE"),
        ("ZDROJ_ID", "zdroj.id", "BIGINT"),
    ]),
    "PROJEKT_FORMAPODPORY": ("formaPodpory", [
        ("FORMAPODPORY_ID", "formaPodpory.id", "BIGINT"),
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "PROJEKT_HOSPODARSKACINNOST": ("hospodarskaCinnost", [
        ("HOSPODARSKACINNOST_ID", "hospodarskaCinnost.id", "BIGINT"),
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "PROJEKT_INEUDAJE": ("ineUdaje", [
        ("INEUDAJESC_INEUDAJE_KOD", "ineUdajeSC.ineUdaje.kod", "VARCHAR"),
        ("INEUDAJESC_INEUDAJE_MERNAJEDNOTKA_ID", "ineUdajeSC.ineUdaje.mernaJednotka.id", "BIGINT"),
        ("INEUDAJESC_INEUDAJE_NAZOVDE", "ineUdajeSC.ineUdaje.nazovDe", "VARCHAR"),
        ("INEUDAJESC_INEUDAJE_NAZOVEN", "ineUdajeSC.ineUdaje.nazovEn", "VARCHAR"),
        ("INEUDAJESC_INEUDAJE_NAZOVSK", "ineUdajeSC.ineUdaje.nazovSk", "VARCHAR"),
        ("INEUDAJESC_KATEGORIAREGIONOV_ID", "ineUdajeSC.kategoriaRegionov.id", "BIGINT"),
        ("INEUDAJESC_SPECIFICKYCIELPROGRAMU_ID", "ineUdajeSC.specifickyCielProgramu.id", "BIGINT"),
        ("SUBJEKTNAPROJEKT_PLATNOSTDO", "subjektNaProjekt.platnostDo", "BIGINT"),
        ("SUBJEKTNAPROJEKT_PLATNOSTOD", "subjektNaProjekt.platnostOd", "BIGINT"),
        ("SUBJEKTNAPROJEKT_ROLA", "subjektNaProjekt.rola", "VARCHAR"),
        ("SUBJEKTNAPROJEKT_SUBJEKT_ID", "subjektNaProjekt.subjekt.id", "BIGINT"),
    ]),
    "PROJEKT_INTENZITY": ("intenzity", [("ID", "id", "BIGINT")]),
    "PROJEKT_KATEGORIAREGIONOV": ("kategoriaRegionov", [("ID", "id", "BIGINT")]),
    "PROJEKT_MAKROREGIONALNASTRATEGIAASTRATEGIAPREMORSKEOBLASTI": (
        "makroregionalnaStrategiaAStrategiaPreMorskeOblasti", [
            ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
            ("MAKROREGIONALNASTRATEGIAASTRATEGIAPREMORSKEOBLASTI_ID",
             "makroregionalnaStrategiaAStrategiaPreMorskeOblasti.id", "BIGINT"),
            ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ]),
    "PROJEKT_MIESTOREALIZACIE": ("miestoRealizacie", [("ID", "id", "BIGINT")]),
    "PROJEKT_MIESTOREALIZACIEFULL": ("miestoRealizacieFull", [
        ("LOKALITA", "lokalita", "VARCHAR"),
        ("NUTS2_ID", "nuts2.id", "BIGINT"),
        ("NUTS3_ID", "nuts3.id", "BIGINT"),
        ("NUTS4_ID", "nuts4.id", "BIGINT"),
        ("NUTS5_ID", "nuts5.id", "BIGINT"),
        ("OBECMIMOEU", "obecMimoEu", "VARCHAR"),
        ("OKRESMIMOEU", "okresMimoEU", "VARCHAR"),
        ("SAMOSPRAVNYKRAJMIMOEU", "samospravnyKrajMimoEU", "VARCHAR"),
        ("STAT_ID", "stat.id", "BIGINT"),
    ]),
    "PROJEKT_MONITOROVACIETERMINY": ("monitorovacieTerminy", [
        ("ID", "id", "BIGINT"),
        ("DATUMPREDLOZENIANAJNESKORSI", "datumPredlozeniaNajneskorsi", "BIGINT"),
        ("PORADOVECISLO", "poradoveCislo", "INTEGER"),
        ("TERMINMONITOROVANIA", "terminMonitorovania", "BIGINT"),
        ("TYPMONITOROVANIA", "typMonitorovania", "VARCHAR"),
    ]),
    "PROJEKT_OBLASTINTERVENCIE": ("oblastIntervencie", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("OBLASTINTERVENCIE_ID", "oblastIntervencie.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "PROJEKT_OPATRENIE": ("opatrenie", [("ID", "id", "BIGINT")]),
    "PROJEKT_ORGANIZACNEZLOZKY": ("organizacneZlozky", [
        ("ID", "id", "BIGINT"),
        ("NAZOV", "nazov", "VARCHAR"),
        ("ADRESA_ULICA", "adresa.ulica", "VARCHAR"),
        ("ADRESA_CISLO", "adresa.cislo", "VARCHAR"),
        ("ADRESA_PSC", "adresa.psc", "VARCHAR"),
        ("ADRESA_OBEC", "adresa.obec", "VARCHAR"),
        ("ADRESA_STAT_ID", "adresa.stat.id", "BIGINT"),
    ]),
    "PROJEKT_PARTNER": ("partner", [("ID", "id", "BIGINT")]),
    "PROJEKT_POLOZKYROZPOCTU": ("polozkyRozpoctu", [("ID", "id", "BIGINT")]),
    "PROJEKT_PREDCHODCA": ("predchodca", [
        ("PLATNOSTNASLEDNIKAOD", "platnostNaslednikaOd", "BIGINT"),
        ("PLATNOSTPREDCHODCUDO", "platnostPredchodcuDo", "BIGINT"),
        ("NASLEDNIK_ROLA", "naslednik.rola", "VARCHAR"),
        ("NASLEDNIK_PLATNOSTOD", "naslednik.platnostOd", "BIGINT"),
        ("NASLEDNIK_PLATNOSTDO", "naslednik.platnostDo", "BIGINT"),
        ("NASLEDNIK_SUBJEKT_ID", "naslednik.subjekt.id", "BIGINT"),
        ("PREDCHODCA_ROLA", "predchodca.rola", "VARCHAR"),
        ("PREDCHODCA_PLATNOSTOD", "predchodca.platnostOd", "BIGINT"),
        ("PREDCHODCA_PLATNOSTDO", "predchodca.platnostDo", "BIGINT"),
        ("PREDCHODCA_SUBJEKT_ID", "predchodca.subjekt.id", "BIGINT"),
    ]),
    "PROJEKT_PROJEKTOVYZAMERIUS": ("projektovyZamerIUS", [("ID", "id", "BIGINT")]),
    "PROJEKT_RODOVAROVNOST": ("rodovaRovnost", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("RODOVAROVNOST_ID", "rodovaRovnost.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "PROJEKT_SEKUNDARNYTEMATICKYOKRUH": ("sekundarnyTematickyOkruh", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SEKUNDARNYTEMATICKYOKRUH_ID", "sekundarnyTematickyOkruh.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "PROJEKT_SPECIFICKYCIELPROGRAMU": ("specifickyCielProgramu", [("ID", "id", "BIGINT")]),
    "PROJEKT_TYPAKCIE": ("typAkcie", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ("TYPAKCIE_ID", "typAkcie.id", "BIGINT"),
    ]),
    "PROJEKT_TYPINTERVENCIE": ("typIntervencie", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ("TYPINTERVENCIE_ID", "typIntervencie.id", "BIGINT"),
    ]),
    "PROJEKT_UKAZOVATELVYSLEDKU": ("ukazovatelVysledku", [
        ("CIELOVAHODNOTASPOLU", "cielovaHodnotaSpolu", "DOUBLE"),
        ("VYCHODISKOVAHODNOTASPOLU", "vychodiskovaHodnotaSpolu", "DOUBLE"),
        ("UKAZOVATELPROJEKTOVYSC_ID", "ukazovatelProjektovySC.id", "BIGINT"),
        ("UKAZOVATELPROJEKTOVYSC_KATEGORIAREGIONOV_ID", "ukazovatelProjektovySC.kategoriaRegionov.id", "BIGINT"),
        ("UKAZOVATELPROJEKTOVYSC_SPECIFICKYCIELPROGRAMU_ID",
         "ukazovatelProjektovySC.specifickyCielProgramu.id", "BIGINT"),
        ("UKAZOVATELPROJEKTOVYSC_UKAZOVATELPROJEKTOVY_ID",
         "ukazovatelProjektovySC.ukazovatelProjektovy.id", "BIGINT"),
    ]),
    "PROJEKT_UKAZOVATELVYSTUPU": ("ukazovatelVystupu", [
        ("CIELOVAHODNOTASPOLU", "cielovaHodnotaSpolu", "DOUBLE"),
        ("VYCHODISKOVAHODNOTASPOLU", "vychodiskovaHodnotaSpolu", "DOUBLE"),
        ("UKAZOVATELPROJEKTOVYSC_ID", "ukazovatelProjektovySC.id", "BIGINT"),
        ("UKAZOVATELPROJEKTOVYSC_KATEGORIAREGIONOV_ID", "ukazovatelProjektovySC.kategoriaRegionov.id", "BIGINT"),
        ("UKAZOVATELPROJEKTOVYSC_SPECIFICKYCIELPROGRAMU_ID",
         "ukazovatelProjektovySC.specifickyCielProgramu.id", "BIGINT"),
        ("UKAZOVATELPROJEKTOVYSC_UKAZOVATELPROJEKTOVY_ID",
         "ukazovatelProjektovySC.ukazovatelProjektovy.id", "BIGINT"),
    ]),
    "PROJEKT_URCITATEMA": ("urcitaTema", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ("URCITATEMA_ID", "urcitaTema.id", "BIGINT"),
    ]),
    "PROJEKT_UZEMNYMECHANIZMUSAZAMERANIE": ("uzemnyMechanizmusAZameranie", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ("UZEMNYMECHANIZMUSAZAMERANIE_ID", "uzemnyMechanizmusAZameranie.id", "BIGINT"),
    ]),
    "PROJEKT_VYKONAVANIE": ("vykonavanie", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ("VYKONAVANIE_ID", "vykonavanie.id", "BIGINT"),
    ]),
    "PROJEKT_ZMENAPROJEKT": ("zmenaProjekt", [
        ("ID", "id", "BIGINT"),
        ("CISLODODATKU", "cisloDodatku", "VARCHAR"),
        ("PREDMET", "predmet", "VARCHAR"),
        ("DATUMPLATNOSTI", "datumPlatnosti", "BIGINT"),
        ("DATUMUCINNOSTI", "datumUcinnosti", "BIGINT"),
        ("URL", "url", "VARCHAR"),
    ]),
}


def ensure_full_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create PROJEKT and every PROJEKT_* child/grandchild table if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in PROJEKT_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_PROJEKT} (
            {cols_sql},
            PRIMARY KEY (ID)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                PROJECT_ID BIGINT,
                {cols_sql}
            )
        """)

    # Two grandchild tables: a "dokument" list living one level deeper than a
    # child-table row (PROJEKT_ZMENAPROJEKT) or the flattened PROJEKT row
    # (zmluvaProjekt). Both carry PROJECT_ID so they join back to PROJEKT.
    con.execute("""
        CREATE TABLE IF NOT EXISTS PROJEKT_ZMENAPROJEKT_DOKUMENT (
            PROJECT_ID BIGINT,
            ZMENAPROJEKT_ID BIGINT,
            NAZOV VARCHAR,
            UUID VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS PROJEKT_ZMLUVAPROJEKT_DOKUMENT (
            PROJECT_ID BIGINT,
            NAZOV VARCHAR,
            UUID VARCHAR
        )
    """)


def store_full_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Decompose one project's full detail JSON into PROJEKT + all child/grandchild
    tables. Only ever called once per project id (gated by get_known_ids in
    sync_projects), so this is a plain INSERT — never re-fetched, never updated."""
    project_id = detail.get("id")

    columns_sql = ", ".join(col for col, _, _ in PROJEKT_COLUMNS)
    placeholders = ", ".join("?" for _ in PROJEKT_COLUMNS)
    values = [_get(detail, path) for _, path, _ in PROJEKT_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_PROJEKT} ({columns_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (ID) DO NOTHING",
        values,
    )

    for table, (source_field, columns) in CHILD_TABLES.items():
        items = detail.get(source_field) or []
        if not items:
            continue
        columns_sql = ", ".join(col for col, _, _ in columns)
        placeholders = ", ".join("?" for _ in columns)
        rows = [
            [project_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {table} (PROJECT_ID, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )

    zmena_doc_rows = []
    for item in detail.get("zmenaProjekt") or []:
        zmena_id = item.get("id")
        for doc in item.get("dokument") or []:
            zmena_doc_rows.append([project_id, zmena_id, doc.get("nazov"), doc.get("uuid")])
    if zmena_doc_rows:
        con.executemany(
            "INSERT INTO PROJEKT_ZMENAPROJEKT_DOKUMENT (PROJECT_ID, ZMENAPROJEKT_ID, NAZOV, UUID) "
            "VALUES (?, ?, ?, ?)",
            zmena_doc_rows,
        )

    zmluva_docs = (detail.get("zmluvaProjekt") or {}).get("dokument") or []
    if zmluva_docs:
        con.executemany(
            "INSERT INTO PROJEKT_ZMLUVAPROJEKT_DOKUMENT (PROJECT_ID, NAZOV, UUID) VALUES (?, ?, ?)",
            [[project_id, doc.get("nazov"), doc.get("uuid")] for doc in zmluva_docs],
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored (both the simple summary and the full detail
    schema are written together), so we never re-fetch them."""
    rows = con.execute(f"SELECT ID FROM {TABLE_PROJEKT}").fetchall()
    return {row[0] for row in rows}


def upsert_row(con: duckdb.DuckDBPyConnection, row: dict) -> None:
    con.execute(f"""
        INSERT INTO {TABLE_CURRENT} (
            project_id, kod, nazov, program_skratka, program_nazov,
            prijimatel_nazov, prijimatel_ico, stav, vrealizacii, ukonceny,
            suma_eu, suma_sr, suma_spolu, celkova_zazmluvnena_suma,
            poskytnute_prostriedky, planovany_zaciatok, planovany_koniec,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (project_id) DO UPDATE SET
            kod = excluded.kod,
            nazov = excluded.nazov,
            program_skratka = excluded.program_skratka,
            program_nazov = excluded.program_nazov,
            prijimatel_nazov = excluded.prijimatel_nazov,
            prijimatel_ico = excluded.prijimatel_ico,
            stav = excluded.stav,
            vrealizacii = excluded.vrealizacii,
            ukonceny = excluded.ukonceny,
            suma_eu = excluded.suma_eu,
            suma_sr = excluded.suma_sr,
            suma_spolu = excluded.suma_spolu,
            celkova_zazmluvnena_suma = excluded.celkova_zazmluvnena_suma,
            poskytnute_prostriedky = excluded.poskytnute_prostriedky,
            planovany_zaciatok = excluded.planovany_zaciatok,
            planovany_koniec = excluded.planovany_koniec,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at
    """, [
        row["project_id"], row["kod"], row["nazov"], row["program_skratka"], row["program_nazov"],
        row["prijimatel_nazov"], row["prijimatel_ico"], row["stav"], row["vrealizacii"], row["ukonceny"],
        row["suma_eu"], row["suma_sr"], row["suma_spolu"], row["celkova_zazmluvnena_suma"],
        row["poskytnute_prostriedky"], row["planovany_zaciatok"], row["planovany_koniec"],
        row["created_at"], row["updated_at"],
    ])


def sync_projects() -> tuple[int, int, int]:
    """Fetch the list, then fetch full details only for ids not already stored.

    Existing rows are left untouched (no re-fetch) and nothing is ever deleted,
    even if a project drops out of the current API list.

    Returns (total_in_list, fetched_count, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    ensure_table(con)
    ensure_full_schema(con)

    print("Fetching project list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} projects.")

    known_ids = get_known_ids(con)

    to_fetch = [item.get("id") for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_fetch)} new project(s) to fetch; {len(list_items) - len(to_fetch)} already stored (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, pid): pid for pid in to_fetch}
            for i, future in enumerate(as_completed(future_to_id), start=1):
                pid = future_to_id[future]
                detail = future.result()
                if detail is None:
                    failed_count += 1
                    continue
                row = flatten_project(detail)
                upsert_row(con, row)
                store_full_detail(con, detail)
                fetched_count += 1

                if i % 50 == 0 or i == len(to_fetch):
                    con.commit()  # periodic commit so progress survives an interruption
                    print(f"  Progress: {i}/{len(to_fetch)} processed "
                          f"({fetched_count} ok, {failed_count} failed)")

    con.commit()
    con.close()

    return len(list_items), fetched_count, failed_count


def export_to_json() -> None:
    con = duckdb.connect(str(DB_PATH))
    df = con.execute(f"SELECT * FROM {TABLE_CURRENT} ORDER BY suma_spolu DESC").fetchdf()
    con.close()

    JSON_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Convert epoch-millisecond columns to proper ISO date strings for display.
    # DuckDB/pandas keep them as BIGINT internally (fine for storage/queries),
    # but the exported JSON should be human-readable, same as program_data.json.
    date_cols = ["planovany_zaciatok", "planovany_koniec", "created_at", "updated_at"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")

    export = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "projects": json.loads(df.to_json(orient="records", date_format="iso")),
    }

    with open(JSON_OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)

    print(f"Exported {len(df)} projects to {JSON_OUT_PATH}")


def main():
    total, fetched, failed = sync_projects()
    print(f"Done. {total} total in list, {fetched} newly fetched, {failed} failed.")
    export_to_json()


if __name__ == "__main__":
    main()