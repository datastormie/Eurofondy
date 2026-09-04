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

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate table inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file
JSON_OUT_PATH = Path("docs/project_data.json")

TABLE_PREFIX = "itms21_"
TABLE_CURRENT = f"{TABLE_PREFIX}projects_current"

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
    # One-time rename from the pre-"itms21_"-prefix table name; a no-op once
    # the rename has happened (ALTER TABLE IF EXISTS is idempotent).
    con.execute(f"ALTER TABLE IF EXISTS projects_current RENAME TO {TABLE_CURRENT}")
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
# Mirrors the ITMS21 "projekt" detail endpoint field-by-field (see module
# docstring). column_name -> (dotted path in the raw detail JSON, DuckDB type).

TABLE_PROJEKT = f"{TABLE_PREFIX}PROJEKT".lower()

PROJEKT_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("nazov", "nazov", "VARCHAR"),
    ("akronym", "akronym", "VARCHAR"),
    ("stav", "stav", "VARCHAR"),
    ("mrk", "mrk", "VARCHAR"),
    ("zameranieprojektu", "zameranieProjektu", "VARCHAR"),
    ("ucel", "ucel", "VARCHAR"),
    ("popis", "popis", "VARCHAR"),
    ("popiskapacityprijimatela", "popisKapacityPrijimatela", "VARCHAR"),
    ("popissituacieporealizacii", "popisSituaciePoRealizacii", "VARCHAR"),
    ("popissposoburealizacie", "popisSposobuRealizacie", "VARCHAR"),
    ("popisvychodiskovejsituacie", "popisVychodiskovejSituacie", "VARCHAR"),
    ("celkovazazmluvnenasuma", "celkovaZazmluvnenaSuma", "DOUBLE"),
    ("celkovazazmluvnenasumapovodna", "celkovaZazmluvnenaSumaPovodna", "DOUBLE"),
    ("zazmluvnenasumanfp", "zazmluvnenaSumaNfp", "DOUBLE"),
    ("poskytnuteprostriedky", "poskytnuteProstriedky", "DOUBLE"),
    ("datumzaciatkuhlavnychaktivit", "datumZaciatkuHlavnychAktivit", "BIGINT"),
    ("datumkoncahlavnychaktivit", "datumKoncaHlavnychAktivit", "BIGINT"),
    ("dlzkacelkovahlavnychaktivit", "dlzkaCelkovaHlavnychAktivit", "INTEGER"),
    ("dlzkacelkovaprojektu", "dlzkaCelkovaProjektu", "INTEGER"),
    ("planovanarealizaciazaciatok", "planovanaRealizaciaZaciatok", "BIGINT"),
    ("planovanarealizaciakoniec", "planovanaRealizaciaKoniec", "BIGINT"),
    ("skutocnarealizaciazaciatok", "skutocnaRealizaciaZaciatok", "BIGINT"),
    ("skutocnarealizaciakoniec", "skutocnaRealizaciaKoniec", "BIGINT"),
    ("jeotvorenazmena", "jeOtvorenaZmena", "BOOLEAN"),
    ("makategoriuregionov", "maKategoriuRegionov", "BOOLEAN"),
    ("mimoriadneukoncenyneprispel", "mimoriadneUkoncenyNeprispel", "BOOLEAN"),
    ("mimoriadneukoncenyprispel", "mimoriadneUkoncenyPrispel", "BOOLEAN"),
    ("obsahujemiestorealizaciezahranicie", "obsahujeMiestoRealizacieZahranicie", "BOOLEAN"),
    ("realizaciaaktivitpozastavena", "realizaciaAktivitPozastavena", "BOOLEAN"),
    ("udrzatelnyrozvojmiest", "udrzatelnyRozvojMiest", "BOOLEAN"),
    ("ukonceny", "ukonceny", "BOOLEAN"),
    ("vrealizacii", "vrealizacii", "BOOLEAN"),
    ("vylucenyzfinancovania", "vylucenyZFinancovania", "BOOLEAN"),
    ("prijimatel_id", "prijimatel.id", "BIGINT"),
    ("program_id", "program.id", "BIGINT"),
    ("vyzva_id", "vyzva.id", "BIGINT"),
    ("zonfp_id", "zonfp.id", "BIGINT"),
    ("poskytovatelorgan_id", "poskytovatelOrgan.id", "BIGINT"),
    ("poskytovatelorgan_kod", "poskytovatelOrgan.kod", "VARCHAR"),
    ("poskytovatelorgan_nazov", "poskytovatelOrgan.nazov", "VARCHAR"),
    ("poskytovatelsubjekt_id", "poskytovatelSubjekt.id", "BIGINT"),
    ("vyhlasovatelorgan_id", "vyhlasovatelOrgan.id", "BIGINT"),
    ("vyhlasovatelorgan_kod", "vyhlasovatelOrgan.kod", "VARCHAR"),
    ("vyhlasovatelorgan_nazov", "vyhlasovatelOrgan.nazov", "VARCHAR"),
    ("zmluvaprojekt_id", "zmluvaProjekt.id", "BIGINT"),
    ("zmluvaprojekt_cislo", "zmluvaProjekt.cislo", "VARCHAR"),
    ("zmluvaprojekt_datumplatnosti", "zmluvaProjekt.datumPlatnosti", "BIGINT"),
    ("zmluvaprojekt_datumucinnosti", "zmluvaProjekt.datumUcinnosti", "BIGINT"),
    ("zmluvaprojekt_url", "zmluvaProjekt.url", "VARCHAR"),
    ("createdat", "createdAt", "BIGINT"),
    ("updatedat", "updatedAt", "BIGINT"),
]

# table_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "PROJEKT_AKTIVITY": ("aktivity", [("id", "id", "BIGINT")]),
    "PROJEKT_CIELOVASKUPINA": ("cielovaSkupina", [("id", "id", "BIGINT")]),
    "PROJEKT_DODAVATEL": ("dodavatel", [
        ("dic", "dic", "VARCHAR"),
        ("ico", "ico", "VARCHAR"),
        ("ineidentifikacnecislo", "ineIdentifikacneCislo", "VARCHAR"),
        ("nazov", "nazov", "VARCHAR"),
    ]),
    "PROJEKT_FINANCNYPLAN": ("financnyPlan", [
        ("suma", "suma", "DOUBLE"),
        ("zdroj_id", "zdroj.id", "BIGINT"),
    ]),
    "PROJEKT_FORMAPODPORY": ("formaPodpory", [
        ("formapodpory_id", "formaPodpory.id", "BIGINT"),
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "PROJEKT_HOSPODARSKACINNOST": ("hospodarskaCinnost", [
        ("hospodarskacinnost_id", "hospodarskaCinnost.id", "BIGINT"),
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "PROJEKT_INEUDAJE": ("ineUdaje", [
        ("ineudajesc_ineudaje_kod", "ineUdajeSC.ineUdaje.kod", "VARCHAR"),
        ("ineudajesc_ineudaje_mernajednotka_id", "ineUdajeSC.ineUdaje.mernaJednotka.id", "BIGINT"),
        ("ineudajesc_ineudaje_nazovde", "ineUdajeSC.ineUdaje.nazovDe", "VARCHAR"),
        ("ineudajesc_ineudaje_nazoven", "ineUdajeSC.ineUdaje.nazovEn", "VARCHAR"),
        ("ineudajesc_ineudaje_nazovsk", "ineUdajeSC.ineUdaje.nazovSk", "VARCHAR"),
        ("ineudajesc_kategoriaregionov_id", "ineUdajeSC.kategoriaRegionov.id", "BIGINT"),
        ("ineudajesc_specifickycielprogramu_id", "ineUdajeSC.specifickyCielProgramu.id", "BIGINT"),
        ("subjektnaprojekt_platnostdo", "subjektNaProjekt.platnostDo", "BIGINT"),
        ("subjektnaprojekt_platnostod", "subjektNaProjekt.platnostOd", "BIGINT"),
        ("subjektnaprojekt_rola", "subjektNaProjekt.rola", "VARCHAR"),
        ("subjektnaprojekt_subjekt_id", "subjektNaProjekt.subjekt.id", "BIGINT"),
    ]),
    "PROJEKT_INTENZITY": ("intenzity", [("id", "id", "BIGINT")]),
    "PROJEKT_KATEGORIAREGIONOV": ("kategoriaRegionov", [("id", "id", "BIGINT")]),
    "PROJEKT_MAKROREGIONALNASTRATEGIAASTRATEGIAPREMORSKEOBLASTI": (
        "makroregionalnaStrategiaAStrategiaPreMorskeOblasti", [
            ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
            ("makroregionalnastrategiaastrategiapremorskeoblasti_id",
             "makroregionalnaStrategiaAStrategiaPreMorskeOblasti.id", "BIGINT"),
            ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ]),
    "PROJEKT_MIESTOREALIZACIE": ("miestoRealizacie", [("id", "id", "BIGINT")]),
    "PROJEKT_MIESTOREALIZACIEFULL": ("miestoRealizacieFull", [
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
    "PROJEKT_MONITOROVACIETERMINY": ("monitorovacieTerminy", [
        ("id", "id", "BIGINT"),
        ("datumpredlozenianajneskorsi", "datumPredlozeniaNajneskorsi", "BIGINT"),
        ("poradovecislo", "poradoveCislo", "INTEGER"),
        ("terminmonitorovania", "terminMonitorovania", "BIGINT"),
        ("typmonitorovania", "typMonitorovania", "VARCHAR"),
    ]),
    "PROJEKT_OBLASTINTERVENCIE": ("oblastIntervencie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("oblastintervencie_id", "oblastIntervencie.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "PROJEKT_OPATRENIE": ("opatrenie", [("id", "id", "BIGINT")]),
    "PROJEKT_ORGANIZACNEZLOZKY": ("organizacneZlozky", [
        ("id", "id", "BIGINT"),
        ("nazov", "nazov", "VARCHAR"),
        ("adresa_ulica", "adresa.ulica", "VARCHAR"),
        ("adresa_cislo", "adresa.cislo", "VARCHAR"),
        ("adresa_psc", "adresa.psc", "VARCHAR"),
        ("adresa_obec", "adresa.obec", "VARCHAR"),
        ("adresa_stat_id", "adresa.stat.id", "BIGINT"),
    ]),
    "PROJEKT_PARTNER": ("partner", [("id", "id", "BIGINT")]),
    "PROJEKT_POLOZKYROZPOCTU": ("polozkyRozpoctu", [("id", "id", "BIGINT")]),
    "PROJEKT_PREDCHODCA": ("predchodca", [
        ("platnostnaslednikaod", "platnostNaslednikaOd", "BIGINT"),
        ("platnostpredchodcudo", "platnostPredchodcuDo", "BIGINT"),
        ("naslednik_rola", "naslednik.rola", "VARCHAR"),
        ("naslednik_platnostod", "naslednik.platnostOd", "BIGINT"),
        ("naslednik_platnostdo", "naslednik.platnostDo", "BIGINT"),
        ("naslednik_subjekt_id", "naslednik.subjekt.id", "BIGINT"),
        ("predchodca_rola", "predchodca.rola", "VARCHAR"),
        ("predchodca_platnostod", "predchodca.platnostOd", "BIGINT"),
        ("predchodca_platnostdo", "predchodca.platnostDo", "BIGINT"),
        ("predchodca_subjekt_id", "predchodca.subjekt.id", "BIGINT"),
    ]),
    "PROJEKT_PROJEKTOVYZAMERIUS": ("projektovyZamerIUS", [("id", "id", "BIGINT")]),
    "PROJEKT_RODOVAROVNOST": ("rodovaRovnost", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("rodovarovnost_id", "rodovaRovnost.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "PROJEKT_SEKUNDARNYTEMATICKYOKRUH": ("sekundarnyTematickyOkruh", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("sekundarnytematickyokruh_id", "sekundarnyTematickyOkruh.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "PROJEKT_SPECIFICKYCIELPROGRAMU": ("specifickyCielProgramu", [("id", "id", "BIGINT")]),
    "PROJEKT_TYPAKCIE": ("typAkcie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("typakcie_id", "typAkcie.id", "BIGINT"),
    ]),
    "PROJEKT_TYPINTERVENCIE": ("typIntervencie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("typintervencie_id", "typIntervencie.id", "BIGINT"),
    ]),
    "PROJEKT_UKAZOVATELVYSLEDKU": ("ukazovatelVysledku", [
        ("cielovahodnotaspolu", "cielovaHodnotaSpolu", "DOUBLE"),
        ("vychodiskovahodnotaspolu", "vychodiskovaHodnotaSpolu", "DOUBLE"),
        ("ukazovatelprojektovysc_id", "ukazovatelProjektovySC.id", "BIGINT"),
        ("ukazovatelprojektovysc_kategoriaregionov_id", "ukazovatelProjektovySC.kategoriaRegionov.id", "BIGINT"),
        ("ukazovatelprojektovysc_specifickycielprogramu_id",
         "ukazovatelProjektovySC.specifickyCielProgramu.id", "BIGINT"),
        ("ukazovatelprojektovysc_ukazovatelprojektovy_id",
         "ukazovatelProjektovySC.ukazovatelProjektovy.id", "BIGINT"),
    ]),
    "PROJEKT_UKAZOVATELVYSTUPU": ("ukazovatelVystupu", [
        ("cielovahodnotaspolu", "cielovaHodnotaSpolu", "DOUBLE"),
        ("vychodiskovahodnotaspolu", "vychodiskovaHodnotaSpolu", "DOUBLE"),
        ("ukazovatelprojektovysc_id", "ukazovatelProjektovySC.id", "BIGINT"),
        ("ukazovatelprojektovysc_kategoriaregionov_id", "ukazovatelProjektovySC.kategoriaRegionov.id", "BIGINT"),
        ("ukazovatelprojektovysc_specifickycielprogramu_id",
         "ukazovatelProjektovySC.specifickyCielProgramu.id", "BIGINT"),
        ("ukazovatelprojektovysc_ukazovatelprojektovy_id",
         "ukazovatelProjektovySC.ukazovatelProjektovy.id", "BIGINT"),
    ]),
    "PROJEKT_URCITATEMA": ("urcitaTema", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("urcitatema_id", "urcitaTema.id", "BIGINT"),
    ]),
    "PROJEKT_UZEMNYMECHANIZMUSAZAMERANIE": ("uzemnyMechanizmusAZameranie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("uzemnymechanizmusazameranie_id", "uzemnyMechanizmusAZameranie.id", "BIGINT"),
    ]),
    "PROJEKT_VYKONAVANIE": ("vykonavanie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("vykonavanie_id", "vykonavanie.id", "BIGINT"),
    ]),
    "PROJEKT_ZMENAPROJEKT": ("zmenaProjekt", [
        ("id", "id", "BIGINT"),
        ("cislododatku", "cisloDodatku", "VARCHAR"),
        ("predmet", "predmet", "VARCHAR"),
        ("datumplatnosti", "datumPlatnosti", "BIGINT"),
        ("datumucinnosti", "datumUcinnosti", "BIGINT"),
        ("url", "url", "VARCHAR"),
    ]),
}


TABLE_COMMENTS: dict[str, str] = {
    TABLE_CURRENT: (
        "Flat summary of every funded project, one row per project id; feeds "
        "docs/project_data.json for the projects.html page. Unlike "
        "itms21_programs_current, this table is NOT refreshed on every run - "
        "a row is only ever written once, when the project's detail is "
        "first fetched (insertion is gated by the same 'id not yet known' "
        "check as itms21_projekt), so its status/amounts are only as fresh "
        "as that single fetch."
    ),
    TABLE_PROJEKT: (
        "One row per funded project ('projekt'), created once a grant "
        "application ('zonfp') is approved. Purely additive: once an id is "
        "stored it is never re-fetched, updated, or deleted."
    ),
    _t("PROJEKT_AKTIVITY"): "Project activities carried out within the project.",
    _t("PROJEKT_CIELOVASKUPINA"): "Target groups declared for the project.",
    _t("PROJEKT_DODAVATEL"): "Suppliers/contractors engaged by the project.",
    _t("PROJEKT_FINANCNYPLAN"): "Financial plan line items for the project, split by financing source (e.g. EU fund vs. national budget).",
    _t("PROJEKT_FORMAPODPORY"): "Forms of support used by the project, by region category and specific objective.",
    _t("PROJEKT_HOSPODARSKACINNOST"): "Economic activities (NACE-style classification) declared for the project, by region category and specific objective.",
    _t("PROJEKT_INEUDAJE"): "Additional data items ('ine udaje') recorded for the project, together with the related entity's role and validity period.",
    _t("PROJEKT_INTENZITY"): "Aid intensity records for the project.",
    _t("PROJEKT_KATEGORIAREGIONOV"): "Region categories the project falls under.",
    _t("PROJEKT_MAKROREGIONALNASTRATEGIAASTRATEGIAPREMORSKEOBLASTI"): (
        "Macro-regional strategies and sea-basin strategies (e.g. EUSDR) the project contributes to, by region category and specific objective."
    ),
    _t("PROJEKT_MIESTOREALIZACIE"): "Places of implementation for the project.",
    _t("PROJEKT_MIESTOREALIZACIEFULL"): "Detailed places of implementation (NUTS levels, municipality/district) for the project.",
    _t("PROJEKT_MONITOROVACIETERMINY"): "Monitoring report deadlines defined for the project.",
    _t("PROJEKT_OBLASTINTERVENCIE"): "Intervention fields (EU classification dimension) the project targets, by region category and specific objective.",
    _t("PROJEKT_OPATRENIE"): "Measure(s) the project is funded under.",
    _t("PROJEKT_ORGANIZACNEZLOZKY"): "Organisational units of the beneficiary involved in implementing the project.",
    _t("PROJEKT_PARTNER"): "Project partners.",
    _t("PROJEKT_POLOZKYROZPOCTU"): "Approved budget items for the project.",
    _t("PROJEKT_PREDCHODCA"): "Links between this project and a predecessor/successor project (e.g. after a change/succession).",
    _t("PROJEKT_PROJEKTOVYZAMERIUS"): "Project intention ('projektovy zamer IUS') linked to the project.",
    _t("PROJEKT_RODOVAROVNOST"): "Gender-equality classifications tagged on the project, by region category and specific objective.",
    _t("PROJEKT_SEKUNDARNYTEMATICKYOKRUH"): "Secondary thematic focus areas tagged on the project, by region category and specific objective.",
    _t("PROJEKT_SPECIFICKYCIELPROGRAMU"): "Specific objective(s) the project contributes to.",
    _t("PROJEKT_TYPAKCIE"): "Types of action carried out by the project, by region category and specific objective.",
    _t("PROJEKT_TYPINTERVENCIE"): "Types of intervention used by the project, by region category and specific objective.",
    _t("PROJEKT_UKAZOVATELVYSLEDKU"): "Result indicator targets for the project, by region category and specific objective.",
    _t("PROJEKT_UKAZOVATELVYSTUPU"): "Output indicator targets for the project, by region category and specific objective.",
    _t("PROJEKT_URCITATEMA"): "Specific thematic tags on the project, by region category and specific objective.",
    _t("PROJEKT_UZEMNYMECHANIZMUSAZAMERANIE"): "Territorial mechanisms and focus (e.g. ITI, CLLD) used by the project, by region category and specific objective.",
    _t("PROJEKT_VYKONAVANIE"): "Implementation modes tagged on the project, by region category and specific objective.",
    _t("PROJEKT_ZMENAPROJEKT"): "Approved changes to the project (e.g. contract amendments).",
    _t("PROJEKT_ZMENAPROJEKT_DOKUMENT"): (
        "Documents attached to a project change/amendment. Grandchild rows carrying both project_id and "
        "zmenaprojekt_id back to itms21_projekt and itms21_projekt_zmenaprojekt; inserted once when the "
        "parent detail is first fetched, never updated/deleted."
    ),
    _t("PROJEKT_ZMLUVAPROJEKT_DOKUMENT"): (
        "Documents attached to the project's funding contract ('zmluva o poskytnutie NFP'). Grandchild rows "
        "carrying only project_id (the contract itself is flattened onto itms21_projekt, not its own table) "
        "back to itms21_projekt; inserted once when the parent detail is first fetched, never updated/deleted."
    ),
}
for _child_table in CHILD_TABLES:
    TABLE_COMMENTS[_t(_child_table)] += (
        " Child rows carrying project_id back to itms21_projekt; inserted "
        "once when the parent detail is first fetched, never updated/deleted."
    )

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_CURRENT: {
        "project_id": "ITMS21 numeric id of the project.",
        "kod": "Project code.",
        "nazov": "Project name.",
        "program_skratka": "Abbreviation of the parent programme.",
        "program_nazov": "Name of the parent programme in Slovak.",
        "prijimatel_nazov": "Name of the beneficiary (recipient) implementing the project.",
        "prijimatel_ico": "Company registration number (ICO) of the beneficiary.",
        "stav": "Status of the project at the time it was fetched.",
        "vrealizacii": "Whether the project was being implemented at the time it was fetched.",
        "ukonceny": "Whether the project was completed at the time it was fetched.",
        "suma_eu": "EU-fund contribution, computed from the project's financial plan, in euro.",
        "suma_sr": "Slovak national co-financing contribution, computed from the project's financial plan, in euro.",
        "suma_spolu": "Total contribution (EU + national), in euro.",
        "celkova_zazmluvnena_suma": "Total contracted project value at the time it was fetched, in euro.",
        "poskytnute_prostriedky": "Funds disbursed to the project at the time it was fetched, in euro.",
        "planovany_zaciatok": "Planned implementation start date (epoch milliseconds).",
        "planovany_koniec": "Planned implementation end date (epoch milliseconds).",
        "created_at": "Record creation timestamp in the source system (epoch milliseconds).",
        "updated_at": "Record last-updated timestamp in the source system, at the time it was fetched (epoch milliseconds).",
    },
    TABLE_PROJEKT: {
        "id": "ITMS21 numeric id of the project.",
        "href": "API URL of this project's own detail resource.",
        "kod": "Project code.",
        "nazov": "Project name.",
        "akronym": "Project acronym.",
        "stav": "Current status of the project.",
        "mrk": "Marginalized Roma community ('marginalizovana romska komunita', MRK) indicator/code for the project, when it targets this focus area.",
        "zameranieprojektu": "Project focus/orientation.",
        "ucel": "Stated purpose of the project.",
        "popis": "Project description.",
        "popiskapacityprijimatela": "Description of the beneficiary's implementation capacity.",
        "popissituacieporealizacii": "Description of the expected situation after project implementation.",
        "popissposoburealizacie": "Description of the project's implementation approach.",
        "popisvychodiskovejsituacie": "Description of the baseline situation before the project.",
        "celkovazazmluvnenasuma": "Total contracted project value, in euro.",
        "celkovazazmluvnenasumapovodna": "Original total contracted project value before amendments, in euro.",
        "zazmluvnenasumanfp": "Contracted grant amount (NFP), in euro.",
        "poskytnuteprostriedky": "Funds disbursed to the project so far, in euro.",
        "datumzaciatkuhlavnychaktivit": "Start date of the main activities (epoch milliseconds).",
        "datumkoncahlavnychaktivit": "End date of the main activities (epoch milliseconds).",
        "dlzkacelkovahlavnychaktivit": "Total duration of the main activities, in months.",
        "dlzkacelkovaprojektu": "Total project duration, in months.",
        "planovanarealizaciazaciatok": "Planned implementation start date (epoch milliseconds).",
        "planovanarealizaciakoniec": "Planned implementation end date (epoch milliseconds).",
        "skutocnarealizaciazaciatok": "Actual implementation start date (epoch milliseconds).",
        "skutocnarealizaciakoniec": "Actual implementation end date (epoch milliseconds).",
        "jeotvorenazmena": "Whether an open/pending change request exists for the project.",
        "makategoriuregionov": "Whether the project has a region category assigned.",
        "mimoriadneukoncenyneprispel": "Whether the project was terminated early without receiving EU contribution.",
        "mimoriadneukoncenyprispel": "Whether the project was terminated early but still received EU contribution.",
        "obsahujemiestorealizaciezahranicie": "Whether the project's implementation includes a place abroad.",
        "realizaciaaktivitpozastavena": "Whether implementation of the project's activities is currently suspended.",
        "udrzatelnyrozvojmiest": "Whether the project supports sustainable urban development.",
        "ukonceny": "Whether the project is completed.",
        "vrealizacii": "Whether the project is currently being implemented.",
        "vylucenyzfinancovania": "Whether the project has been excluded from financing.",
        "prijimatel_id": "Id of the beneficiary (recipient) implementing the project.",
        "program_id": "Id of the parent programme this project belongs to.",
        "vyzva_id": "Id of the call this project was funded under (itms21_vyzva.id).",
        "zonfp_id": "Id of the grant application this project originated from (itms21_zonfp.id).",
        "poskytovatelorgan_id": "Id of the provider/managing body organ responsible for the project.",
        "poskytovatelorgan_kod": "Code of the provider/managing body organ.",
        "poskytovatelorgan_nazov": "Name of the provider/managing body organ.",
        "poskytovatelsubjekt_id": "Id of the provider entity/institution.",
        "vyhlasovatelorgan_id": "Id of the organ that announced the call the project was funded under.",
        "vyhlasovatelorgan_kod": "Code of the organ that announced the call.",
        "vyhlasovatelorgan_nazov": "Name of the organ that announced the call.",
        "zmluvaprojekt_id": "Id of the project's funding contract ('zmluva o poskytnutie NFP').",
        "zmluvaprojekt_cislo": "Contract number of the project's funding contract.",
        "zmluvaprojekt_datumplatnosti": "Validity date of the project's funding contract (epoch milliseconds).",
        "zmluvaprojekt_datumucinnosti": "Effective date of the project's funding contract (epoch milliseconds).",
        "zmluvaprojekt_url": "URL of the published funding contract document.",
        "createdat": "Record creation timestamp in the source system (epoch milliseconds).",
        "updatedat": "Record last-updated timestamp in the source system (epoch milliseconds).",
    },
    _t("PROJEKT_AKTIVITY"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the project activity (itms21_aktivitaprojekt.id).",
    },
    _t("PROJEKT_CIELOVASKUPINA"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the target group.",
    },
    _t("PROJEKT_DODAVATEL"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "dic": "Supplier's tax identification number (DIC).",
        "ico": "Supplier's company registration number (ICO).",
        "ineidentifikacnecislo": "Supplier's other identification number, when it has neither a DIC nor an ICO.",
        "nazov": "Supplier name.",
    },
    _t("PROJEKT_FINANCNYPLAN"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "suma": "Amount for this financing-source line, in euro.",
        "zdroj_id": "Id of the financing source (e.g. EU fund vs. national budget).",
    },
    _t("PROJEKT_FORMAPODPORY"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "formapodpory_id": "Id of the form of support (e.g. grant, refundable assistance).",
        "kategoriaregionov_id": "Id of the region category this form of support applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this form of support applies to.",
    },
    _t("PROJEKT_HOSPODARSKACINNOST"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "hospodarskacinnost_id": "Id of the declared economic activity (NACE-style classification).",
        "kategoriaregionov_id": "Id of the region category this economic activity applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this economic activity applies to.",
    },
    _t("PROJEKT_INEUDAJE"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "ineudajesc_ineudaje_kod": "Code of the additional data item.",
        "ineudajesc_ineudaje_mernajednotka_id": "Id of the unit of measurement for the additional data item.",
        "ineudajesc_ineudaje_nazovde": "Additional data item name in German.",
        "ineudajesc_ineudaje_nazoven": "Additional data item name in English.",
        "ineudajesc_ineudaje_nazovsk": "Additional data item name in Slovak.",
        "ineudajesc_kategoriaregionov_id": "Id of the region category this additional data item applies to.",
        "ineudajesc_specifickycielprogramu_id": "Id of the specific objective this additional data item applies to.",
        "subjektnaprojekt_platnostdo": "End of the related entity's validity period on the project (epoch milliseconds).",
        "subjektnaprojekt_platnostod": "Start of the related entity's validity period on the project (epoch milliseconds).",
        "subjektnaprojekt_rola": "Role of the related entity on the project.",
        "subjektnaprojekt_subjekt_id": "Id of the related entity/institution.",
    },
    _t("PROJEKT_INTENZITY"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the aid intensity record.",
    },
    _t("PROJEKT_KATEGORIAREGIONOV"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the region category.",
    },
    _t("PROJEKT_MAKROREGIONALNASTRATEGIAASTRATEGIAPREMORSKEOBLASTI"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "kategoriaregionov_id": "Id of the region category this strategy association applies to.",
        "makroregionalnastrategiaastrategiapremorskeoblasti_id": "Id of the macro-regional strategy or sea-basin strategy (e.g. EUSDR).",
        "specifickycielprogramu_id": "Id of the specific objective this strategy association applies to.",
    },
    _t("PROJEKT_MIESTOREALIZACIE"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the place of implementation.",
    },
    _t("PROJEKT_MIESTOREALIZACIEFULL"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "lokalita": "Free-text description of the location.",
        "nuts2_id": "Id of the NUTS2 region classification.",
        "nuts3_id": "Id of the NUTS3 region classification.",
        "nuts4_id": "Id of the NUTS4 region classification.",
        "nuts5_id": "Id of the NUTS5 region classification.",
        "obecmimoeu": "Municipality name, when the place of implementation is outside the EU.",
        "okresmimoeu": "District name, when the place of implementation is outside the EU.",
        "samospravnykrajmimoeu": "Self-governing region name, when the place of implementation is outside the EU.",
        "stat_id": "Id of the country of implementation.",
    },
    _t("PROJEKT_MONITOROVACIETERMINY"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the monitoring deadline record.",
        "datumpredlozenianajneskorsi": "Latest allowed submission date for this monitoring report (epoch milliseconds).",
        "poradovecislo": "Ordering position of this monitoring deadline within the project.",
        "terminmonitorovania": "Monitoring period date this report covers (epoch milliseconds).",
        "typmonitorovania": "Type of monitoring report (e.g. periodic, final).",
    },
    _t("PROJEKT_OBLASTINTERVENCIE"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "kategoriaregionov_id": "Id of the region category this intervention field applies to.",
        "oblastintervencie_id": "Id of the intervention field (EU classification dimension code).",
        "specifickycielprogramu_id": "Id of the specific objective this intervention field applies to.",
    },
    _t("PROJEKT_OPATRENIE"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the measure the project is funded under (itms21_program_opatrenie.id).",
    },
    _t("PROJEKT_ORGANIZACNEZLOZKY"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the organisational unit.",
        "nazov": "Organisational unit name.",
        "adresa_ulica": "Street of the organisational unit's address.",
        "adresa_cislo": "Street/building number of the organisational unit's address.",
        "adresa_psc": "Postal code of the organisational unit's address.",
        "adresa_obec": "Municipality of the organisational unit's address.",
        "adresa_stat_id": "Id of the country of the organisational unit's address.",
    },
    _t("PROJEKT_PARTNER"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the project partner.",
    },
    _t("PROJEKT_POLOZKYROZPOCTU"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the approved budget item.",
    },
    _t("PROJEKT_PREDCHODCA"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "platnostnaslednikaod": "Start date of the successor's validity (epoch milliseconds).",
        "platnostpredchodcudo": "End date of the predecessor's validity (epoch milliseconds).",
        "naslednik_rola": "Role of the successor project in the relationship.",
        "naslednik_platnostod": "Start date of the successor project's validity (epoch milliseconds).",
        "naslednik_platnostdo": "End date of the successor project's validity (epoch milliseconds).",
        "naslednik_subjekt_id": "Id of the successor project's entity.",
        "predchodca_rola": "Role of the predecessor project in the relationship.",
        "predchodca_platnostod": "Start date of the predecessor project's validity (epoch milliseconds).",
        "predchodca_platnostdo": "End date of the predecessor project's validity (epoch milliseconds).",
        "predchodca_subjekt_id": "Id of the predecessor project's entity.",
    },
    _t("PROJEKT_PROJEKTOVYZAMERIUS"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the linked project intention ('projektovy zamer IUS').",
    },
    _t("PROJEKT_RODOVAROVNOST"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "kategoriaregionov_id": "Id of the region category this gender-equality tag applies to.",
        "rodovarovnost_id": "Id of the gender-equality classification.",
        "specifickycielprogramu_id": "Id of the specific objective this gender-equality tag applies to.",
    },
    _t("PROJEKT_SEKUNDARNYTEMATICKYOKRUH"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "kategoriaregionov_id": "Id of the region category this thematic focus applies to.",
        "sekundarnytematickyokruh_id": "Id of the secondary thematic focus area.",
        "specifickycielprogramu_id": "Id of the specific objective this thematic focus applies to.",
    },
    _t("PROJEKT_SPECIFICKYCIELPROGRAMU"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the specific objective the project contributes to (itms21_program_specifickycielprogramu.id).",
    },
    _t("PROJEKT_TYPAKCIE"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "kategoriaregionov_id": "Id of the region category this type of action applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this type of action applies to.",
        "typakcie_id": "Id of the type of action.",
    },
    _t("PROJEKT_TYPINTERVENCIE"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "kategoriaregionov_id": "Id of the region category this type of intervention applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this type of intervention applies to.",
        "typintervencie_id": "Id of the type of intervention.",
    },
    _t("PROJEKT_UKAZOVATELVYSLEDKU"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "cielovahodnotaspolu": "Total target value of the result indicator.",
        "vychodiskovahodnotaspolu": "Total baseline value of the result indicator.",
        "ukazovatelprojektovysc_id": "Id of the underlying project-level result-indicator definition.",
        "ukazovatelprojektovysc_kategoriaregionov_id": "Id of the region category this indicator definition applies to.",
        "ukazovatelprojektovysc_specifickycielprogramu_id": "Id of the specific objective this indicator definition applies to.",
        "ukazovatelprojektovysc_ukazovatelprojektovy_id": "Id of the underlying project-level indicator definition.",
    },
    _t("PROJEKT_UKAZOVATELVYSTUPU"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "cielovahodnotaspolu": "Total target value of the output indicator.",
        "vychodiskovahodnotaspolu": "Total baseline value of the output indicator.",
        "ukazovatelprojektovysc_id": "Id of the underlying project-level output-indicator definition.",
        "ukazovatelprojektovysc_kategoriaregionov_id": "Id of the region category this indicator definition applies to.",
        "ukazovatelprojektovysc_specifickycielprogramu_id": "Id of the specific objective this indicator definition applies to.",
        "ukazovatelprojektovysc_ukazovatelprojektovy_id": "Id of the underlying project-level indicator definition.",
    },
    _t("PROJEKT_URCITATEMA"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "kategoriaregionov_id": "Id of the region category this thematic tag applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this thematic tag applies to.",
        "urcitatema_id": "Id of the specific theme.",
    },
    _t("PROJEKT_UZEMNYMECHANIZMUSAZAMERANIE"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "kategoriaregionov_id": "Id of the region category this territorial mechanism applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this territorial mechanism applies to.",
        "uzemnymechanizmusazameranie_id": "Id of the territorial mechanism and focus (e.g. ITI, CLLD).",
    },
    _t("PROJEKT_VYKONAVANIE"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "kategoriaregionov_id": "Id of the region category this implementation mode applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this implementation mode applies to.",
        "vykonavanie_id": "Id of the implementation mode.",
    },
    _t("PROJEKT_ZMENAPROJEKT"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "id": "Id of the project change/amendment.",
        "cislododatku": "Amendment (dodatok) number.",
        "predmet": "Subject of the change.",
        "datumplatnosti": "Validity date of the change (epoch milliseconds).",
        "datumucinnosti": "Effective date of the change (epoch milliseconds).",
        "url": "URL of the published amendment document.",
    },
    _t("PROJEKT_ZMENAPROJEKT_DOKUMENT"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "zmenaprojekt_id": "Id of the parent project change/amendment (itms21_projekt_zmenaprojekt.id).",
        "nazov": "Document name.",
        "uuid": "Document's file identifier (uuid), used to build its download URL.",
    },
    _t("PROJEKT_ZMLUVAPROJEKT_DOKUMENT"): {
        "project_id": "Id of the parent project (itms21_projekt.id).",
        "nazov": "Document name.",
        "uuid": "Document's file identifier (uuid), used to build its download URL.",
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
    bare_names = ["PROJEKT"] + list(CHILD_TABLES.keys()) + [
        "PROJEKT_ZMENAPROJEKT_DOKUMENT",
        "PROJEKT_ZMLUVAPROJEKT_DOKUMENT",
    ]
    for name in bare_names:
        con.execute(f"ALTER TABLE IF EXISTS {name} RENAME TO {_t(name)}")
        # Also catch a table already prefixed but with the old ALL-CAPS name
        # (e.g. from a run of this script before lowercasing was added).
        con.execute(f"ALTER TABLE IF EXISTS {TABLE_PREFIX}{name} RENAME TO {_t(name)}")

    if _table_exists(con, TABLE_PROJEKT):
        _rename_columns(con, TABLE_PROJEKT, [col for col, _, _ in PROJEKT_COLUMNS])

    for table, (_, columns) in CHILD_TABLES.items():
        full = _t(table)
        if _table_exists(con, full):
            con.execute(f"ALTER TABLE {full} RENAME COLUMN PROJECT_ID TO project_id")
            _rename_columns(con, full, [col for col, _, _ in columns])

    if _table_exists(con, _t("PROJEKT_ZMENAPROJEKT_DOKUMENT")):
        full = _t("PROJEKT_ZMENAPROJEKT_DOKUMENT")
        con.execute(f"ALTER TABLE {full} RENAME COLUMN PROJECT_ID TO project_id")
        con.execute(f"ALTER TABLE {full} RENAME COLUMN ZMENAPROJEKT_ID TO zmenaprojekt_id")
        _rename_columns(con, full, ["nazov", "uuid"])

    if _table_exists(con, _t("PROJEKT_ZMLUVAPROJEKT_DOKUMENT")):
        full = _t("PROJEKT_ZMLUVAPROJEKT_DOKUMENT")
        con.execute(f"ALTER TABLE {full} RENAME COLUMN PROJECT_ID TO project_id")
        _rename_columns(con, full, ["nazov", "uuid"])


def ensure_full_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create PROJEKT and every PROJEKT_* child/grandchild table if missing."""
    migrate_table_prefix(con)

    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in PROJEKT_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_PROJEKT} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                project_id BIGINT,
                {cols_sql}
            )
        """)

    # Two grandchild tables: a "dokument" list living one level deeper than a
    # child-table row (PROJEKT_ZMENAPROJEKT) or the flattened PROJEKT row
    # (zmluvaProjekt). Both carry project_id so they join back to PROJEKT.
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {_t("PROJEKT_ZMENAPROJEKT_DOKUMENT")} (
            project_id BIGINT,
            zmenaprojekt_id BIGINT,
            nazov VARCHAR,
            uuid VARCHAR
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {_t("PROJEKT_ZMLUVAPROJEKT_DOKUMENT")} (
            project_id BIGINT,
            nazov VARCHAR,
            uuid VARCHAR
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
            [project_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (project_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )

    zmena_doc_rows = []
    for item in detail.get("zmenaProjekt") or []:
        zmena_id = item.get("id")
        for doc in item.get("dokument") or []:
            zmena_doc_rows.append([project_id, zmena_id, doc.get("nazov"), doc.get("uuid")])
    if zmena_doc_rows:
        con.executemany(
            f"INSERT INTO {_t('PROJEKT_ZMENAPROJEKT_DOKUMENT')} (project_id, zmenaprojekt_id, nazov, uuid) "
            "VALUES (?, ?, ?, ?)",
            zmena_doc_rows,
        )

    zmluva_docs = (detail.get("zmluvaProjekt") or {}).get("dokument") or []
    if zmluva_docs:
        con.executemany(
            f"INSERT INTO {_t('PROJEKT_ZMLUVAPROJEKT_DOKUMENT')} (project_id, nazov, uuid) VALUES (?, ?, ?)",
            [[project_id, doc.get("nazov"), doc.get("uuid")] for doc in zmluva_docs],
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored (both the simple summary and the full detail
    schema are written together), so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_PROJEKT}").fetchall()
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
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_table(con)
    ensure_full_schema(con)
    apply_comments(con)

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
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    apply_comments(con)
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