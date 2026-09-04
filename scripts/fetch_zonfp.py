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

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

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


TABLE_COMMENTS: dict[str, str] = {
    TABLE_ZONFP: (
        "One row per grant application ('ziadost o nenavratny financny "
        "prispevok', ZoNFP) submitted under a call for proposals. Purely "
        "additive: once an id is stored it is never re-fetched, updated, or "
        "deleted."
    ),
    _t("aktivity"): "Requested project activities included in the grant application.",
    _t("aktivityschvalene"): "Approved project activities under the grant application, after assessment.",
    _t("cielovaskupina"): "Target groups declared in the grant application.",
    _t("dokumenthodnotenia"): "Evaluation documents attached to the grant application's assessment.",
    _t("formapodpory"): "Forms of support requested in the grant application, by region category and specific objective.",
    _t("hodnotitel"): "Evaluators assigned to assess the grant application.",
    _t("hospodarskacinnost"): "Economic activities (NACE-style classification) declared in the grant application, by region category and specific objective.",
    _t("kategoriaregionov"): "Region categories the grant application falls under.",
    _t("makroregionalnastrategiaastrategiapremorskeoblasti"): (
        "Macro-regional strategies and sea-basin strategies (e.g. EUSDR) the grant application contributes to, by region category and specific objective."
    ),
    _t("miestorealizacie"): "Places of implementation declared in the grant application.",
    _t("miestorealizaciefull"): "Detailed places of implementation (NUTS levels, municipality/district) declared in the grant application.",
    _t("opatrenie"): "Measure(s) the grant application is submitted under.",
    _t("organizacnezlozky"): "Organisational units of the applicant involved in implementing the project.",
    _t("partner"): "Project partners declared in the grant application.",
    _t("polozkyrozpoctu"): "Requested budget items in the grant application.",
    _t("polozkyrozpoctuschvalene"): "Approved budget items in the grant application, after assessment.",
    _t("predchodca"): "Link between this grant application and a predecessor/successor application (e.g. a resubmission).",
    _t("projektovyzamerius"): "Project intention ('projektovy zamer IUS') linked to the grant application.",
    _t("rodovarovnost"): "Gender-equality classifications tagged on the grant application, by region category and specific objective.",
    _t("sekundarnytematickyokruh"): "Secondary thematic focus areas tagged on the grant application, by region category and specific objective.",
    _t("specifickycielprogramu"): "Specific objective(s) the grant application contributes to.",
    _t("typakcie"): "Types of action declared in the grant application, by region category and specific objective.",
    _t("typintervencie"): "Types of intervention declared in the grant application, by region category and specific objective.",
    _t("ukazovatelschvalenyvysledku"): "Approved result-indicator targets for the grant application, after assessment.",
    _t("ukazovatelschvalenyvystupu"): "Approved output-indicator targets for the grant application, after assessment.",
    _t("ukazovatelziadanyvysledku"): "Requested result-indicator targets in the grant application.",
    _t("ukazovatelziadanyvystupu"): "Requested output-indicator targets in the grant application.",
    _t("urcitatema"): "Specific thematic tags on the grant application, by region category and specific objective.",
    _t("uzemnymechanizmusazameranie"): "Territorial mechanisms and focus (e.g. ITI, CLLD) used by the grant application, by region category and specific objective.",
    _t("vykonavanie"): "Implementation modes tagged on the grant application, by region category and specific objective.",
}
for _child_table in CHILD_TABLES:
    TABLE_COMMENTS[_t(_child_table)] += (
        " Child rows carrying zonfp_id back to itms21_zonfp; inserted once "
        "when the parent detail is first fetched, never updated/deleted."
    )

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_ZONFP: {
        "id": "ITMS21 numeric id of the grant application.",
        "href": "API URL of this grant application's own detail resource.",
        "kod": "Grant application code.",
        "nazov": "Project name as submitted in the grant application.",
        "akronym": "Project acronym.",
        "mrk": "Marginalized Roma community ('marginalizovana romska komunita', MRK) indicator/code for the grant application, when it targets this focus area.",
        "zameranieprojektu": "Project focus/orientation.",
        "popis": "Project description.",
        "ucel": "Stated purpose of the project.",
        "stav": "Current status of the grant application (e.g. submitted, approved, rejected).",
        "datumvytvorenia": "Creation date of the application.",
        "datumpredlozenia": "Submission date of the application.",
        "datumregistracie": "Registration date of the application (epoch milliseconds).",
        "datumcasodoslania": "Date/time the application was sent (epoch milliseconds).",
        "datumodoslania": "Date the application was sent.",
        "datumschvalenia": "Approval date of the application (epoch milliseconds).",
        "datumzamietnutia": "Rejection date of the application (epoch milliseconds).",
        "datumziadanyzaciatkuhlavnychaktivit": "Requested start date of the main activities (epoch milliseconds).",
        "datumziadanykoncahlavnychaktivit": "Requested end date of the main activities (epoch milliseconds).",
        "datumschvalenyzaciatkuhlavnychaktivit": "Approved start date of the main activities (epoch milliseconds).",
        "datumschvalenykoncahlavnychaktivit": "Approved end date of the main activities (epoch milliseconds).",
        "dlzkaziadanacelkovaprojektu": "Requested total project duration, in months.",
        "dlzkaziadanacelkovahlavnychaktivit": "Requested total duration of the main activities, in months.",
        "dlzkaschvalenacelkovaprojektu": "Approved total project duration, in months.",
        "dlzkaschvalenacelkovahlavnychaktivit": "Approved total duration of the main activities, in months.",
        "percetnoziadane": "Requested co-financing rate (percentage).",
        "percetnoschvalene": "Approved co-financing rate (percentage).",
        "pocetbodovhodnoteniacelkovy": "Total evaluation score received.",
        "sumaziadananfp": "Requested grant amount (NFP), in euro.",
        "sumaziadanavz": "Requested own-resources amount (VZ), in euro.",
        "sumaziadanacelkova": "Total requested project value, in euro.",
        "sumaschvalenanfp": "Approved grant amount (NFP), in euro.",
        "sumaschvalenavz": "Approved own-resources amount (VZ), in euro.",
        "sumaschvalenacelkova": "Total approved project value, in euro.",
        "schvalenavyskanfp": "Final approved grant (NFP) amount, in euro.",
        "schvalena": "Whether the application has been approved.",
        "neschvalena": "Whether the application has been rejected.",
        "vylucenazfinancovania": "Whether the application has been excluded from financing.",
        "osobitnyproceskonania": "Whether the application follows a special assessment procedure.",
        "obsahujemiestorealizaciezahranicie": "Whether the application includes a place of implementation abroad.",
        "makategoriuregionov": "Whether the application has a region category assigned.",
        "udrzatelnyrozvojmiest": "Whether the application supports sustainable urban development.",
        "posudzovaneobdobie_datumuzavierky": "Submission deadline of the assessment period the application was submitted under (epoch milliseconds).",
        "posudzovaneobdobie_poradovecislo": "Ordering position of that assessment period within the call.",
        "program_id": "Id of the parent programme this application belongs to.",
        "projekt_id": "Id of the project this application became once approved (itms21_projekt.id).",
        "vyzva_id": "Id of the parent call this application was submitted under (itms21_vyzva.id).",
        "ziadatel_id": "Id of the applicant entity.",
        "createdat": "Record creation timestamp in the source system.",
        "updatedat": "Record last-updated timestamp in the source system.",
    },
    _t("aktivity"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "id": "Id of the requested project activity.",
    },
    _t("aktivityschvalene"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "id": "Id of the approved project activity.",
    },
    _t("cielovaskupina"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "id": "Id of the target group.",
    },
    _t("dokumenthodnotenia"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "nazov": "Evaluation document name.",
        "uuid": "Evaluation document's file identifier (uuid), used to build its download URL.",
    },
    _t("formapodpory"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "formapodpory_id": "Id of the requested form of support.",
        "kategoriaregionov_id": "Id of the region category this form of support applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this form of support applies to.",
    },
    _t("hodnotitel"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "osoba_meno": "Evaluator's first name.",
        "osoba_menouplne": "Evaluator's full name.",
        "osoba_priezvisko": "Evaluator's surname.",
        "osoba_titulpred": "Evaluator's academic title placed before the name.",
        "osoba_titulza": "Evaluator's academic title placed after the name.",
        "program_id": "Id of the programme the evaluator is assigned under.",
    },
    _t("hospodarskacinnost"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "hospodarskacinnost_id": "Id of the declared economic activity (NACE-style classification).",
        "kategoriaregionov_id": "Id of the region category this economic activity applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this economic activity applies to.",
    },
    _t("kategoriaregionov"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "id": "Id of the region category.",
    },
    _t("makroregionalnastrategiaastrategiapremorskeoblasti"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "kategoriaregionov_id": "Id of the region category this strategy association applies to.",
        "makroregionalnastrategiaastrategiapremorskeoblasti_id": "Id of the macro-regional strategy or sea-basin strategy (e.g. EUSDR).",
        "specifickycielprogramu_id": "Id of the specific objective this strategy association applies to.",
    },
    _t("miestorealizacie"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "id": "Id of the place of implementation.",
    },
    _t("miestorealizaciefull"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
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
    _t("opatrenie"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "id": "Id of the measure the application is submitted under (itms21_program_opatrenie.id).",
    },
    _t("organizacnezlozky"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "id": "Id of the organisational unit.",
        "nazov": "Organisational unit name.",
        "adresa_ulica": "Street of the organisational unit's address.",
        "adresa_cislo": "Street/building number of the organisational unit's address.",
        "adresa_psc": "Postal code of the organisational unit's address.",
        "adresa_obec": "Municipality of the organisational unit's address.",
        "adresa_stat_id": "Id of the country of the organisational unit's address.",
    },
    _t("partner"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "id": "Id of the project partner.",
    },
    _t("polozkyrozpoctu"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "id": "Id of the requested budget item.",
    },
    _t("polozkyrozpoctuschvalene"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "id": "Id of the approved budget item.",
    },
    _t("predchodca"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "naslednik_id": "Id of the successor grant application.",
        "platnostnaslednikaod": "Start date of the successor's validity (epoch milliseconds).",
        "platnostpredchodcudo": "End date of the predecessor's validity (epoch milliseconds).",
        "predchodca_id": "Id of the predecessor grant application.",
    },
    _t("projektovyzamerius"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "id": "Id of the linked project intention ('projektovy zamer IUS').",
    },
    _t("rodovarovnost"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "kategoriaregionov_id": "Id of the region category this gender-equality tag applies to.",
        "rodovarovnost_id": "Id of the gender-equality classification.",
        "specifickycielprogramu_id": "Id of the specific objective this gender-equality tag applies to.",
    },
    _t("sekundarnytematickyokruh"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "kategoriaregionov_id": "Id of the region category this thematic focus applies to.",
        "sekundarnytematickyokruh_id": "Id of the secondary thematic focus area.",
        "specifickycielprogramu_id": "Id of the specific objective this thematic focus applies to.",
    },
    _t("specifickycielprogramu"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "id": "Id of the specific objective the application contributes to (itms21_program_specifickycielprogramu.id).",
    },
    _t("typakcie"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "kategoriaregionov_id": "Id of the region category this type of action applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this type of action applies to.",
        "typakcie_id": "Id of the type of action.",
    },
    _t("typintervencie"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "kategoriaregionov_id": "Id of the region category this type of intervention applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this type of intervention applies to.",
        "typintervencie_id": "Id of the type of intervention.",
    },
    _t("ukazovatelschvalenyvysledku"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "cielovahodnotaspolu": "Total approved target value of the result indicator.",
        "vychodiskovahodnotaspolu": "Total approved baseline value of the result indicator.",
        "ukazovatelprojektovysc_id": "Id of the underlying project-level result-indicator definition.",
        "ukazovatelprojektovysc_kategoriaregionov_id": "Id of the region category this indicator definition applies to.",
        "ukazovatelprojektovysc_specifickycielprogramu_id": "Id of the specific objective this indicator definition applies to.",
        "ukazovatelprojektovysc_ukazovatelprojektovy_id": "Id of the underlying project-level indicator definition.",
    },
    _t("ukazovatelschvalenyvystupu"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "cielovahodnotaspolu": "Total approved target value of the output indicator.",
        "vychodiskovahodnotaspolu": "Total approved baseline value of the output indicator.",
        "ukazovatelprojektovysc_id": "Id of the underlying project-level output-indicator definition.",
        "ukazovatelprojektovysc_kategoriaregionov_id": "Id of the region category this indicator definition applies to.",
        "ukazovatelprojektovysc_specifickycielprogramu_id": "Id of the specific objective this indicator definition applies to.",
        "ukazovatelprojektovysc_ukazovatelprojektovy_id": "Id of the underlying project-level indicator definition.",
    },
    _t("ukazovatelziadanyvysledku"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "cielovahodnotaspolu": "Total requested target value of the result indicator.",
        "vychodiskovahodnotaspolu": "Total requested baseline value of the result indicator.",
        "ukazovatelprojektovysc_id": "Id of the underlying project-level result-indicator definition.",
        "ukazovatelprojektovysc_kategoriaregionov_id": "Id of the region category this indicator definition applies to.",
        "ukazovatelprojektovysc_specifickycielprogramu_id": "Id of the specific objective this indicator definition applies to.",
        "ukazovatelprojektovysc_ukazovatelprojektovy_id": "Id of the underlying project-level indicator definition.",
    },
    _t("ukazovatelziadanyvystupu"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "cielovahodnotaspolu": "Total requested target value of the output indicator.",
        "vychodiskovahodnotaspolu": "Total requested baseline value of the output indicator.",
        "ukazovatelprojektovysc_id": "Id of the underlying project-level output-indicator definition.",
        "ukazovatelprojektovysc_kategoriaregionov_id": "Id of the region category this indicator definition applies to.",
        "ukazovatelprojektovysc_specifickycielprogramu_id": "Id of the specific objective this indicator definition applies to.",
        "ukazovatelprojektovysc_ukazovatelprojektovy_id": "Id of the underlying project-level indicator definition.",
    },
    _t("urcitatema"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "kategoriaregionov_id": "Id of the region category this thematic tag applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this thematic tag applies to.",
        "urcitatema_id": "Id of the specific theme.",
    },
    _t("uzemnymechanizmusazameranie"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "kategoriaregionov_id": "Id of the region category this territorial mechanism applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this territorial mechanism applies to.",
        "uzemnymechanizmusazameranie_id": "Id of the territorial mechanism and focus (e.g. ITI, CLLD).",
    },
    _t("vykonavanie"): {
        "zonfp_id": "Id of the parent grant application (itms21_zonfp.id).",
        "kategoriaregionov_id": "Id of the region category this implementation mode applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this implementation mode applies to.",
        "vykonavanie_id": "Id of the implementation mode.",
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
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_full_schema(con)
    apply_comments(con)

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
