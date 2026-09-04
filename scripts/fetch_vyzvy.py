"""
Fetches the ITMS21 "vyzva" (call for proposals) list, then fetches full
detail ONLY for ids not yet stored in DuckDB. The list itself is not
persisted - it exists purely to discover ids. Ids already stored are never
re-fetched and never deleted, even if they disappear from the API list
(purely additive incremental sync, same approach as fetch_projects.py).

Each detail fetched decomposes into a normalized VYZVA table plus ~35
VYZVA_* child/grandchild tables (one row per inner list item), mirroring the
ITMS21 "vyzva (detail)" field-by-field mapping. Every child/grandchild table
carries VYZVA_ID so it can be joined back to VYZVA. Rows are inserted once
and never updated/deleted, gated by the same "id not yet known" check.

DuckDB-only: there is no JSON export / website page for this data.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/vyzva?limit=-1"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/vyzva/id/{id}"

DB_PATH = Path("data/eufunds.duckdb")  # shared DuckDB file, separate tables inside
DB_SCHEMA = "slovakia"  # dedicated schema inside the shared file

TABLE_PREFIX = "itms21_"
TABLE_VYZVA = f"{TABLE_PREFIX}VYZVA".lower()

MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

LIST_REQUEST_TIMEOUT = 180  # the list endpoint returns every vyzva in one response (limit=-1)


def fetch_list() -> list[dict]:
    """Call the list endpoint and return all vyzva summary records (used only
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


def fetch_detail(vyzva_id: int) -> dict | None:
    """Call the detail endpoint for one vyzva, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=vyzva_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={vyzva_id} after {RETRY_ATTEMPTS} attempts: {e}")
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
# Mirrors the ITMS21 "vyzva" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).

VYZVA_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "id", "BIGINT"),
    ("href", "href", "VARCHAR"),
    ("kod", "kod", "VARCHAR"),
    ("nazovsk", "nazovSk", "VARCHAR"),
    ("nazoven", "nazovEn", "VARCHAR"),
    ("nazovde", "nazovDe", "VARCHAR"),
    ("druh", "druh", "VARCHAR"),
    ("typ", "typ", "VARCHAR"),
    ("mrk", "mrk", "VARCHAR"),
    ("zameranieprojektu", "zameranieProjektu", "VARCHAR"),
    ("cielvyzvy", "cielVyzvy", "VARCHAR"),
    ("doplnujuceinformacie", "doplnujuceInformacie", "VARCHAR"),
    ("dovodvznikuverzie", "dovodVznikuVerzie", "VARCHAR"),
    ("inaskutocnostuzatvorenievyzvy", "inaSkutocnostUzatvorenieVyzvy", "VARCHAR"),
    ("datumvyhlasenia", "datumVyhlasenia", "BIGINT"),
    ("datumukoncenia", "datumUkoncenia", "BIGINT"),
    ("programoveobdobie", "programoveObdobie", "VARCHAR"),
    ("sposobpodaniazonfp", "sposobPodaniaZoNFP", "VARCHAR"),
    ("miestoprepodaniezonfp", "miestoPrePodanieZoNFP", "VARCHAR"),
    ("predpokladanalehotanarozhodnutie", "predpokladanaLehotaNaRozhodnutie", "VARCHAR"),
    ("minvyska", "minVyska", "VARCHAR"),
    ("maxvyska", "maxVyska", "VARCHAR"),
    ("minziadanavyskanfp", "minZiadanaVyskaNfp", "VARCHAR"),
    ("maxziadanavyskanfp", "maxZiadanaVyskaNfp", "VARCHAR"),
    ("maxmiera", "maxMiera", "VARCHAR"),
    ("mieraspolufinancovania", "mieraSpolufinancovania", "VARCHAR"),
    ("sumaeu", "sumaEu", "DOUBLE"),
    ("sumasr", "sumaSr", "DOUBLE"),
    ("pocetpredlozenychziadosti", "pocetPredlozenychZiadosti", "INTEGER"),
    ("pocetschvalenychziadosti", "pocetSchvalenychZiadosti", "INTEGER"),
    ("pocetneschvalenychziadosti", "pocetNeschvalenychZiadosti", "INTEGER"),
    ("pocetziadostivkonani", "pocetZiadostiVKonani", "INTEGER"),
    ("pocetrealizovanychprojektov", "pocetRealizovanychProjektov", "INTEGER"),
    ("obsahujemiestorealizaciezahranicie", "obsahujeMiestoRealizacieZahranicie", "BOOLEAN"),
    ("percentozapolozku", "percentoZaPolozku", "BOOLEAN"),
    ("povinnostvo", "povinnostVo", "BOOLEAN"),
    ("pozastavenepredkladaniezonfp", "pozastavenePredkladanieZonfp", "BOOLEAN"),
    ("predvyber", "predvyber", "BOOLEAN"),
    ("uzavreta", "uzavreta", "BOOLEAN"),
    ("vyhlasena", "vyhlasena", "BOOLEAN"),
    ("zamerpredlozeny", "zamerPredlozeny", "BOOLEAN"),
    ("zmenaazrusenievyzvy", "zmenaAZrusenieVyzvy", "VARCHAR"),
    ("zrusena", "zrusena", "BOOLEAN"),
    ("kontaktemail", "kontaktEmail", "VARCHAR"),
    ("kontaktnazov", "kontaktNazov", "VARCHAR"),
    ("kontakttelefon", "kontaktTelefon", "VARCHAR"),
    ("kontaktneudajeposkytovatela", "kontaktneUdajePoskytovatela", "VARCHAR"),
    ("poskytovatel_id", "poskytovatel.id", "BIGINT"),
    ("program_id", "program.id", "BIGINT"),
    ("vyhlasovatel_id", "vyhlasovatel.id", "BIGINT"),
    ("createdat", "createdAt", "VARCHAR"),
    ("updatedat", "updatedAt", "VARCHAR"),
]

# table_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "VYZVA_AKTUALITANAVYZVE": ("aktualitaNaVyzve", [
        ("datumzverejnenia", "datumZverejnenia", "BIGINT"),
        ("nazov", "nazov", "VARCHAR"),
        ("text", "text", "VARCHAR"),
    ]),
    "VYZVA_CIELOVASKUPINA": ("cielovaSkupina", [("id", "id", "BIGINT")]),
    "VYZVA_DALSIAFORMALNANALEZITOST": ("dalsiaFormalnaNalezitost", [
        ("dalsiaformalnanalezitost", "dalsiaFormalnaNalezitost", "VARCHAR"),
        ("kod", "kod", "VARCHAR"),
    ]),
    "VYZVA_DALSIASKUTOCNOST": ("dalsiaSkutocnost", [
        ("dalsiaskutocnost", "dalsiaSkutocnost", "VARCHAR"),
        ("kod", "kod", "VARCHAR"),
    ]),
    "VYZVA_DOKUMENT": ("dokument", [
        ("nazov", "nazov", "VARCHAR"),
        ("uuid", "uuid", "VARCHAR"),
    ]),
    "VYZVA_EXTERNYZDROJ": ("externyZdroj", [
        ("nazov", "nazov", "VARCHAR"),
        ("url", "url", "VARCHAR"),
    ]),
    "VYZVA_FOND": ("fond", [("id", "id", "BIGINT")]),
    "VYZVA_FORMAPODPORY": ("formaPodpory", [
        ("formapodpory_id", "formaPodpory.id", "BIGINT"),
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "VYZVA_HOSPODARSKACINNOST": ("hospodarskaCinnost", [
        ("hospodarskacinnost_id", "hospodarskaCinnost.id", "BIGINT"),
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "VYZVA_KATEGORIAREGIONOV": ("kategoriaRegionov", [("id", "id", "BIGINT")]),
    "VYZVA_KONTAKTNAOSOBA": ("kontaktnaOsoba", [
        ("email", "email", "VARCHAR"),
        ("osoba_meno", "osoba.meno", "VARCHAR"),
        ("osoba_menouplne", "osoba.menoUplne", "VARCHAR"),
        ("osoba_priezvisko", "osoba.priezvisko", "VARCHAR"),
        ("osoba_titulpred", "osoba.titulPred", "VARCHAR"),
        ("osoba_titulza", "osoba.titulZa", "VARCHAR"),
        ("telefon", "telefon", "VARCHAR"),
    ]),
    "VYZVA_MAKROREGIONALNASTRATEGIAASTRATEGIAPREMORSKEOBLASTI": (
        "makroregionalnaStrategiaAStrategiaPreMorskeOblasti", [
            ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
            ("makroregionalnastrategiaastrategiapremorskeoblasti_id",
             "makroregionalnaStrategiaAStrategiaPreMorskeOblasti.id", "BIGINT"),
            ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ]),
    "VYZVA_MIESTOREALIZACIE": ("miestoRealizacie", [("id", "id", "BIGINT")]),
    "VYZVA_MIESTOREALIZACIEFULL": ("miestoRealizacieFull", [("id", "id", "BIGINT")]),
    "VYZVA_MIESTOREALIZACIESTAT": ("miestoRealizacieStat", [("id", "id", "BIGINT")]),
    "VYZVA_OBLASTINTERVENCIE": ("oblastIntervencie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("oblastintervencie_id", "oblastIntervencie.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "VYZVA_OPATRENIE": ("opatrenie", [("id", "id", "BIGINT")]),
    "VYZVA_OPRAVNENEVYDAVKY": ("opravneneVydavky", [
        ("kod", "kod", "VARCHAR"),
        ("nazovde", "nazovDe", "VARCHAR"),
        ("nazoven", "nazovEn", "VARCHAR"),
        ("nazovsk", "nazovSk", "VARCHAR"),
        ("typakcieprogramu_id", "typAkcieProgramu.id", "BIGINT"),
    ]),
    "VYZVA_PARTNER": ("partner", [("id", "id", "BIGINT")]),
    "VYZVA_PLANOVANAVYZVA": ("planovanaVyzva", [("id", "id", "BIGINT")]),
    "VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU": ("podmienkaPoskytnutiaPrispevku", [
        ("nazovde", "nazovDe", "VARCHAR"),
        ("nazoven", "nazovEn", "VARCHAR"),
        ("nazovsk", "nazovSk", "VARCHAR"),
        ("popisde", "popisDe", "VARCHAR"),
        ("popisen", "popisEn", "VARCHAR"),
        ("popissk", "popisSk", "VARCHAR"),
        ("poradovecislo", "poradoveCislo", "INTEGER"),
        ("poznamka", "poznamka", "VARCHAR"),
    ]),
    "VYZVA_POSUDZOVANEOBDOBIE": ("posudzovaneObdobie", [
        ("datumuzavierky", "datumUzavierky", "BIGINT"),
        ("poradovecislo", "poradoveCislo", "INTEGER"),
    ]),
    "VYZVA_PROJEKTOVYZAMERIUS": ("projektovyZamerIUS", [("id", "id", "BIGINT")]),
    "VYZVA_RODOVAROVNOST": ("rodovaRovnost", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("rodovarovnost_id", "rodovaRovnost.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "VYZVA_SEKUNDARNYTEMATICKYOKRUH": ("sekundarnyTematickyOkruh", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("sekundarnytematickyokruh_id", "sekundarnyTematickyOkruh.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "VYZVA_SPECIFICKYCIELPROGRAMU": ("specifickyCielProgramu", [("id", "id", "BIGINT")]),
    "VYZVA_STATNAPOMOC": ("statnaPomoc", [
        ("kod", "kod", "VARCHAR"),
        ("nazov", "nazov", "VARCHAR"),
        ("platnostdo", "platnostDo", "BIGINT"),
        ("platnostod", "platnostOd", "BIGINT"),
        ("typ", "typ", "VARCHAR"),
    ]),
    "VYZVA_SUBJEKT": ("subjekt", [("id", "id", "BIGINT")]),
    "VYZVA_TYPAKCIE": ("typAkcie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("typakcie_id", "typAkcie.id", "BIGINT"),
    ]),
    "VYZVA_TYPAKCIEPROGRAMU": ("typAkcieProgramu", [("id", "id", "BIGINT")]),
    "VYZVA_TYPINTERVENCIE": ("typIntervencie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("typintervencie_id", "typIntervencie.id", "BIGINT"),
    ]),
    "VYZVA_UKAZOVATELVYSLEDKOVY": ("ukazovatelVysledkovy", [
        ("id", "id", "BIGINT"),
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("ukazovatelprojektovy_id", "ukazovatelProjektovy.id", "BIGINT"),
    ]),
    "VYZVA_UKAZOVATELVYSTUPOVY": ("ukazovatelVystupovy", [
        ("id", "id", "BIGINT"),
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("ukazovatelprojektovy_id", "ukazovatelProjektovy.id", "BIGINT"),
    ]),
    "VYZVA_URCITATEMA": ("urcitaTema", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("urcitatema_id", "urcitaTema.id", "BIGINT"),
    ]),
    "VYZVA_UZEMNYMECHANIZMUSAZAMERANIE": ("uzemnyMechanizmusAZameranie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("uzemnymechanizmusazameranie_id", "uzemnyMechanizmusAZameranie.id", "BIGINT"),
    ]),
    "VYZVA_VERZIA": ("verzia", [("id", "id", "BIGINT")]),
    "VYZVA_VYKONAVANIE": ("vykonavanie", [
        ("kategoriaregionov_id", "kategoriaRegionov.id", "BIGINT"),
        ("specifickycielprogramu_id", "specifickyCielProgramu.id", "BIGINT"),
        ("vykonavanie_id", "vykonavanie.id", "BIGINT"),
    ]),
    "VYZVA_ZIADATEL": ("ziadatel", [("id", "id", "BIGINT")]),
}


TABLE_COMMENTS: dict[str, str] = {
    TABLE_VYZVA: (
        "One row per call for proposals ('vyzva'), the mechanism through "
        "which a programme's measures are opened for grant applications "
        "('zonfp'). Purely additive: once an id is stored it is never "
        "re-fetched, updated, or deleted."
    ),
    _t("VYZVA_AKTUALITANAVYZVE"): "News/update items published on the call ('aktualita na vyzve').",
    _t("VYZVA_CIELOVASKUPINA"): "Eligible target groups for the call.",
    _t("VYZVA_DALSIAFORMALNANALEZITOST"): "Additional formal requirements applicants must meet under the call.",
    _t("VYZVA_DALSIASKUTOCNOST"): "Additional facts/notices published about the call.",
    _t("VYZVA_DOKUMENT"): "Documents (e.g. call text, annexes) attached to the call.",
    _t("VYZVA_EXTERNYZDROJ"): "External web links referenced by the call.",
    _t("VYZVA_FOND"): "EU fund(s) (e.g. ERDF, ESF+, Cohesion Fund) financing the call.",
    _t("VYZVA_FORMAPODPORY"): "Forms of support (e.g. grant, refundable assistance) available under the call, by region category and specific objective.",
    _t("VYZVA_HOSPODARSKACINNOST"): "Eligible economic activities (NACE-style classification) under the call, by region category and specific objective.",
    _t("VYZVA_KATEGORIAREGIONOV"): "Region categories the call applies to.",
    _t("VYZVA_KONTAKTNAOSOBA"): "Contact persons published for applicant queries about the call.",
    _t("VYZVA_MAKROREGIONALNASTRATEGIAASTRATEGIAPREMORSKEOBLASTI"): (
        "Macro-regional strategies and sea-basin strategies (e.g. EUSDR) the call contributes to, by region category and specific objective."
    ),
    _t("VYZVA_MIESTOREALIZACIE"): "Eligible places of implementation for the call.",
    _t("VYZVA_MIESTOREALIZACIEFULL"): "Eligible places of implementation for the call (detailed/full location entries).",
    _t("VYZVA_MIESTOREALIZACIESTAT"): "Eligible countries of implementation for the call.",
    _t("VYZVA_OBLASTINTERVENCIE"): "Intervention fields (EU classification dimension) the call targets, by region category and specific objective.",
    _t("VYZVA_OPATRENIE"): "Measure(s) the call is issued under.",
    _t("VYZVA_OPRAVNENEVYDAVKY"): "Categories of eligible expenditure under the call, by programme action type.",
    _t("VYZVA_PARTNER"): "Eligible project-partner types for the call.",
    _t("VYZVA_PLANOVANAVYZVA"): "Planned call(s) this call for proposals originated from.",
    _t("VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU"): "Conditions for granting the contribution (eligibility conditions) applicants must satisfy under the call.",
    _t("VYZVA_POSUDZOVANEOBDOBIE"): "Assessment periods (evaluation rounds) defined for the call, with their submission deadlines.",
    _t("VYZVA_PROJEKTOVYZAMERIUS"): "Project intentions ('projektovy zamer IUS') linked to the call.",
    _t("VYZVA_RODOVAROVNOST"): "Gender-equality classifications tagged on the call, by region category and specific objective.",
    _t("VYZVA_SEKUNDARNYTEMATICKYOKRUH"): "Secondary thematic focus areas tagged on the call, by region category and specific objective.",
    _t("VYZVA_SPECIFICKYCIELPROGRAMU"): "Specific objective(s) the call contributes to.",
    _t("VYZVA_STATNAPOMOC"): "State aid scheme(s) the call operates under.",
    _t("VYZVA_SUBJEKT"): "Entities/institutions related to the call (e.g. co-awarding bodies).",
    _t("VYZVA_TYPAKCIE"): "Types of action eligible under the call, by region category and specific objective.",
    _t("VYZVA_TYPAKCIEPROGRAMU"): "Programme action type(s) eligible under the call.",
    _t("VYZVA_TYPINTERVENCIE"): "Types of intervention eligible under the call, by region category and specific objective.",
    _t("VYZVA_UKAZOVATELVYSLEDKOVY"): "Result indicators the call must report against, by region category and specific objective.",
    _t("VYZVA_UKAZOVATELVYSTUPOVY"): "Output indicators the call must report against, by region category and specific objective.",
    _t("VYZVA_URCITATEMA"): "Specific thematic tags on the call, by region category and specific objective.",
    _t("VYZVA_UZEMNYMECHANIZMUSAZAMERANIE"): "Territorial mechanisms and focus (e.g. ITI, CLLD) the call uses, by region category and specific objective.",
    _t("VYZVA_VERZIA"): "Published versions of the call (e.g. after an amendment).",
    _t("VYZVA_VYKONAVANIE"): "Implementation modes tagged on the call, by region category and specific objective.",
    _t("VYZVA_ZIADATEL"): "Eligible applicant types for the call.",
    _t("VYZVA_SPOSOBFINANCOVANIA"): (
        "Financing methods (e.g. grant, refundable form of assistance) applicable to the call. "
        "Child rows carrying vyzva_id back to itms21_vyzva; inserted once when the parent detail is first fetched, never updated/deleted."
    ),
    _t("VYZVA_AKTUALITANAVYZVE_DOKUMENT"): (
        "Documents attached to a news/update item published on the call. Grandchild rows carrying only "
        "vyzva_id (not the specific update item's id, which the API doesn't expose) back to itms21_vyzva; "
        "inserted once when the parent detail is first fetched, never updated/deleted."
    ),
    _t("VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU_PRILOHA"): (
        "Attachments required by a condition for granting the contribution. Grandchild rows carrying only "
        "vyzva_id (not the specific condition's id, which the API doesn't expose) back to itms21_vyzva; "
        "inserted once when the parent detail is first fetched, never updated/deleted."
    ),
}
for _child_table in CHILD_TABLES:
    TABLE_COMMENTS[_t(_child_table)] += (
        " Child rows carrying vyzva_id back to itms21_vyzva; inserted once "
        "when the parent detail is first fetched, never updated/deleted."
    )

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    TABLE_VYZVA: {
        "id": "ITMS21 numeric id of the call for proposals.",
        "href": "API URL of this call's own detail resource.",
        "kod": "Call code.",
        "nazovsk": "Call name in Slovak.",
        "nazoven": "Call name in English.",
        "nazovde": "Call name in German.",
        "druh": "Kind/category of the call.",
        "typ": "Type of the call.",
        "mrk": "Marginalized Roma community ('marginalizovana romska komunita', MRK) indicator/code for the call, when it targets this focus area.",
        "zameranieprojektu": "Project focus/orientation targeted by the call.",
        "cielvyzvy": "Stated goal/objective of the call.",
        "doplnujuceinformacie": "Additional free-text information published about the call.",
        "dovodvznikuverzie": "Reason this version of the call record was created (e.g. amendment reason).",
        "inaskutocnostuzatvorenievyzvy": "Other stated fact/reason for closing the call.",
        "datumvyhlasenia": "Date the call was announced (epoch milliseconds).",
        "datumukoncenia": "Date the call was closed (epoch milliseconds).",
        "programoveobdobie": "Programming period the call belongs to (e.g. '2021-2027').",
        "sposobpodaniazonfp": "Method for submitting the grant application (ZoNFP) under this call.",
        "miestoprepodaniezonfp": "Place designated for submitting the grant application under this call.",
        "predpokladanalehotanarozhodnutie": "Expected time limit for a decision on submitted applications.",
        "minvyska": "Minimum total project value eligible under the call.",
        "maxvyska": "Maximum total project value eligible under the call.",
        "minziadanavyskanfp": "Minimum requested grant amount (NFP) eligible under the call.",
        "maxziadanavyskanfp": "Maximum requested grant amount (NFP) eligible under the call.",
        "maxmiera": "Maximum co-financing rate allowed under the call.",
        "mieraspolufinancovania": "Applicable co-financing rate for the call.",
        "sumaeu": "Total EU-fund allocation for the call, in euro.",
        "sumasr": "Total Slovak national co-financing allocation for the call, in euro.",
        "pocetpredlozenychziadosti": "Number of applications submitted under the call.",
        "pocetschvalenychziadosti": "Number of applications approved under the call.",
        "pocetneschvalenychziadosti": "Number of applications rejected under the call.",
        "pocetziadostivkonani": "Number of applications still under assessment.",
        "pocetrealizovanychprojektov": "Number of projects implemented as a result of the call.",
        "obsahujemiestorealizaciezahranicie": "Whether the call allows a place of implementation abroad.",
        "percentozapolozku": "Whether budget item limits under the call are expressed as a percentage.",
        "povinnostvo": "Whether public procurement is mandatory for applicants under the call.",
        "pozastavenepredkladaniezonfp": "Whether submission of grant applications under the call is currently suspended.",
        "predvyber": "Whether the call uses a pre-selection (short-listing) step before full assessment.",
        "uzavreta": "Whether the call is closed.",
        "vyhlasena": "Whether the call has been formally announced/opened.",
        "zamerpredlozeny": "Whether a project intention had to be submitted before the full application.",
        "zmenaazrusenievyzvy": "Description of a change to, or cancellation of, the call.",
        "zrusena": "Whether the call has been cancelled.",
        "kontaktemail": "Contact email published for applicant queries.",
        "kontaktnazov": "Contact name/desk published for applicant queries.",
        "kontakttelefon": "Contact phone number published for applicant queries.",
        "kontaktneudajeposkytovatela": "Contact details of the provider/managing body.",
        "poskytovatel_id": "Id of the provider/managing body responsible for the call.",
        "program_id": "Id of the parent programme this call belongs to.",
        "vyhlasovatel_id": "Id of the entity that announced the call.",
        "createdat": "Record creation timestamp in the source system.",
        "updatedat": "Record last-updated timestamp in the source system.",
    },
    _t("VYZVA_AKTUALITANAVYZVE"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "datumzverejnenia": "Publication date of the news/update item (epoch milliseconds).",
        "nazov": "Title of the news/update item.",
        "text": "Body text of the news/update item.",
    },
    _t("VYZVA_CIELOVASKUPINA"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the eligible target group.",
    },
    _t("VYZVA_DALSIAFORMALNANALEZITOST"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "dalsiaformalnanalezitost": "Text of the additional formal requirement.",
        "kod": "Code of the additional formal requirement.",
    },
    _t("VYZVA_DALSIASKUTOCNOST"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "dalsiaskutocnost": "Text of the additional fact/notice.",
        "kod": "Code of the additional fact/notice.",
    },
    _t("VYZVA_DOKUMENT"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "nazov": "Document name.",
        "uuid": "Document's file identifier (uuid), used to build its download URL.",
    },
    _t("VYZVA_EXTERNYZDROJ"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "nazov": "External source name.",
        "url": "External source URL.",
    },
    _t("VYZVA_FOND"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the EU fund financing the call.",
    },
    _t("VYZVA_FORMAPODPORY"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "formapodpory_id": "Id of the form of support (e.g. grant, refundable assistance).",
        "kategoriaregionov_id": "Id of the region category this form of support applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this form of support applies to.",
    },
    _t("VYZVA_HOSPODARSKACINNOST"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "hospodarskacinnost_id": "Id of the eligible economic activity (NACE-style classification).",
        "kategoriaregionov_id": "Id of the region category this economic activity applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this economic activity applies to.",
    },
    _t("VYZVA_KATEGORIAREGIONOV"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the region category the call applies to.",
    },
    _t("VYZVA_KONTAKTNAOSOBA"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "email": "Contact person's email address.",
        "osoba_meno": "Contact person's first name.",
        "osoba_menouplne": "Contact person's full name.",
        "osoba_priezvisko": "Contact person's surname.",
        "osoba_titulpred": "Contact person's academic title placed before the name.",
        "osoba_titulza": "Contact person's academic title placed after the name.",
        "telefon": "Contact person's phone number.",
    },
    _t("VYZVA_MAKROREGIONALNASTRATEGIAASTRATEGIAPREMORSKEOBLASTI"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "kategoriaregionov_id": "Id of the region category this strategy association applies to.",
        "makroregionalnastrategiaastrategiapremorskeoblasti_id": "Id of the macro-regional strategy or sea-basin strategy (e.g. EUSDR).",
        "specifickycielprogramu_id": "Id of the specific objective this strategy association applies to.",
    },
    _t("VYZVA_MIESTOREALIZACIE"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the eligible place of implementation.",
    },
    _t("VYZVA_MIESTOREALIZACIEFULL"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the eligible place of implementation (detailed/full location entry).",
    },
    _t("VYZVA_MIESTOREALIZACIESTAT"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the eligible country of implementation.",
    },
    _t("VYZVA_OBLASTINTERVENCIE"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "kategoriaregionov_id": "Id of the region category this intervention field applies to.",
        "oblastintervencie_id": "Id of the intervention field (EU classification dimension code).",
        "specifickycielprogramu_id": "Id of the specific objective this intervention field applies to.",
    },
    _t("VYZVA_OPATRENIE"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the measure this call is issued under (itms21_program_opatrenie.id).",
    },
    _t("VYZVA_OPRAVNENEVYDAVKY"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "kod": "Code of the eligible expenditure category.",
        "nazovde": "Eligible expenditure category name in German.",
        "nazoven": "Eligible expenditure category name in English.",
        "nazovsk": "Eligible expenditure category name in Slovak.",
        "typakcieprogramu_id": "Id of the programme action type this eligible expenditure category applies to.",
    },
    _t("VYZVA_PARTNER"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the eligible project-partner type.",
    },
    _t("VYZVA_PLANOVANAVYZVA"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the planned call this call for proposals originated from (itms21_planovanavyzva.id).",
    },
    _t("VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "nazovde": "Condition name in German.",
        "nazoven": "Condition name in English.",
        "nazovsk": "Condition name in Slovak.",
        "popisde": "Condition description in German.",
        "popisen": "Condition description in English.",
        "popissk": "Condition description in Slovak.",
        "poradovecislo": "Ordering position of the condition within the call.",
        "poznamka": "Free-text note on the condition.",
    },
    _t("VYZVA_POSUDZOVANEOBDOBIE"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "datumuzavierky": "Submission deadline for this assessment period (epoch milliseconds).",
        "poradovecislo": "Ordering position of this assessment period within the call.",
    },
    _t("VYZVA_PROJEKTOVYZAMERIUS"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the linked project intention ('projektovy zamer IUS').",
    },
    _t("VYZVA_RODOVAROVNOST"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "kategoriaregionov_id": "Id of the region category this gender-equality tag applies to.",
        "rodovarovnost_id": "Id of the gender-equality classification.",
        "specifickycielprogramu_id": "Id of the specific objective this gender-equality tag applies to.",
    },
    _t("VYZVA_SEKUNDARNYTEMATICKYOKRUH"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "kategoriaregionov_id": "Id of the region category this thematic focus applies to.",
        "sekundarnytematickyokruh_id": "Id of the secondary thematic focus area.",
        "specifickycielprogramu_id": "Id of the specific objective this thematic focus applies to.",
    },
    _t("VYZVA_SPECIFICKYCIELPROGRAMU"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the specific objective this call contributes to (itms21_program_specifickycielprogramu.id).",
    },
    _t("VYZVA_STATNAPOMOC"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "kod": "State aid scheme code.",
        "nazov": "State aid scheme name.",
        "platnostdo": "End of the state aid scheme's validity period (epoch milliseconds).",
        "platnostod": "Start of the state aid scheme's validity period (epoch milliseconds).",
        "typ": "Type of the state aid scheme.",
    },
    _t("VYZVA_SUBJEKT"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the related entity/institution (e.g. a co-awarding body).",
    },
    _t("VYZVA_TYPAKCIE"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "kategoriaregionov_id": "Id of the region category this type of action applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this type of action applies to.",
        "typakcie_id": "Id of the type of action.",
    },
    _t("VYZVA_TYPAKCIEPROGRAMU"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the eligible programme action type (itms21_program_typakcieprogramu.id).",
    },
    _t("VYZVA_TYPINTERVENCIE"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "kategoriaregionov_id": "Id of the region category this type of intervention applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this type of intervention applies to.",
        "typintervencie_id": "Id of the type of intervention.",
    },
    _t("VYZVA_UKAZOVATELVYSLEDKOVY"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of this result-indicator association.",
        "kategoriaregionov_id": "Id of the region category this result indicator applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this result indicator applies to.",
        "ukazovatelprojektovy_id": "Id of the underlying project-level result indicator definition.",
    },
    _t("VYZVA_UKAZOVATELVYSTUPOVY"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of this output-indicator association.",
        "kategoriaregionov_id": "Id of the region category this output indicator applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this output indicator applies to.",
        "ukazovatelprojektovy_id": "Id of the underlying project-level output indicator definition.",
    },
    _t("VYZVA_URCITATEMA"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "kategoriaregionov_id": "Id of the region category this thematic tag applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this thematic tag applies to.",
        "urcitatema_id": "Id of the specific theme.",
    },
    _t("VYZVA_UZEMNYMECHANIZMUSAZAMERANIE"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "kategoriaregionov_id": "Id of the region category this territorial mechanism applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this territorial mechanism applies to.",
        "uzemnymechanizmusazameranie_id": "Id of the territorial mechanism and focus (e.g. ITI, CLLD).",
    },
    _t("VYZVA_VERZIA"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of this published version of the call.",
    },
    _t("VYZVA_VYKONAVANIE"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "kategoriaregionov_id": "Id of the region category this implementation mode applies to.",
        "specifickycielprogramu_id": "Id of the specific objective this implementation mode applies to.",
        "vykonavanie_id": "Id of the implementation mode.",
    },
    _t("VYZVA_ZIADATEL"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "id": "Id of the eligible applicant type.",
    },
    _t("VYZVA_SPOSOBFINANCOVANIA"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "sposobfinancovania": "Financing method applicable to the call (e.g. grant, refundable form of assistance).",
    },
    _t("VYZVA_AKTUALITANAVYZVE_DOKUMENT"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "nazov": "Document name.",
        "uuid": "Document's file identifier (uuid), used to build its download URL.",
    },
    _t("VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU_PRILOHA"): {
        "vyzva_id": "Id of the parent call (itms21_vyzva.id).",
        "integracie": "Integration/system reference for the attachment.",
        "nazovde": "Attachment name in German.",
        "nazoven": "Attachment name in English.",
        "nazovsk": "Attachment name in Slovak.",
        "poradovecislo": "Ordering position of the attachment within its condition.",
        "prilohapovinna": "Whether the attachment is mandatory.",
        "sposobpredlozenia": "Method by which the attachment must be submitted.",
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
    bare_names = ["VYZVA"] + list(CHILD_TABLES.keys()) + [
        "VYZVA_SPOSOBFINANCOVANIA",
        "VYZVA_AKTUALITANAVYZVE_DOKUMENT",
        "VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU_PRILOHA",
    ]
    for name in bare_names:
        con.execute(f"ALTER TABLE IF EXISTS {name} RENAME TO {_t(name)}")
        # Also catch a table already prefixed but with the old ALL-CAPS name
        # (e.g. from a run of this script before lowercasing was added).
        con.execute(f"ALTER TABLE IF EXISTS {TABLE_PREFIX}{name} RENAME TO {_t(name)}")

    if _table_exists(con, TABLE_VYZVA):
        _rename_columns(con, TABLE_VYZVA, [col for col, _, _ in VYZVA_COLUMNS])

    for table, (_, columns) in CHILD_TABLES.items():
        full = _t(table)
        if _table_exists(con, full):
            con.execute(f"ALTER TABLE {full} RENAME COLUMN VYZVA_ID TO vyzva_id")
            _rename_columns(con, full, [col for col, _, _ in columns])

    if _table_exists(con, _t("VYZVA_SPOSOBFINANCOVANIA")):
        full = _t("VYZVA_SPOSOBFINANCOVANIA")
        con.execute(f"ALTER TABLE {full} RENAME COLUMN VYZVA_ID TO vyzva_id")
        _rename_columns(con, full, ["sposobfinancovania"])

    if _table_exists(con, _t("VYZVA_AKTUALITANAVYZVE_DOKUMENT")):
        full = _t("VYZVA_AKTUALITANAVYZVE_DOKUMENT")
        con.execute(f"ALTER TABLE {full} RENAME COLUMN VYZVA_ID TO vyzva_id")
        _rename_columns(con, full, ["nazov", "uuid"])

    if _table_exists(con, _t("VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU_PRILOHA")):
        full = _t("VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU_PRILOHA")
        con.execute(f"ALTER TABLE {full} RENAME COLUMN VYZVA_ID TO vyzva_id")
        _rename_columns(con, full, [
            "integracie", "nazovde", "nazoven", "nazovsk",
            "poradovecislo", "prilohapovinna", "sposobpredlozenia",
        ])


def ensure_full_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create VYZVA and every VYZVA_* child/grandchild table if missing."""
    migrate_table_prefix(con)

    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in VYZVA_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_VYZVA} (
            {cols_sql},
            PRIMARY KEY (id)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_t(table)} (
                vyzva_id BIGINT,
                {cols_sql}
            )
        """)

    # sposobFinancovania is a plain list of strings (not a list of objects),
    # so it doesn't fit the generic CHILD_TABLES column-path shape above.
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {_t("VYZVA_SPOSOBFINANCOVANIA")} (
            vyzva_id BIGINT,
            sposobfinancovania VARCHAR
        )
    """)

    # Two grandchild tables: a "dokument"/"priloha" list living one level
    # deeper than a child-table row (aktualitaNaVyzve / podmienkaPoskytnutiaPrispevku).
    # Neither parent item carries its own id in the API, so these grandchild
    # rows are only linkable back to the vyzva itself, not to the specific
    # parent item.
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {_t("VYZVA_AKTUALITANAVYZVE_DOKUMENT")} (
            vyzva_id BIGINT,
            nazov VARCHAR,
            uuid VARCHAR
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {_t("VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU_PRILOHA")} (
            vyzva_id BIGINT,
            integracie VARCHAR,
            nazovde VARCHAR,
            nazoven VARCHAR,
            nazovsk VARCHAR,
            poradovecislo INTEGER,
            prilohapovinna BOOLEAN,
            sposobpredlozenia VARCHAR
        )
    """)


def store_full_detail(con: duckdb.DuckDBPyConnection, detail: dict) -> None:
    """Decompose one vyzva's full detail JSON into VYZVA + all child/grandchild
    tables. Only ever called once per vyzva id (gated by get_known_ids in
    sync_vyzvy), so this is a plain INSERT - never re-fetched, never updated."""
    vyzva_id = detail.get("id")

    columns_sql = ", ".join(col for col, _, _ in VYZVA_COLUMNS)
    placeholders = ", ".join("?" for _ in VYZVA_COLUMNS)
    values = [_get(detail, path) for _, path, _ in VYZVA_COLUMNS]
    con.execute(
        f"INSERT INTO {TABLE_VYZVA} ({columns_sql}) VALUES ({placeholders}) "
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
            [vyzva_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {_t(table)} (vyzva_id, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )

    sposob_rows = [[vyzva_id, s] for s in (detail.get("sposobFinancovania") or [])]
    if sposob_rows:
        con.executemany(
            f"INSERT INTO {_t('VYZVA_SPOSOBFINANCOVANIA')} (vyzva_id, sposobfinancovania) VALUES (?, ?)",
            sposob_rows,
        )

    aktualita_doc_rows = []
    for item in detail.get("aktualitaNaVyzve") or []:
        for doc in item.get("dokument") or []:
            aktualita_doc_rows.append([vyzva_id, doc.get("nazov"), doc.get("uuid")])
    if aktualita_doc_rows:
        con.executemany(
            f"INSERT INTO {_t('VYZVA_AKTUALITANAVYZVE_DOKUMENT')} (vyzva_id, nazov, uuid) VALUES (?, ?, ?)",
            aktualita_doc_rows,
        )

    priloha_rows = []
    for item in detail.get("podmienkaPoskytnutiaPrispevku") or []:
        for pri in item.get("priloha") or []:
            priloha_rows.append([
                vyzva_id, pri.get("integracie"), pri.get("nazovDe"), pri.get("nazovEn"),
                pri.get("nazovSk"), pri.get("poradoveCislo"), pri.get("prilohaPovinna"),
                pri.get("sposobPredlozenia"),
            ])
    if priloha_rows:
        con.executemany(
            f"INSERT INTO {_t('VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU_PRILOHA')} ("
            "vyzva_id, integracie, nazovde, nazoven, nazovsk, poradovecislo, "
            "prilohapovinna, sposobpredlozenia) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            priloha_rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT id FROM {TABLE_VYZVA}").fetchall()
    return {row[0] for row in rows}


def sync_vyzvy() -> tuple[int, int, int]:
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

    print("Fetching vyzva list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} vyzvy.")

    known_ids = get_known_ids(con)

    to_fetch = [item.get("id") for item in list_items if item.get("id") not in known_ids]

    print(f"{len(to_fetch)} new vyzva(s) to fetch; {len(list_items) - len(to_fetch)} already stored (skipped).")

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
    total, fetched, failed = sync_vyzvy()
    print(f"Done. {total} total in list, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
