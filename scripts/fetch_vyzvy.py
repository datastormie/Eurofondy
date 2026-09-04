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

DB_PATH = Path("data/eurofondy.duckdb")  # shared DuckDB file, separate tables inside

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
    ensure_full_schema(con)

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
