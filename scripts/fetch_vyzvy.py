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

Run weekly via GitHub Actions (.github/workflows/weekly.yml).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests

LIST_URL = "https://api.itms21.sk/public/v1/vyzva?limit=-1"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/vyzva/id/{id}"

DB_PATH = Path("data/eurofondy.duckdb")  # shared DuckDB file, separate tables inside

TABLE_VYZVA = "VYZVA"

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


# --- Full normalized schema -------------------------------------------------
# Mirrors the ITMS21 "vyzva" detail endpoint field-by-field.
# column_name -> (dotted path in the raw detail JSON, DuckDB type).

VYZVA_COLUMNS: list[tuple[str, str, str]] = [
    ("ID", "id", "BIGINT"),
    ("HREF", "href", "VARCHAR"),
    ("KOD", "kod", "VARCHAR"),
    ("NAZOVSK", "nazovSk", "VARCHAR"),
    ("NAZOVEN", "nazovEn", "VARCHAR"),
    ("NAZOVDE", "nazovDe", "VARCHAR"),
    ("DRUH", "druh", "VARCHAR"),
    ("TYP", "typ", "VARCHAR"),
    ("MRK", "mrk", "VARCHAR"),
    ("ZAMERANIEPROJEKTU", "zameranieProjektu", "VARCHAR"),
    ("CIELVYZVY", "cielVyzvy", "VARCHAR"),
    ("DOPLNUJUCEINFORMACIE", "doplnujuceInformacie", "VARCHAR"),
    ("DOVODVZNIKUVERZIE", "dovodVznikuVerzie", "VARCHAR"),
    ("INASKUTOCNOSTUZATVORENIEVYZVY", "inaSkutocnostUzatvorenieVyzvy", "VARCHAR"),
    ("DATUMVYHLASENIA", "datumVyhlasenia", "BIGINT"),
    ("DATUMUKONCENIA", "datumUkoncenia", "BIGINT"),
    ("PROGRAMOVEOBDOBIE", "programoveObdobie", "VARCHAR"),
    ("SPOSOBPODANIAZONFP", "sposobPodaniaZoNFP", "VARCHAR"),
    ("MIESTOPREPODANIEZONFP", "miestoPrePodanieZoNFP", "VARCHAR"),
    ("PREDPOKLADANALEHOTANAROZHODNUTIE", "predpokladanaLehotaNaRozhodnutie", "VARCHAR"),
    ("MINVYSKA", "minVyska", "VARCHAR"),
    ("MAXVYSKA", "maxVyska", "VARCHAR"),
    ("MINZIADANAVYSKANFP", "minZiadanaVyskaNfp", "VARCHAR"),
    ("MAXZIADANAVYSKANFP", "maxZiadanaVyskaNfp", "VARCHAR"),
    ("MAXMIERA", "maxMiera", "VARCHAR"),
    ("MIERASPOLUFINANCOVANIA", "mieraSpolufinancovania", "VARCHAR"),
    ("SUMAEU", "sumaEu", "DOUBLE"),
    ("SUMASR", "sumaSr", "DOUBLE"),
    ("POCETPREDLOZENYCHZIADOSTI", "pocetPredlozenychZiadosti", "INTEGER"),
    ("POCETSCHVALENYCHZIADOSTI", "pocetSchvalenychZiadosti", "INTEGER"),
    ("POCETNESCHVALENYCHZIADOSTI", "pocetNeschvalenychZiadosti", "INTEGER"),
    ("POCETZIADOSTIVKONANI", "pocetZiadostiVKonani", "INTEGER"),
    ("POCETREALIZOVANYCHPROJEKTOV", "pocetRealizovanychProjektov", "INTEGER"),
    ("OBSAHUJEMIESTOREALIZACIEZAHRANICIE", "obsahujeMiestoRealizacieZahranicie", "BOOLEAN"),
    ("PERCENTOZAPOLOZKU", "percentoZaPolozku", "BOOLEAN"),
    ("POVINNOSTVO", "povinnostVo", "BOOLEAN"),
    ("POZASTAVENEPREDKLADANIEZONFP", "pozastavenePredkladanieZonfp", "BOOLEAN"),
    ("PREDVYBER", "predvyber", "BOOLEAN"),
    ("UZAVRETA", "uzavreta", "BOOLEAN"),
    ("VYHLASENA", "vyhlasena", "BOOLEAN"),
    ("ZAMERPREDLOZENY", "zamerPredlozeny", "BOOLEAN"),
    ("ZMENAAZRUSENIEVYZVY", "zmenaAZrusenieVyzvy", "VARCHAR"),
    ("ZRUSENA", "zrusena", "BOOLEAN"),
    ("KONTAKTEMAIL", "kontaktEmail", "VARCHAR"),
    ("KONTAKTNAZOV", "kontaktNazov", "VARCHAR"),
    ("KONTAKTTELEFON", "kontaktTelefon", "VARCHAR"),
    ("KONTAKTNEUDAJEPOSKYTOVATELA", "kontaktneUdajePoskytovatela", "VARCHAR"),
    ("POSKYTOVATEL_ID", "poskytovatel.id", "BIGINT"),
    ("PROGRAM_ID", "program.id", "BIGINT"),
    ("VYHLASOVATEL_ID", "vyhlasovatel.id", "BIGINT"),
    ("CREATEDAT", "createdAt", "VARCHAR"),
    ("UPDATEDAT", "updatedAt", "VARCHAR"),
]

# table_name -> (source list field in the detail JSON, [(column, path-within-item, type), ...])
CHILD_TABLES: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
    "VYZVA_AKTUALITANAVYZVE": ("aktualitaNaVyzve", [
        ("DATUMZVEREJNENIA", "datumZverejnenia", "BIGINT"),
        ("NAZOV", "nazov", "VARCHAR"),
        ("TEXT", "text", "VARCHAR"),
    ]),
    "VYZVA_CIELOVASKUPINA": ("cielovaSkupina", [("ID", "id", "BIGINT")]),
    "VYZVA_DALSIAFORMALNANALEZITOST": ("dalsiaFormalnaNalezitost", [
        ("DALSIAFORMALNANALEZITOST", "dalsiaFormalnaNalezitost", "VARCHAR"),
        ("KOD", "kod", "VARCHAR"),
    ]),
    "VYZVA_DALSIASKUTOCNOST": ("dalsiaSkutocnost", [
        ("DALSIASKUTOCNOST", "dalsiaSkutocnost", "VARCHAR"),
        ("KOD", "kod", "VARCHAR"),
    ]),
    "VYZVA_DOKUMENT": ("dokument", [
        ("NAZOV", "nazov", "VARCHAR"),
        ("UUID", "uuid", "VARCHAR"),
    ]),
    "VYZVA_EXTERNYZDROJ": ("externyZdroj", [
        ("NAZOV", "nazov", "VARCHAR"),
        ("URL", "url", "VARCHAR"),
    ]),
    "VYZVA_FOND": ("fond", [("ID", "id", "BIGINT")]),
    "VYZVA_FORMAPODPORY": ("formaPodpory", [
        ("FORMAPODPORY_ID", "formaPodpory.id", "BIGINT"),
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "VYZVA_HOSPODARSKACINNOST": ("hospodarskaCinnost", [
        ("HOSPODARSKACINNOST_ID", "hospodarskaCinnost.id", "BIGINT"),
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "VYZVA_KATEGORIAREGIONOV": ("kategoriaRegionov", [("ID", "id", "BIGINT")]),
    "VYZVA_KONTAKTNAOSOBA": ("kontaktnaOsoba", [
        ("EMAIL", "email", "VARCHAR"),
        ("OSOBA_MENO", "osoba.meno", "VARCHAR"),
        ("OSOBA_MENOUPLNE", "osoba.menoUplne", "VARCHAR"),
        ("OSOBA_PRIEZVISKO", "osoba.priezvisko", "VARCHAR"),
        ("OSOBA_TITULPRED", "osoba.titulPred", "VARCHAR"),
        ("OSOBA_TITULZA", "osoba.titulZa", "VARCHAR"),
        ("TELEFON", "telefon", "VARCHAR"),
    ]),
    "VYZVA_MAKROREGIONALNASTRATEGIAASTRATEGIAPREMORSKEOBLASTI": (
        "makroregionalnaStrategiaAStrategiaPreMorskeOblasti", [
            ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
            ("MAKROREGIONALNASTRATEGIAASTRATEGIAPREMORSKEOBLASTI_ID",
             "makroregionalnaStrategiaAStrategiaPreMorskeOblasti.id", "BIGINT"),
            ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ]),
    "VYZVA_MIESTOREALIZACIE": ("miestoRealizacie", [("ID", "id", "BIGINT")]),
    "VYZVA_MIESTOREALIZACIEFULL": ("miestoRealizacieFull", [("ID", "id", "BIGINT")]),
    "VYZVA_MIESTOREALIZACIESTAT": ("miestoRealizacieStat", [("ID", "id", "BIGINT")]),
    "VYZVA_OBLASTINTERVENCIE": ("oblastIntervencie", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("OBLASTINTERVENCIE_ID", "oblastIntervencie.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "VYZVA_OPATRENIE": ("opatrenie", [("ID", "id", "BIGINT")]),
    "VYZVA_OPRAVNENEVYDAVKY": ("opravneneVydavky", [
        ("KOD", "kod", "VARCHAR"),
        ("NAZOVDE", "nazovDe", "VARCHAR"),
        ("NAZOVEN", "nazovEn", "VARCHAR"),
        ("NAZOVSK", "nazovSk", "VARCHAR"),
        ("TYPAKCIEPROGRAMU_ID", "typAkcieProgramu.id", "BIGINT"),
    ]),
    "VYZVA_PARTNER": ("partner", [("ID", "id", "BIGINT")]),
    "VYZVA_PLANOVANAVYZVA": ("planovanaVyzva", [("ID", "id", "BIGINT")]),
    "VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU": ("podmienkaPoskytnutiaPrispevku", [
        ("NAZOVDE", "nazovDe", "VARCHAR"),
        ("NAZOVEN", "nazovEn", "VARCHAR"),
        ("NAZOVSK", "nazovSk", "VARCHAR"),
        ("POPISDE", "popisDe", "VARCHAR"),
        ("POPISEN", "popisEn", "VARCHAR"),
        ("POPISSK", "popisSk", "VARCHAR"),
        ("PORADOVECISLO", "poradoveCislo", "INTEGER"),
        ("POZNAMKA", "poznamka", "VARCHAR"),
    ]),
    "VYZVA_POSUDZOVANEOBDOBIE": ("posudzovaneObdobie", [
        ("DATUMUZAVIERKY", "datumUzavierky", "BIGINT"),
        ("PORADOVECISLO", "poradoveCislo", "INTEGER"),
    ]),
    "VYZVA_PROJEKTOVYZAMERIUS": ("projektovyZamerIUS", [("ID", "id", "BIGINT")]),
    "VYZVA_RODOVAROVNOST": ("rodovaRovnost", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("RODOVAROVNOST_ID", "rodovaRovnost.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "VYZVA_SEKUNDARNYTEMATICKYOKRUH": ("sekundarnyTematickyOkruh", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SEKUNDARNYTEMATICKYOKRUH_ID", "sekundarnyTematickyOkruh.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
    ]),
    "VYZVA_SPECIFICKYCIELPROGRAMU": ("specifickyCielProgramu", [("ID", "id", "BIGINT")]),
    "VYZVA_STATNAPOMOC": ("statnaPomoc", [
        ("KOD", "kod", "VARCHAR"),
        ("NAZOV", "nazov", "VARCHAR"),
        ("PLATNOSTDO", "platnostDo", "BIGINT"),
        ("PLATNOSTOD", "platnostOd", "BIGINT"),
        ("TYP", "typ", "VARCHAR"),
    ]),
    "VYZVA_SUBJEKT": ("subjekt", [("ID", "id", "BIGINT")]),
    "VYZVA_TYPAKCIE": ("typAkcie", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ("TYPAKCIE_ID", "typAkcie.id", "BIGINT"),
    ]),
    "VYZVA_TYPAKCIEPROGRAMU": ("typAkcieProgramu", [("ID", "id", "BIGINT")]),
    "VYZVA_TYPINTERVENCIE": ("typIntervencie", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ("TYPINTERVENCIE_ID", "typIntervencie.id", "BIGINT"),
    ]),
    "VYZVA_UKAZOVATELVYSLEDKOVY": ("ukazovatelVysledkovy", [
        ("ID", "id", "BIGINT"),
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ("UKAZOVATELPROJEKTOVY_ID", "ukazovatelProjektovy.id", "BIGINT"),
    ]),
    "VYZVA_UKAZOVATELVYSTUPOVY": ("ukazovatelVystupovy", [
        ("ID", "id", "BIGINT"),
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ("UKAZOVATELPROJEKTOVY_ID", "ukazovatelProjektovy.id", "BIGINT"),
    ]),
    "VYZVA_URCITATEMA": ("urcitaTema", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ("URCITATEMA_ID", "urcitaTema.id", "BIGINT"),
    ]),
    "VYZVA_UZEMNYMECHANIZMUSAZAMERANIE": ("uzemnyMechanizmusAZameranie", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ("UZEMNYMECHANIZMUSAZAMERANIE_ID", "uzemnyMechanizmusAZameranie.id", "BIGINT"),
    ]),
    "VYZVA_VERZIA": ("verzia", [("ID", "id", "BIGINT")]),
    "VYZVA_VYKONAVANIE": ("vykonavanie", [
        ("KATEGORIAREGIONOV_ID", "kategoriaRegionov.id", "BIGINT"),
        ("SPECIFICKYCIELPROGRAMU_ID", "specifickyCielProgramu.id", "BIGINT"),
        ("VYKONAVANIE_ID", "vykonavanie.id", "BIGINT"),
    ]),
    "VYZVA_ZIADATEL": ("ziadatel", [("ID", "id", "BIGINT")]),
}


def ensure_full_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create VYZVA and every VYZVA_* child/grandchild table if missing."""
    cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in VYZVA_COLUMNS)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_VYZVA} (
            {cols_sql},
            PRIMARY KEY (ID)
        )
    """)

    for table, (_, columns) in CHILD_TABLES.items():
        cols_sql = ",\n            ".join(f"{col} {sqltype}" for col, _, sqltype in columns)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                VYZVA_ID BIGINT,
                {cols_sql}
            )
        """)

    # sposobFinancovania is a plain list of strings (not a list of objects),
    # so it doesn't fit the generic CHILD_TABLES column-path shape above.
    con.execute("""
        CREATE TABLE IF NOT EXISTS VYZVA_SPOSOBFINANCOVANIA (
            VYZVA_ID BIGINT,
            SPOSOBFINANCOVANIA VARCHAR
        )
    """)

    # Two grandchild tables: a "dokument"/"priloha" list living one level
    # deeper than a child-table row (aktualitaNaVyzve / podmienkaPoskytnutiaPrispevku).
    # Neither parent item carries its own id in the API, so these grandchild
    # rows are only linkable back to the vyzva itself, not to the specific
    # parent item.
    con.execute("""
        CREATE TABLE IF NOT EXISTS VYZVA_AKTUALITANAVYZVE_DOKUMENT (
            VYZVA_ID BIGINT,
            NAZOV VARCHAR,
            UUID VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU_PRILOHA (
            VYZVA_ID BIGINT,
            INTEGRACIE VARCHAR,
            NAZOVDE VARCHAR,
            NAZOVEN VARCHAR,
            NAZOVSK VARCHAR,
            PORADOVECISLO INTEGER,
            PRILOHAPOVINNA BOOLEAN,
            SPOSOBPREDLOZENIA VARCHAR
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
            [vyzva_id] + [_get(item, path) for _, path, _ in columns]
            for item in items
        ]
        con.executemany(
            f"INSERT INTO {table} (VYZVA_ID, {columns_sql}) VALUES (?, {placeholders})",
            rows,
        )

    sposob_rows = [[vyzva_id, s] for s in (detail.get("sposobFinancovania") or [])]
    if sposob_rows:
        con.executemany(
            "INSERT INTO VYZVA_SPOSOBFINANCOVANIA (VYZVA_ID, SPOSOBFINANCOVANIA) VALUES (?, ?)",
            sposob_rows,
        )

    aktualita_doc_rows = []
    for item in detail.get("aktualitaNaVyzve") or []:
        for doc in item.get("dokument") or []:
            aktualita_doc_rows.append([vyzva_id, doc.get("nazov"), doc.get("uuid")])
    if aktualita_doc_rows:
        con.executemany(
            "INSERT INTO VYZVA_AKTUALITANAVYZVE_DOKUMENT (VYZVA_ID, NAZOV, UUID) VALUES (?, ?, ?)",
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
            "INSERT INTO VYZVA_PODMIENKAPOSKYTNUTIAPRISPEVKU_PRILOHA ("
            "VYZVA_ID, INTEGRACIE, NAZOVDE, NAZOVEN, NAZOVSK, PORADOVECISLO, "
            "PRILOHAPOVINNA, SPOSOBPREDLOZENIA) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            priloha_rows,
        )


def get_known_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Ids already fully stored, so we never re-fetch them."""
    rows = con.execute(f"SELECT ID FROM {TABLE_VYZVA}").fetchall()
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
