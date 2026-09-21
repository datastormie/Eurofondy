"""
Builds the `datamart` schema inside the shared eufunds.duckdb: denormalized,
public-interest tables computed with plain SQL over the already-fetched
`slovakia` tables (no API calls, no dependency on the retired `website`
schema). Each table is fully rebuilt every run (CREATE OR REPLACE TABLE)
since these are cheap aggregation queries, not incremental syncs like the
fetch scripts. Every table is also exported to a docs/*_data.json file for
its corresponding explorer page.

Depends on slovakia.itms21_projekt, itms21_program, itms21_subjekt,
itms21_ciselniky_detail, itms21_projekt_miestorealizaciefull,
itms21_projekt_financnyplan, itms21_projekt_hospodarskacinnost,
itms21_zmluvaverejneobstaravanie, itms21_verejneobstaravanie_detail,
itms21_verejneobstaravanie_detail_projekty, itms21_dodavatelobstaravatel,
and itms21_zop all being populated -- run after every fetch script in
.github/workflows/monthly.yml.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

DB_PATH = Path("data/eufunds.duckdb")
SLOVAKIA_SCHEMA = "slovakia"
DATAMART_SCHEMA = "datamart"
DOCS_DIR = Path("docs")


def _esc(value: str) -> str:
    """Escape a string for embedding in a single-quoted SQL literal."""
    return value.replace("'", "''")


REGIONAL_FUNDING_SQL = f"""
    CREATE OR REPLACE TABLE {DATAMART_SCHEMA}.regional_funding AS
    WITH fp AS (
        SELECT
            f.project_id,
            sum(CASE WHEN zd.nazovsk LIKE '%EÚ%' OR zd.nazovsk LIKE '%EU%' THEN f.suma ELSE 0 END) AS suma_eu,
            sum(CASE WHEN zd.nazovsk LIKE '%ŠR%' OR zd.nazovsk LIKE '%SR%' THEN f.suma ELSE 0 END) AS suma_sr
        FROM {SLOVAKIA_SCHEMA}.itms21_projekt_financnyplan f
        LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_ciselniky_detail zd
            ON zd.ciselnik_kod = '1052' AND zd.id = f.zdroj_id
        GROUP BY f.project_id
    ),
    regions AS (
        SELECT DISTINCT m.project_id, m.nuts3_id, nuts.nazovsk AS kraj_nazov
        FROM {SLOVAKIA_SCHEMA}.itms21_projekt_miestorealizaciefull m
        JOIN {SLOVAKIA_SCHEMA}.itms21_ciselniky_detail nuts
            ON nuts.ciselnik_kod = '1006' AND nuts.id = m.nuts3_id
        WHERE m.stat_id = 210 AND m.nuts3_id IS NOT NULL
    ),
    region_agg AS (
        SELECT project_id, count(*) AS region_count,
               string_agg(kraj_nazov, ', ' ORDER BY kraj_nazov) AS kraj_names
        FROM regions GROUP BY project_id
    )
    SELECT
        p.id AS project_id,
        p.kod AS project_kod,
        p.nazov AS project_nazov,
        p.program_id,
        prog.skratka AS program_skratka,
        prog.nazovsk AS program_nazov,
        ben.nazov AS prijimatel_nazov,
        ben.ico AS prijimatel_ico,
        ra.region_count,
        ra.kraj_names,
        fp.suma_eu,
        fp.suma_sr,
        fp.suma_eu + fp.suma_sr AS suma_spolu,
        p.celkovazazmluvnenasuma AS celkova_zazmluvnena_suma,
        p.poskytnuteprostriedky AS poskytnute_prostriedky,
        p.stav,
        p.ukonceny,
        p.vrealizacii,
        p.planovanarealizaciazaciatok AS planovany_zaciatok,
        p.planovanarealizaciakoniec AS planovany_koniec,
        p.createdat AS created_at,
        p.updatedat AS updated_at
    FROM {SLOVAKIA_SCHEMA}.itms21_projekt p
    -- INNER JOIN: a project with zero Slovak location rows would silently
    -- disappear from regional_funding (and projects.html/top_projects_chart.html)
    -- entirely. Currently every project has >=1 such row; if that ever
    -- changes, consider a LEFT JOIN with a "Nezaradené" (unassigned) fallback.
    JOIN region_agg ra ON ra.project_id = p.id
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_program prog ON prog.id = p.program_id
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_subjekt ben ON ben.id = p.prijimatel_id
    LEFT JOIN fp ON fp.project_id = p.id
"""

REGIONAL_SUMMARY_SQL = f"""
    CREATE OR REPLACE TABLE {DATAMART_SCHEMA}.regional_summary AS
    SELECT
        r.nuts3_id,
        r.kraj_nazov,
        count(*) AS project_count,
        sum(rf.suma_eu) AS suma_eu,
        sum(rf.suma_sr) AS suma_sr,
        sum(rf.suma_spolu) AS suma_spolu
    FROM (
        SELECT DISTINCT m.project_id, m.nuts3_id, nuts.nazovsk AS kraj_nazov
        FROM {SLOVAKIA_SCHEMA}.itms21_projekt_miestorealizaciefull m
        JOIN {SLOVAKIA_SCHEMA}.itms21_ciselniky_detail nuts
            ON nuts.ciselnik_kod = '1006' AND nuts.id = m.nuts3_id
        WHERE m.stat_id = 210 AND m.nuts3_id IS NOT NULL
    ) r
    JOIN {DATAMART_SCHEMA}.regional_funding rf ON rf.project_id = r.project_id AND rf.region_count = 1
    GROUP BY r.nuts3_id, r.kraj_nazov
    UNION ALL
    SELECT
        NULL AS nuts3_id,
        'Celoštátne (viacregionálne)' AS kraj_nazov,
        count(*) AS project_count,
        sum(suma_eu) AS suma_eu,
        sum(suma_sr) AS suma_sr,
        sum(suma_spolu) AS suma_spolu
    FROM {DATAMART_SCHEMA}.regional_funding
    WHERE region_count > 1
"""

PROGRAM_SUMMARY_SQL = f"""
    CREATE OR REPLACE TABLE {DATAMART_SCHEMA}.program_summary AS
    WITH proj_counts AS (
        SELECT program_id, count(*) AS project_count
        FROM {SLOVAKIA_SCHEMA}.itms21_projekt
        GROUP BY program_id
    ),
    zazmluvnene AS (
        SELECT program_id,
               sum(suma_eu) AS zazmluvnene_eu,
               sum(suma_sr) AS zazmluvnene_sr,
               sum(suma_spolu) AS zazmluvnene_spolu
        FROM {DATAMART_SCHEMA}.regional_funding
        GROUP BY program_id
    )
    SELECT
        prog.id AS program_id,
        prog.kod,
        prog.skratka,
        prog.nazovsk AS nazov_sk,
        prog.nazoven AS nazov_en,
        prog.typprogramu_typ AS typ_programu,
        prog.riadiaciorgan_nazov AS riadiaci_organ,
        subj.nazov AS subjekt_nazov,
        COALESCE(pc.project_count, 0) AS project_count,
        prog.sumaeu AS alokacia_eu,
        prog.sumasr AS alokacia_sr,
        prog.sumaspolu AS alokacia_spolu,
        COALESCE(z.zazmluvnene_eu, 0) AS zazmluvnene_eu,
        COALESCE(z.zazmluvnene_sr, 0) AS zazmluvnene_sr,
        COALESCE(z.zazmluvnene_spolu, 0) AS zazmluvnene_spolu
    FROM {SLOVAKIA_SCHEMA}.itms21_program prog
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_subjekt subj ON subj.id = prog.riadiaciorgan_subjekt_id
    LEFT JOIN proj_counts pc ON pc.program_id = prog.id
    LEFT JOIN zazmluvnene z ON z.program_id = prog.id
"""

BENEFICIARY_FUNDING_SQL = f"""
    CREATE OR REPLACE TABLE {DATAMART_SCHEMA}.beneficiary_funding AS
    WITH fp AS (
        SELECT
            f.project_id,
            sum(CASE WHEN zd.nazovsk LIKE '%EÚ%' OR zd.nazovsk LIKE '%EU%' THEN f.suma ELSE 0 END) AS suma_eu,
            sum(CASE WHEN zd.nazovsk LIKE '%ŠR%' OR zd.nazovsk LIKE '%SR%' THEN f.suma ELSE 0 END) AS suma_sr
        FROM {SLOVAKIA_SCHEMA}.itms21_projekt_financnyplan f
        LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_ciselniky_detail zd
            ON zd.ciselnik_kod = '1052' AND zd.id = f.zdroj_id
        GROUP BY f.project_id
    ),
    proj AS (
        SELECT p.id AS project_id, p.prijimatel_id, p.planovanarealizaciazaciatok,
               fp.suma_eu, fp.suma_sr, (fp.suma_eu + fp.suma_sr) AS suma_spolu
        FROM {SLOVAKIA_SCHEMA}.itms21_projekt p
        LEFT JOIN fp ON fp.project_id = p.id
        WHERE p.prijimatel_id IS NOT NULL
    ),
    sector_counts AS (
        SELECT p.prijimatel_id, hc.nazovsk AS sector, count(*) AS n
        FROM {SLOVAKIA_SCHEMA}.itms21_projekt p
        JOIN {SLOVAKIA_SCHEMA}.itms21_projekt_hospodarskacinnost h ON h.project_id = p.id
        JOIN {SLOVAKIA_SCHEMA}.itms21_ciselniky_detail hc
            ON hc.ciselnik_kod = '1038' AND hc.id = h.hospodarskacinnost_id
        WHERE p.prijimatel_id IS NOT NULL
        GROUP BY 1, 2
    ),
    primary_sector AS (
        SELECT prijimatel_id, sector
        FROM (
            SELECT prijimatel_id, sector,
                   row_number() OVER (PARTITION BY prijimatel_id ORDER BY n DESC) AS rn
            FROM sector_counts
        )
        WHERE rn = 1
    )
    SELECT
        s.id AS subjekt_id,
        s.nazov,
        s.ico,
        pf.nazovsk AS pravnaforma_nazov,
        s.adresa_obec,
        ps.sector AS primary_sector,
        count(*) AS project_count,
        sum(proj.suma_eu) AS suma_eu,
        sum(proj.suma_sr) AS suma_sr,
        sum(proj.suma_spolu) AS suma_spolu,
        min(year(epoch_ms(proj.planovanarealizaciazaciatok))) AS first_project_year,
        max(year(epoch_ms(proj.planovanarealizaciazaciatok))) AS last_project_year
    FROM proj
    JOIN {SLOVAKIA_SCHEMA}.itms21_subjekt s ON s.id = proj.prijimatel_id
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_ciselniky_detail pf
        ON pf.ciselnik_kod = '1009' AND pf.id = s.pravnaforma_id
    LEFT JOIN primary_sector ps ON ps.prijimatel_id = s.id
    GROUP BY s.id, s.nazov, s.ico, pf.nazovsk, s.adresa_obec, ps.sector
"""

PROCUREMENT_CONTRACTS_SQL = f"""
    CREATE OR REPLACE TABLE {DATAMART_SCHEMA}.procurement_contracts AS
    SELECT
        z.id AS contract_id,
        z.kod AS contract_kod,
        z.nazov AS contract_nazov,
        z.cislozmluvy,
        z.celkovasumazmluvy,
        z.sumabezdph,
        TRY_CAST(z.datumucinnosti AS BIGINT) AS datumucinnosti,
        TRY_CAST(z.datumplatnosti AS BIGINT) AS datumplatnosti,
        z.urlodkaznazmluvu AS url_zmluva,
        COALESCE(sup_s.nazov, sup_d.nazov) AS supplier_nazov,
        COALESCE(sup_s.ico, sup_d.ico) AS supplier_ico,
        vo.id AS procurement_id,
        vo.kod AS procurement_kod,
        vo.nazov AS procurement_nazov,
        vo.stav AS procurement_stav,
        cpv.nazovsk AS cpv_nazov,
        proj.id AS project_id,
        proj.kod AS project_kod,
        proj.nazov AS project_nazov,
        prog.skratka AS program_skratka
    FROM {SLOVAKIA_SCHEMA}.itms21_zmluvaverejneobstaravanie z
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_subjekt sup_s ON sup_s.id = z.hlavnydodavatelsubjekt_id
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_dodavatelobstaravatel sup_d ON sup_d.id = z.hlavnydodavateldodavatelobstaravatel_id
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_verejneobstaravanie_detail vo ON vo.id = z.verejneobstaravanie_id
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_ciselniky_detail cpv
        ON cpv.ciselnik_kod = '1049' AND cpv.id = vo.hlavnypredmethlavnyslovnik_id
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_verejneobstaravanie_detail_projekty link
        ON link.verejneobstaravanie_detail_id = vo.id
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_projekt proj ON proj.id = link.id
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_program prog ON prog.id = proj.program_id
"""

PAYMENT_DISBURSEMENTS_SQL = f"""
    CREATE OR REPLACE TABLE {DATAMART_SCHEMA}.payment_disbursements AS
    SELECT
        z.id AS zop_id,
        z.kod,
        z.typ,
        z.projekt_id,
        p.kod AS project_kod,
        p.nazov AS project_nazov,
        p.program_id,
        prog.skratka AS program_skratka,
        s.nazov AS prijimatel_nazov,
        z.narokovanasuma,
        COALESCE(
            TRY_CAST(z.datumuhrady AS BIGINT),
            TRY_CAST(z.datumprijatia AS BIGINT),
            TRY_CAST(z.createdat AS BIGINT)
        ) AS event_date,
        z.neuhradena,
        z.zopjezaverecna
    FROM {SLOVAKIA_SCHEMA}.itms21_zop z
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_projekt p ON p.id = z.projekt_id
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_program prog ON prog.id = p.program_id
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_subjekt s ON s.id = z.prijimatel_id
"""

MART_TABLE_COMMENTS: dict[str, str] = {
    "regional_funding": (
        "One row per project. Fully rebuilt on every run from "
        "slovakia.itms21_projekt joined to itms21_projekt_miestorealizaciefull "
        "(Slovak locations only) and itms21_projekt_financnyplan (EU vs. SR "
        "split). kraj_names lists every distinct region the project runs in; "
        "region_count > 1 marks a 'nationwide' project - see "
        "datamart.regional_summary for the region-level rollup that avoids "
        "double-counting those into any one region's total."
    ),
    "regional_summary": (
        "One row per Slovak region (kraj), plus one synthetic 'nationwide' "
        "row (nuts3_id IS NULL) for projects implemented in more than one "
        "region. Sums only region_count = 1 rows from "
        "datamart.regional_funding, so multi-region projects never inflate "
        "a single region's total."
    ),
    "program_summary": (
        "One row per Operational Programme. project_count and zazmluvnene_* "
        "are computed live from slovakia.itms21_projekt / "
        "datamart.regional_funding; alokacia_* is the programme's total "
        "allocation ceiling from itms21_program, not what is actually "
        "contracted to projects - the two are expected to differ."
    ),
    "beneficiary_funding": (
        "One row per beneficiary (subjekt) that is the recipient on at "
        "least one project. Fully rebuilt on every run. primary_sector is "
        "the most frequent hospodarskaCinnost across the beneficiary's "
        "projects, not an exhaustive list of all sectors they operate in."
    ),
    "procurement_contracts": (
        "One row per (public-procurement contract x project it serves) - a "
        "contract with no linked project, or a procedure that funds "
        "multiple projects, both appear here (LEFT JOIN), so a contract can "
        "have more than one row. Fully rebuilt on every run. "
        "supplier_nazov/supplier_ico resolve the two mutually-exclusive "
        "supplier FKs on itms21_zmluvaverejneobstaravanie into a single "
        "pair of columns."
    ),
    "payment_disbursements": (
        "One row per payment request (itms21_zop). Fully rebuilt on every "
        "run. event_date is COALESCE(datumuhrady, datumprijatia, createdat) "
        "normalized to a single epoch-millisecond value, since the source "
        "zop table stores these as VARCHAR text containing epoch-ms digits."
    ),
}

MART_COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    "regional_funding": {
        "project_id": "Id of the project (itms21_projekt.id).",
        "project_kod": "Project code.",
        "project_nazov": "Project name.",
        "program_id": "Id of the parent programme (itms21_program.id).",
        "program_skratka": "Abbreviation of the parent programme.",
        "program_nazov": "Name of the parent programme in Slovak.",
        "prijimatel_nazov": "Name of the beneficiary implementing the project.",
        "prijimatel_ico": "Company registration number (ICO) of the beneficiary.",
        "region_count": "Number of distinct NUTS3 regions this project is implemented in.",
        "kraj_names": "Comma-separated, alphabetically sorted list of every distinct Slovak region (kraj) this project is implemented in.",
        "suma_eu": "EU-fund contribution, computed from the project's financial plan, in euro.",
        "suma_sr": "Slovak national co-financing contribution, computed from the project's financial plan, in euro.",
        "suma_spolu": "Total contribution (EU + national), in euro.",
        "celkova_zazmluvnena_suma": "Total contracted project value, in euro.",
        "poskytnute_prostriedky": "Funds disbursed to the project so far, in euro.",
        "stav": "Project status.",
        "ukonceny": "Whether the project is completed.",
        "vrealizacii": "Whether the project is currently being implemented.",
        "planovany_zaciatok": "Planned implementation start date (epoch milliseconds).",
        "planovany_koniec": "Planned implementation end date (epoch milliseconds).",
        "created_at": "Record creation timestamp in the source system (epoch milliseconds).",
        "updated_at": "Record last-updated timestamp in the source system (epoch milliseconds).",
    },
    "regional_summary": {
        "nuts3_id": "NUTS3 region id (itms21_ciselniky_detail, ciselnik_kod 1006), NULL for the synthetic nationwide row.",
        "kraj_nazov": "Decoded Slovak region (kraj) name, or 'Celoštátne (viacregionálne)' for the nationwide row.",
        "project_count": "Number of projects attributed to this region (or, for the nationwide row, implemented in more than one region).",
        "suma_eu": "Total EU-fund contribution of those projects, in euro.",
        "suma_sr": "Total Slovak national co-financing contribution of those projects, in euro.",
        "suma_spolu": "Total contribution (EU + national) of those projects, in euro.",
    },
    "program_summary": {
        "program_id": "Id of the programme (itms21_program.id).",
        "kod": "Programme code.",
        "skratka": "Programme abbreviation.",
        "nazov_sk": "Programme name in Slovak.",
        "nazov_en": "Programme name in English.",
        "typ_programu": "Programme type (e.g. national operational programme, cross-border cooperation).",
        "riadiaci_organ": "Name of the managing authority responsible for the programme.",
        "subjekt_nazov": "Name of the institution acting as the managing authority.",
        "project_count": "Number of projects funded under the programme (live count from itms21_projekt).",
        "alokacia_eu": "Total EU-fund allocation ceiling for the programme, in euro.",
        "alokacia_sr": "Total Slovak national co-financing allocation ceiling, in euro.",
        "alokacia_spolu": "Total programme allocation ceiling (EU + national), in euro.",
        "zazmluvnene_eu": "EU-fund contribution actually contracted to projects under this programme, in euro.",
        "zazmluvnene_sr": "Slovak national co-financing actually contracted to projects under this programme, in euro.",
        "zazmluvnene_spolu": "Total contribution actually contracted to projects under this programme, in euro.",
    },
    "beneficiary_funding": {
        "subjekt_id": "Id of the beneficiary (itms21_subjekt.id).",
        "nazov": "Beneficiary name.",
        "ico": "Slovak company registration number (ICO).",
        "pravnaforma_nazov": "Decoded legal form (itms21_ciselniky_detail, ciselnik_kod 1009).",
        "adresa_obec": "Municipality of the beneficiary's registered address, as stored (free text).",
        "primary_sector": "Most frequent economic activity (hospodarska cinnost) across the beneficiary's projects.",
        "project_count": "Number of projects this beneficiary received funding for.",
        "suma_eu": "Total EU-fund contribution across all their projects, in euro.",
        "suma_sr": "Total Slovak national co-financing contribution across all their projects, in euro.",
        "suma_spolu": "Total contribution (EU + national) across all their projects, in euro.",
        "first_project_year": "Planned start year of their earliest project.",
        "last_project_year": "Planned start year of their most recent project.",
    },
    "procurement_contracts": {
        "contract_id": "Id of the contract (itms21_zmluvaverejneobstaravanie.id).",
        "contract_kod": "Contract code.",
        "contract_nazov": "Contract name.",
        "cislozmluvy": "Contract number.",
        "celkovasumazmluvy": "Total contract value, in euro.",
        "sumabezdph": "Contract value excluding VAT, in euro.",
        "datumucinnosti": "Contract effective date (epoch milliseconds).",
        "datumplatnosti": "Contract validity date (epoch milliseconds).",
        "url_zmluva": "URL of the published contract document.",
        "supplier_nazov": "Supplier/contractor name.",
        "supplier_ico": "Supplier/contractor company registration number (ICO), when available.",
        "procurement_id": "Id of the parent procurement procedure (itms21_verejneobstaravanie_detail.id).",
        "procurement_kod": "Procurement procedure code.",
        "procurement_nazov": "Procurement procedure name.",
        "procurement_stav": "Procurement procedure status.",
        "cpv_nazov": "Main CPV category name (itms21_ciselniky_detail, ciselnik_kod 1049).",
        "project_id": "Id of the project this contract serves, when linked.",
        "project_kod": "Project code.",
        "project_nazov": "Project name.",
        "program_skratka": "Abbreviation of the project's parent programme.",
    },
    "payment_disbursements": {
        "zop_id": "Id of the payment request (itms21_zop.id).",
        "kod": "Payment request code.",
        "typ": "Payment request type, e.g. ZÁLOHA (advance), REFUNDÁCIA (reimbursement), PREDFINANCOVANIE (pre-financing).",
        "projekt_id": "Id of the project this payment request belongs to.",
        "project_kod": "Project code.",
        "project_nazov": "Project name.",
        "program_id": "Id of the project's parent programme.",
        "program_skratka": "Abbreviation of the project's parent programme.",
        "prijimatel_nazov": "Name of the beneficiary submitting the payment request.",
        "narokovanasuma": "Amount claimed in this payment request, in euro.",
        "event_date": "Best-available date for this payment request (paid, else received, else created), epoch milliseconds.",
        "neuhradena": "Whether the payment request is still unpaid.",
        "zopjezaverecna": "Whether this is the project's final payment request.",
    },
}

# (table_name, create_sql, json_filename, json_key, order_by, epoch_ms_columns)
# Order matters: regional_summary and program_summary both read from the
# already-materialized datamart.regional_funding table.
MARTS: list[tuple[str, str, str, str, str, list[str]]] = [
    ("regional_funding", REGIONAL_FUNDING_SQL, "regional_funding_data.json", "regional_funding",
     "suma_spolu DESC", ["planovany_zaciatok", "planovany_koniec", "created_at", "updated_at"]),
    ("regional_summary", REGIONAL_SUMMARY_SQL, "regional_summary_data.json", "regions",
     "suma_spolu DESC", []),
    ("program_summary", PROGRAM_SUMMARY_SQL, "program_summary_data.json", "programs",
     "zazmluvnene_spolu DESC", []),
    ("beneficiary_funding", BENEFICIARY_FUNDING_SQL, "beneficiary_funding_data.json", "beneficiaries",
     "suma_spolu DESC", []),
    ("procurement_contracts", PROCUREMENT_CONTRACTS_SQL, "procurement_contracts_data.json", "contracts",
     "celkovasumazmluvy DESC", ["datumucinnosti", "datumplatnosti"]),
    ("payment_disbursements", PAYMENT_DISBURSEMENTS_SQL, "payment_disbursements_data.json", "payments",
     "event_date DESC NULLS LAST", ["event_date"]),
]


def apply_comments(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Attach English COMMENT ON metadata to a datamart table + its columns."""
    con.execute(f"COMMENT ON TABLE {DATAMART_SCHEMA}.{table} IS '{_esc(MART_TABLE_COMMENTS[table])}'")
    for column, comment in MART_COLUMN_COMMENTS[table].items():
        con.execute(f"COMMENT ON COLUMN {DATAMART_SCHEMA}.{table}.{column} IS '{_esc(comment)}'")


def build_marts(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DATAMART_SCHEMA}")
    for table, sql, _, _, _, _ in MARTS:
        con.execute(sql)
        apply_comments(con, table)


def export_mart(con: duckdb.DuckDBPyConnection, table: str, json_filename: str,
                 json_key: str, order_by: str, epoch_ms_columns: list[str]) -> None:
    df = con.execute(f"SELECT * FROM {DATAMART_SCHEMA}.{table} ORDER BY {order_by}").fetchdf()
    for col in epoch_ms_columns:
        df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")
    out_path = DOCS_DIR / json_filename
    out_path.parent.mkdir(parents=True, exist_ok=True)
    export = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        json_key: json.loads(df.to_json(orient="records", date_format="iso")),
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False)
    print(f"Exported {len(df)} rows to {out_path}")


def main():
    con = duckdb.connect(str(DB_PATH))
    build_marts(con)
    for table, _, json_filename, json_key, order_by, epoch_ms_columns in MARTS:
        export_mart(con, table, json_filename, json_key, order_by, epoch_ms_columns)
    con.close()


if __name__ == "__main__":
    main()
