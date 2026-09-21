# EU Funds Slovakia — Datamart-Only Site Rebuild Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retire the `website` DuckDB schema and its two JSON exports, replace them with a `datamart` schema (6 tables) built purely from `slovakia`-schema data, and rebuild every `docs/` page onto those datamart exports — same brand system, verified responsive.

**Architecture:** One script, `scripts/build_datamart.py`, builds all 6 `datamart` tables with plain SQL over `slovakia` and exports each to a `docs/*_data.json` file. `fetch_programs.py`/`fetch_projects.py` lose their `website`-schema half but keep their `slovakia`-schema sync untouched. 7 static pages (4 rebuilt, 3 new-since-last-plan, 1 deleted) consume the new JSON client-side with Chart.js + vanilla JS — no backend, no build step, matching every existing page.

**Tech Stack:** Python 3.12, `duckdb`, `pandas` (already in `requirements.txt`); vanilla JS + Chart.js 4.4.1 (CDN), no bundler.

Design spec: `docs/superpowers/specs/2026-09-21-eufundsslovakia-rebuild-design.md`
Verified SQL reference: `docs/superpowers/specs/project_funding.sql`

## Global Constraints

- No new Python dependencies — `duckdb`, `pandas`, `json`, `pathlib`, `datetime` only.
- `scripts/build_datamart.py` makes **zero** HTTP requests.
- Every new DuckDB table and column gets `COMMENT ON` documentation.
- `itms21_ciselniky_detail` is always joined on the **composite** `(ciselnik_kod, id)`.
- Reuse `docs/styles.css` classes verbatim (`.topbar`, `.card`, `.filters`/`.filter-group`, `.multiselect`/`.ms-*`, `table`/`.table-scroll`, `.pagination`, `.hint`, `.chart-wrap`), Montserrat font, existing EU-blue/gold palette. No new colors or fonts.
- Reuse `docs/projects_logic.js` generic helpers (`sortData`, `paginate`, `topNByAmount`, `distinctPrograms`) via `<script src="projects_logic.js">` rather than reimplementing.
- Every page's shared nav (`.nav-links`) has exactly these 7 links, in this order: Programs, Projects, Top 50 Projects, Regional Funding, Beneficiaries, Procurement, Disbursements.
- Cross-device: every rebuilt/new page must be checked at ~390px, ~768px, ~1440px widths with no page-level horizontal overflow (tables may scroll internally via `.table-scroll`).
- All `datamart.*` JSON exports are written compact (no `indent=2`) — measured on the largest export (payment_disbursements, 22,970 rows): pretty-printed was 11.5MB/1.43MB gzip vs. compact 9.2MB/1.36MB gzip, a free size win with zero frontend changes (still one `fetch().then(r=>r.json())`, still native JSON types). CSV was considered (would gzip to ~1.07MB) and rejected: this data is free-text Slovak with commas/quotes, so a hand-rolled CSV parser is real new error surface, and it would break the one-JSON-fetch convention every existing page shares.

---

### Task 1: `scripts/build_datamart.py` — all 6 datamart tables + JSON exports

**Files:**
- Create: `scripts/build_datamart.py`
- Create: `scripts/_verify_datamart.py` (temporary, deleted in this task's last step)
- Create: `docs/regional_funding_data.json`, `docs/regional_summary_data.json`, `docs/program_summary_data.json`, `docs/beneficiary_funding_data.json`, `docs/procurement_contracts_data.json`, `docs/payment_disbursements_data.json` (all generated, not hand-written)

**Interfaces:**
- Produces: `datamart.regional_funding` (columns: `project_id, project_kod, project_nazov, program_id, program_skratka, program_nazov, prijimatel_nazov, prijimatel_ico, region_count, kraj_names, suma_eu, suma_sr, suma_spolu, celkova_zazmluvnena_suma, poskytnute_prostriedky, stav, ukonceny, vrealizacii, planovany_zaciatok, planovany_koniec, created_at, updated_at`).
- Produces: `datamart.regional_summary` (columns: `nuts3_id, kraj_nazov, project_count, suma_eu, suma_sr, suma_spolu`).
- Produces: `datamart.program_summary` (columns: `program_id, kod, skratka, nazov_sk, nazov_en, typ_programu, riadiaci_organ, subjekt_nazov, project_count, alokacia_eu, alokacia_sr, alokacia_spolu, zazmluvnene_eu, zazmluvnene_sr, zazmluvnene_spolu`).
- Produces: `datamart.beneficiary_funding` (columns: `subjekt_id, nazov, ico, pravnaforma_nazov, adresa_obec, primary_sector, project_count, suma_eu, suma_sr, suma_spolu, first_project_year, last_project_year`).
- Produces: `datamart.procurement_contracts` (columns: `contract_id, contract_kod, contract_nazov, cislozmluvy, celkovasumazmluvy, sumabezdph, datumucinnosti, datumplatnosti, url_zmluva, supplier_nazov, supplier_ico, procurement_id, procurement_kod, procurement_nazov, procurement_stav, cpv_nazov, project_id, project_kod, project_nazov, program_skratka`).
- Produces: `datamart.payment_disbursements` (columns: `zop_id, kod, typ, projekt_id, project_kod, project_nazov, program_id, program_skratka, prijimatel_nazov, narokovanasuma, event_date, neuhradena, zopjezaverecna`).
- Produces: module-level `MARTS: list[tuple[str, str, str, str, str, list[str]]]` (table_name, create_sql, json_filename, json_key, order_by, epoch_ms_columns), `apply_comments(con, table)`, `build_marts(con)`, `export_mart(con, table, json_filename, json_key, order_by, epoch_ms_columns)`, `main()`.

- [ ] **Step 1: Write a verification script that expects all 6 tables to exist**

Create `scripts/_verify_datamart.py`:

```python
"""Ad-hoc verification for scripts/build_datamart.py, run manually during
implementation. Not part of the monthly pipeline."""
import json
import duckdb

con = duckdb.connect("data/eufunds.duckdb", read_only=True)

# --- regional_funding ---
total, missing_start = con.execute(
    "SELECT count(*), sum(CASE WHEN planovany_zaciatok IS NULL THEN 1 ELSE 0 END) "
    "FROM datamart.regional_funding"
).fetchone()
print(f"regional_funding: {total} rows, {missing_start} missing planovany_zaciatok")
assert total == 4375, "expected one row per project (4,375)"
assert missing_start == 0, "planovany_zaciatok should be populated on every project"

with open("docs/regional_funding_data.json", encoding="utf-8") as f:
    payload = json.load(f)
assert "generated_at" in payload
assert len(payload["regional_funding"]) == total
print("regional_funding JSON export OK:", len(payload["regional_funding"]), "rows")

# --- regional_summary ---
region_rows = con.execute("SELECT count(*) FROM datamart.regional_summary").fetchone()[0]
region_project_sum, single_region_count = con.execute(
    "SELECT sum(project_count), (SELECT sum(CASE WHEN region_count = 1 THEN 1 ELSE 0 END) FROM datamart.regional_funding) "
    "FROM datamart.regional_summary"
).fetchone()
print(f"regional_summary: {region_rows} rows, project_count sums to {region_project_sum}")
assert region_rows == 9, "expected 8 kraje + 1 nationwide row"
assert region_project_sum == total, "regional_summary project_count must reconcile to all projects"

with open("docs/regional_summary_data.json", encoding="utf-8") as f:
    payload = json.load(f)
assert len(payload["regions"]) == region_rows
print("regional_summary JSON export OK:", len(payload["regions"]), "rows")

# --- program_summary ---
program_rows, program_project_sum = con.execute(
    "SELECT count(*), sum(project_count) FROM datamart.program_summary"
).fetchone()
print(f"program_summary: {program_rows} rows, project_count sums to {program_project_sum}")
assert program_project_sum == total, "program_summary project_count must reconcile to all projects"

with open("docs/program_summary_data.json", encoding="utf-8") as f:
    payload = json.load(f)
assert len(payload["programs"]) == program_rows
print("program_summary JSON export OK:", len(payload["programs"]), "rows")

# --- beneficiary_funding ---
total_ben = con.execute("SELECT count(*) FROM datamart.beneficiary_funding").fetchone()[0]
print(f"beneficiary_funding: {total_ben} rows")
assert total_ben == 2121, "expected 2,121 distinct beneficiaries"

with open("docs/beneficiary_funding_data.json", encoding="utf-8") as f:
    payload = json.load(f)
assert len(payload["beneficiaries"]) == total_ben
print("beneficiary_funding JSON export OK:", len(payload["beneficiaries"]), "rows")

# --- procurement_contracts ---
total_contracts, no_supplier = con.execute(
    "SELECT count(*), sum(CASE WHEN supplier_nazov IS NULL THEN 1 ELSE 0 END) "
    "FROM datamart.procurement_contracts"
).fetchone()
print(f"procurement_contracts: {total_contracts} rows, {no_supplier} missing supplier")
assert total_contracts == 7714, "expected 7,714 contract-project link rows"
assert no_supplier == 0, "every contract must resolve a supplier"

with open("docs/procurement_contracts_data.json", encoding="utf-8") as f:
    payload = json.load(f)
assert len(payload["contracts"]) == total_contracts
print("procurement_contracts JSON export OK:", len(payload["contracts"]), "rows")

# --- payment_disbursements ---
total_zop, null_project = con.execute(
    "SELECT count(*), sum(CASE WHEN projekt_id IS NULL THEN 1 ELSE 0 END) "
    "FROM datamart.payment_disbursements"
).fetchone()
print(f"payment_disbursements: {total_zop} rows, {null_project} missing project")
assert total_zop == 22970, "expected 22,970 payment requests"
assert null_project == 0, "projekt_id is populated on every row"

with open("docs/payment_disbursements_data.json", encoding="utf-8") as f:
    payload = json.load(f)
assert len(payload["payments"]) == total_zop
print("payment_disbursements JSON export OK:", len(payload["payments"]), "rows")

print("ALL DATAMART CHECKS PASSED")
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `python scripts/_verify_datamart.py`
Expected: FAIL with `Catalog Error: Table with name regional_funding does not exist` (the `datamart` schema doesn't exist yet).

- [ ] **Step 3: Write `scripts/build_datamart.py`**

```python
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
```

- [ ] **Step 4: Run the build script, then re-run verification**

Run: `python scripts/build_datamart.py && python scripts/_verify_datamart.py`
Expected: both commands print success output ending in `ALL DATAMART CHECKS PASSED` with no assertion errors.

- [ ] **Step 5: Delete the scratch verification script and commit**

```bash
git rm scripts/_verify_datamart.py
git add scripts/build_datamart.py docs/regional_funding_data.json docs/regional_summary_data.json docs/program_summary_data.json docs/beneficiary_funding_data.json docs/procurement_contracts_data.json docs/payment_disbursements_data.json
git commit -m "Add build_datamart.py: 6 datamart tables + JSON exports from slovakia schema"
```

---

### Task 2: Retire the `website` schema and its 2 JSON files

**Files:**
- Modify: `data/eufunds.duckdb` (one-time schema drop, not a script)
- Delete: `docs/program_data.json`, `docs/project_data.json`

**Interfaces:** none — this is a one-time cleanup step, run manually.

- [ ] **Step 1: Confirm nothing in `datamart` depends on `website`**

Run:
```bash
python -c "
import duckdb
con = duckdb.connect('data/eufunds.duckdb', read_only=True)
print(con.execute(\"SELECT sql FROM duckdb_views() WHERE sql ILIKE '%website%'\").fetchall())
print(con.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='datamart'\").fetchall())
"
```
Expected: empty view list, and the 6 table names from Task 1 (`build_datamart.py` was verified in Task 1 to read only from `slovakia`, so this is a confirmation, not a discovery step).

- [ ] **Step 2: Drop the `website` schema**

Run:
```bash
python -c "
import duckdb
con = duckdb.connect('data/eufunds.duckdb')
con.execute('DROP SCHEMA IF EXISTS website CASCADE')
con.close()
print('website schema dropped')
"
```
Expected: prints `website schema dropped` with no error.

- [ ] **Step 3: Delete the retired JSON exports**

```bash
git rm docs/program_data.json docs/project_data.json
git commit -m "Retire the website schema and its program_data.json/project_data.json exports"
```

---

### Task 3: Strip website-schema logic out of `scripts/fetch_programs.py`

**Files:**
- Modify: `scripts/fetch_programs.py`

**Interfaces:** none — pure removal, no new callers. `ensure_full_schema`, `store_full_detail`, `get_known_ids`, `sync_programs`'s detail-fetch loop, and the `slovakia.itms21_program`/`itms21_program_graf` sync are all unchanged and depended on by `datamart.program_summary`.

- [ ] **Step 1: Remove the website-schema constants and JSON export path**

In `scripts/fetch_programs.py`, delete lines 28-29 and 32-33 (`WEBSITE_SCHEMA`, `JSON_OUT_PATH`, `TABLE_CURRENT`, `TABLE_CURRENT_FQ`):

```python
WEBSITE_SCHEMA = "website"  # schema for tables that back the live website, e.g. programs_current
JSON_OUT_PATH = Path("docs/program_data.json")
```
```python
TABLE_CURRENT = f"{TABLE_PREFIX}programs_current"
TABLE_CURRENT_FQ = f"{WEBSITE_SCHEMA}.{TABLE_CURRENT}"
```

- [ ] **Step 2: Remove `flatten_program`, `migrate_to_website_schema`, `ensure_table`, `upsert_row`, `export_to_json`**

Delete the function bodies at lines 81-109 (`flatten_program`), 266-322 (`migrate_to_website_schema` + `ensure_table`), 382-411 (`upsert_row`), and 471-493 (`export_to_json`).

- [ ] **Step 3: Remove `TABLE_CURRENT_FQ` entries from `TABLE_COMMENTS`/`COLUMN_COMMENTS`**

In `TABLE_COMMENTS` (around line 161) delete the `TABLE_CURRENT_FQ: (...)` entry. In `COLUMN_COMMENTS` (around line 182) delete the `TABLE_CURRENT_FQ: {...}` entry.

- [ ] **Step 4: Strip `sync_programs` down to the normalized-schema sync only**

Replace the body of `sync_programs` (the version with `ensure_table(con)` and the `upsert_row` loop) with:

```python
def sync_programs() -> tuple[int, int, int, int]:
    """Fetch the list, then fetch full detail only for ids not yet stored in
    the normalized itms21_program schema (purely additive).

    Returns (total_in_list, total_in_list, detail_fetched_count, detail_failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
    ensure_full_schema(con)
    apply_comments(con)

    print("Fetching program list...")
    records = fetch_programs()
    print(f"List returned {len(records)} programs.")

    known_ids = get_known_ids(con)
    to_fetch = [record.get("id") for record in records if record.get("id") not in known_ids]
    print(f"{len(to_fetch)} new program(s) to fetch full detail for; "
          f"{len(records) - len(to_fetch)} already stored (skipped).")

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
                store_full_detail(con, detail)
                fetched_count += 1

                if i % 50 == 0 or i == len(to_fetch):
                    con.commit()
                    print(f"  Progress: {i}/{len(to_fetch)} processed "
                          f"({fetched_count} ok, {failed_count} failed)")

    con.commit()
    con.close()

    return len(records), len(records), fetched_count, failed_count
```

- [ ] **Step 5: Strip `main` down to just `sync_programs`**

```python
def main():
    total, synced, detail_fetched, detail_failed = sync_programs()
    print(f"Done. {total} total in list, {synced} synced, "
          f"{detail_fetched} newly fetched detail record(s), {detail_failed} failed.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Update the module docstring**

Replace the docstring at the top of the file (lines 1-11):

```python
"""
Fetches the current program list from api.itms21.sk and, for any program id
not yet stored, fetches full detail and decomposes it into the normalized
itms21_program / itms21_program_graf tables in the slovakia schema. Purely
additive: once a program id is stored it is never re-fetched, updated, or
deleted.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""
```

- [ ] **Step 7: Run it against the local DB and confirm it still works**

Run: `python scripts/fetch_programs.py`
Expected: exits 0, prints `Done. ... total in list, ... synced, 0 newly fetched detail record(s), 0 failed.` (all program ids should already be known from prior runs, so 0 new fetches — no network calls needed to verify this path).

- [ ] **Step 8: Commit**

```bash
git add scripts/fetch_programs.py
git commit -m "Strip website-schema sync out of fetch_programs.py"
```

---

### Task 4: Strip website-schema logic out of `scripts/fetch_projects.py`

**Files:**
- Modify: `scripts/fetch_projects.py`

**Interfaces:** none — pure removal. `ensure_full_schema`, `store_full_detail`, `get_known_ids`, and the `slovakia.itms21_projekt`/`itms21_projekt_*` child-table sync are unchanged and depended on by every `datamart` table.

- [ ] **Step 1: Remove the website-schema constants and JSON export path**

Delete lines 39-40 and 43-44:

```python
WEBSITE_SCHEMA = "website"  # schema for tables that back the live website, e.g. projects_current
JSON_OUT_PATH = Path("docs/project_data.json")
```
```python
TABLE_CURRENT = f"{TABLE_PREFIX}projects_current"
TABLE_CURRENT_FQ = f"{WEBSITE_SCHEMA}.{TABLE_CURRENT}"
```

- [ ] **Step 2: Remove `extract_eu_sr`, `flatten_project`, `migrate_to_website_schema`, `ensure_table`, `upsert_row`, `export_to_json`**

Delete the function bodies at lines 86-97 (`extract_eu_sr`), 100-126 (`flatten_project`), 137-196 (`migrate_to_website_schema` + `ensure_table`), 960-994 (`upsert_row`), and 1052-1078 (`export_to_json`).

- [ ] **Step 3: Remove `TABLE_CURRENT_FQ` entries from `TABLE_COMMENTS`/`COLUMN_COMMENTS`**

Delete the `TABLE_CURRENT_FQ: (...)` entry from `TABLE_COMMENTS` (around line 454) and the `TABLE_CURRENT_FQ: {...}` entry from `COLUMN_COMMENTS` (around line 519).

- [ ] **Step 4: Strip `sync_projects` down to the normalized-schema sync only**

Replace the body (currently calling `ensure_table(con)`, `flatten_project`, and `upsert_row` inside the fetch loop):

```python
def sync_projects() -> tuple[int, int, int]:
    """Fetch the list, then fetch full details only for ids not already
    stored in the normalized itms21_projekt schema.

    Existing rows are left untouched (no re-fetch) and nothing is ever
    deleted, even if a project drops out of the current API list.

    Returns (total_in_list, fetched_count, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    con.execute(f"SET schema = '{DB_SCHEMA}'")
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
                store_full_detail(con, detail)
                fetched_count += 1

                if i % 50 == 0 or i == len(to_fetch):
                    con.commit()
                    print(f"  Progress: {i}/{len(to_fetch)} processed "
                          f"({fetched_count} ok, {failed_count} failed)")

    con.commit()
    con.close()

    return len(list_items), fetched_count, failed_count
```

- [ ] **Step 5: Strip `main` down to just `sync_projects`**

```python
def main():
    total, fetched, failed = sync_projects()
    print(f"Done. {total} total in list, {fetched} newly fetched, {failed} failed.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Update the module docstring**

```python
"""
Fetches the project list from api.itms21.sk, detects which project ids are
not yet stored in DuckDB, and fetches full details ONLY for those new ids.
Projects already stored are never re-fetched and never deleted, even if
they disappear from the API list -- the table only ever grows (purely
additive incremental sync).

Each detail fetched is decomposed into a normalized PROJEKT table plus
~30 PROJEKT_* child/grandchild tables (one row per inner list item, e.g.
PROJEKT_FINANCNYPLAN, PROJEKT_AKTIVITY, PROJEKT_ZMENAPROJEKT_DOKUMENT...).
Every one of those tables carries a PROJECT_ID (and, where the item is
nested two levels deep, its immediate parent row id) so they can all be
joined back to PROJEKT. Rows are inserted once and never updated/deleted,
gated by the same "id not yet known" check as PROJEKT itself.

Run monthly via GitHub Actions (.github/workflows/monthly.yml).
"""
```

- [ ] **Step 7: Run it against the local DB and confirm it still works**

Run: `python scripts/fetch_projects.py`
Expected: exits 0, prints `Done. ... total in list, 0 newly fetched, 0 failed.`

- [ ] **Step 8: Commit**

```bash
git add scripts/fetch_projects.py
git commit -m "Strip website-schema sync out of fetch_projects.py"
```

---

### Task 5: Wire `build_datamart.py` into the monthly workflow

**Files:**
- Modify: `.github/workflows/monthly.yml`

**Interfaces:**
- Consumes: `scripts/build_datamart.py`'s `main()` (no arguments).

- [ ] **Step 1: Add the build step after every fetch script**

In `.github/workflows/monthly.yml`, append one line after `python scripts/fetch_dodavatelobstaravatel.py` (currently the last line of the `Run fetch scripts` step, `.github/workflows/monthly.yml:61`):

```yaml
          python scripts/fetch_dodavatelobstaravatel.py
          python scripts/build_datamart.py
```

- [ ] **Step 2: Replace the `git add` list in the commit step**

Replace line 77 (`git add docs/program_data.json docs/project_data.json`) with:

```yaml
          git add docs/regional_funding_data.json docs/regional_summary_data.json docs/program_summary_data.json docs/beneficiary_funding_data.json docs/procurement_contracts_data.json docs/payment_disbursements_data.json
```

- [ ] **Step 3: Verify the YAML is well-formed**

Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/monthly.yml', encoding='utf-8'))"`
Expected: no output, exit code 0 (`pip install pyyaml` first if `ModuleNotFoundError` — a throwaway check, not a new project dependency).

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/monthly.yml
git commit -m "Wire build_datamart.py into the monthly workflow, drop retired JSON files from git add"
```

---

### Task 6: `docs/datamart_logic.js` shared helpers

**Files:**
- Create: `docs/datamart_logic.js`

**Interfaces:**
- Produces (on `window.DatamartLogic` in-browser / `module.exports` in Node): `groupSum(data, keyFn, sumFields, labelFn)`, `withCumulative(sortedRows, field, cumulativeField)`, `dedupeBy(data, keyFn)`, `filterByProgram(data, programList)`, `monthKey(epochMsOrIso)`.

- [ ] **Step 1: Write a Node smoke-test script**

Create `scripts/_verify_datamart_logic.js` (temporary, deleted in this task's last step):

```javascript
const DatamartLogic = require('../docs/datamart_logic.js');
const assert = require('assert');

const rows = [
  { kraj: 'A', amount: 10, project_id: 1 },
  { kraj: 'A', amount: 20, project_id: 2 },
  { kraj: 'B', amount: 5, project_id: 3 },
];

const grouped = DatamartLogic.groupSum(rows, r => r.kraj, ['amount']);
assert.deepStrictEqual(grouped.map(g => g.key), ['A', 'B']);
assert.strictEqual(grouped[0].amount, 30);
assert.strictEqual(grouped[0].count, 2);

const withCum = DatamartLogic.withCumulative(
  [{ month: '2026-01', total: 10 }, { month: '2026-02', total: 5 }],
  'total', 'cumulative'
);
assert.deepStrictEqual(withCum.map(r => r.cumulative), [10, 15]);

const deduped = DatamartLogic.dedupeBy(
  [{ project_id: 1, v: 1 }, { project_id: 1, v: 2 }, { project_id: 2, v: 3 }],
  r => r.project_id
);
assert.strictEqual(deduped.length, 2);

const filtered = DatamartLogic.filterByProgram(
  [{ program_skratka: 'PSK' }, { program_skratka: 'AMIF' }],
  ['PSK']
);
assert.strictEqual(filtered.length, 1);

assert.strictEqual(DatamartLogic.monthKey('2026-03-15T10:00:00.000Z'), '2026-03');
assert.strictEqual(DatamartLogic.monthKey(1741996800000), '2026-03');

console.log('datamart_logic.js: all assertions passed');
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `node scripts/_verify_datamart_logic.js`
Expected: FAIL with `Cannot find module '../docs/datamart_logic.js'`.

- [ ] **Step 3: Write `docs/datamart_logic.js`**

```javascript
// Shared logic for the datamart explorer pages -- pure functions, testable
// in Node and browser alike, mirroring projects_logic.js's structure.
// In the browser this file is loaded as a plain <script>, exposing
// window.DatamartLogic. In Node (for testing) it's exported via module.exports.

(function (root) {
  function groupSum(data, keyFn, sumFields, labelFn) {
    const map = new Map();
    data.forEach(function (row) {
      const key = keyFn(row);
      if (!map.has(key)) {
        const entry = { key: key, label: labelFn ? labelFn(row) : key, count: 0 };
        sumFields.forEach(function (f) { entry[f] = 0; });
        map.set(key, entry);
      }
      const entry = map.get(key);
      entry.count += 1;
      sumFields.forEach(function (f) { entry[f] += Number(row[f]) || 0; });
    });
    return Array.from(map.values()).sort(function (a, b) { return b[sumFields[0]] - a[sumFields[0]]; });
  }

  function withCumulative(sortedRows, field, cumulativeField) {
    let running = 0;
    return sortedRows.map(function (row) {
      running += Number(row[field]) || 0;
      const copy = Object.assign({}, row);
      copy[cumulativeField] = running;
      return copy;
    });
  }

  function dedupeBy(data, keyFn) {
    const seen = new Set();
    const out = [];
    data.forEach(function (row) {
      const key = keyFn(row);
      if (!seen.has(key)) {
        seen.add(key);
        out.push(row);
      }
    });
    return out;
  }

  function filterByProgram(data, programList) {
    if (!programList || programList.length === 0) return data;
    return data.filter(function (row) { return programList.includes(row.program_skratka); });
  }

  function monthKey(value) {
    const d = new Date(value);
    const year = d.getUTCFullYear();
    const month = String(d.getUTCMonth() + 1).padStart(2, '0');
    return year + '-' + month;
  }

  const api = { groupSum, withCumulative, dedupeBy, filterByProgram, monthKey };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  } else {
    root.DatamartLogic = api;
  }
})(typeof window !== 'undefined' ? window : globalThis);
```

- [ ] **Step 4: Run the smoke test again, then delete it**

Run: `node scripts/_verify_datamart_logic.js`
Expected: prints `datamart_logic.js: all assertions passed`.

Then: `rm scripts/_verify_datamart_logic.js`

- [ ] **Step 5: Commit**

```bash
git add docs/datamart_logic.js
git commit -m "Add shared datamart_logic.js helpers for the explorer pages"
```

---

### Task 7: `docs/projects_logic.js` field rename (`kod`/`nazov` → `project_kod`/`project_nazov`)

**Files:**
- Modify: `docs/projects_logic.js`

**Interfaces:**
- Produces: `applyFilters(data, filters)` — same signature, now reads `row.project_kod`/`row.project_nazov` instead of `row.kod`/`row.nazov` (the `filters.kod`/`filters.nazov` keys themselves are unchanged, they're just filter-input names, not row field names).

`datamart.regional_funding` (Task 1) names these columns `project_kod`/`project_nazov` (matching the approved design spec), not `kod`/`nazov` like the retired `website.itms21_projects_current` did. This task updates the one shared helper that reads them; Tasks 9-10 update the two pages that consume it.

- [ ] **Step 1: Update `applyFilters`**

In `docs/projects_logic.js`, change:

```javascript
      if (filters.kod) {
        if (!(row.kod || '').toLowerCase().includes(filters.kod.toLowerCase())) return false;
      }
      if (filters.nazov) {
        if (!(row.nazov || '').toLowerCase().includes(filters.nazov.toLowerCase())) return false;
      }
```

to:

```javascript
      if (filters.kod) {
        if (!(row.project_kod || '').toLowerCase().includes(filters.kod.toLowerCase())) return false;
      }
      if (filters.nazov) {
        if (!(row.project_nazov || '').toLowerCase().includes(filters.nazov.toLowerCase())) return false;
      }
```

- [ ] **Step 2: Write a Node smoke-test confirming the new field names are read**

Create `scripts/_verify_projects_logic.js` (temporary, deleted at the end of this task):

```javascript
const ProjectsLogic = require('../docs/projects_logic.js');
const assert = require('assert');

const rows = [
  { project_kod: 'ABC123', project_nazov: 'Bridge repair', program_skratka: 'PSK' },
  { project_kod: 'XYZ999', project_nazov: 'School renovation', program_skratka: 'PSK' },
];

const byKod = ProjectsLogic.applyFilters(rows, { kod: 'abc' });
assert.strictEqual(byKod.length, 1);
assert.strictEqual(byKod[0].project_kod, 'ABC123');

const byNazov = ProjectsLogic.applyFilters(rows, { nazov: 'school' });
assert.strictEqual(byNazov.length, 1);
assert.strictEqual(byNazov[0].project_kod, 'XYZ999');

console.log('projects_logic.js: field rename verified');
```

- [ ] **Step 3: Run it, then delete it**

Run: `node scripts/_verify_projects_logic.js`
Expected: prints `projects_logic.js: field rename verified`.

Then: `rm scripts/_verify_projects_logic.js`

- [ ] **Step 4: Commit**

```bash
git add docs/projects_logic.js
git commit -m "Rename projects_logic.js filter fields to project_kod/project_nazov"
```

---

### Task 8: `docs/assets/slovakia_kraje.svg` + `docs/regional_funding.html`

**Files:**
- Create: `docs/assets/slovakia_kraje.svg`
- Create: `docs/regional_funding.html`
- Modify: `docs/styles.css` (add `.drilldown-title`, `.drilldown-scroll`, `.kraj-map-wrap`, `.kraj-legend`)

**Interfaces:**
- Consumes: `docs/regional_funding_data.json` (`{generated_at, regional_funding: [...]}`), `docs/regional_summary_data.json` (`{generated_at, regions: [...]}`), `ProjectsLogic.distinctPrograms/sortData`, `DatamartLogic.filterByProgram/dedupeBy`.

The choropleth is a deliberately simplified schematic (8 labeled tiles arranged to loosely evoke Slovakia's real west→east layout — north row: Žilinský/Prešovský, middle row: Trenčiansky/Banskobystrický/Košický, south row: Bratislavský/Trnavský/Nitriansky), not a traced geographic boundary — this keeps every region a simple, precisely-clickable/colorable shape rather than requiring complex multi-point path data. It can be swapped for a traced-boundary SVG later without touching the JS, since the JS only depends on each shape having `id="kraj-{nuts3_id}"`.

- [ ] **Step 1: Write `docs/assets/slovakia_kraje.svg`**

```xml
<svg viewBox="0 0 760 340" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Map of Slovak regions">
  <rect id="kraj-13" class="kraj-shape" x="230" y="20" width="140" height="70" rx="10" data-name="Žilinský kraj"/>
  <text x="300" y="60" class="kraj-label">ZA</text>

  <rect id="kraj-15" class="kraj-shape" x="530" y="20" width="140" height="70" rx="10" data-name="Prešovský kraj"/>
  <text x="600" y="60" class="kraj-label">PO</text>

  <rect id="kraj-11" class="kraj-shape" x="70" y="130" width="120" height="70" rx="10" data-name="Trenčiansky kraj"/>
  <text x="130" y="170" class="kraj-label">TN</text>

  <rect id="kraj-14" class="kraj-shape" x="280" y="130" width="180" height="70" rx="10" data-name="Banskobystrický kraj"/>
  <text x="370" y="170" class="kraj-label">BB</text>

  <rect id="kraj-16" class="kraj-shape" x="560" y="130" width="120" height="70" rx="10" data-name="Košický kraj"/>
  <text x="620" y="170" class="kraj-label">KE</text>

  <rect id="kraj-9" class="kraj-shape" x="20" y="240" width="110" height="70" rx="10" data-name="Bratislavský kraj"/>
  <text x="75" y="280" class="kraj-label">BA</text>

  <rect id="kraj-10" class="kraj-shape" x="160" y="240" width="110" height="70" rx="10" data-name="Trnavský kraj"/>
  <text x="215" y="280" class="kraj-label">TT</text>

  <rect id="kraj-12" class="kraj-shape" x="300" y="240" width="140" height="70" rx="10" data-name="Nitriansky kraj"/>
  <text x="370" y="280" class="kraj-label">NR</text>
</svg>
```

- [ ] **Step 2: Add CSS rules**

Append to `docs/styles.css` (after the `.pagination` rule):

```css
.drilldown-title {
  font-family: var(--font-display);
  color: var(--navy-base);
  font-size: 16px;
  font-weight: 600;
  margin: 28px 0 12px 0;
}
.drilldown-scroll { max-height: 420px; overflow-y: auto; }

.kraj-map-wrap { max-width: 720px; margin: 0 auto; }
.kraj-map-wrap svg { width: 100%; height: auto; display: block; }
.kraj-shape { stroke: #ffffff; stroke-width: 2; cursor: pointer; transition: opacity 0.15s; }
.kraj-shape:hover { opacity: 0.85; }
.kraj-label {
  font-family: var(--font-display);
  font-size: 20px;
  font-weight: 700;
  fill: #ffffff;
  text-anchor: middle;
  dominant-baseline: middle;
  pointer-events: none;
}
.kraj-legend { display: flex; align-items: center; justify-content: center; gap: 10px; margin-top: 14px; font-size: 12px; color: var(--muted); }
.kraj-legend-scale { width: 160px; height: 12px; border-radius: 6px; background: linear-gradient(90deg, #cfd9f0, #003399); }

@media (max-width: 768px) {
  .kraj-label { font-size: 15px; }
}
```

- [ ] **Step 3: Write `docs/regional_funding.html`**

```html
<!DOCTYPE html>
<html lang="sk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Regional Funding — Slovakia 2021–2027</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="styles.css?v=4">
</head>
<body>
<div class="topbar">
  <a class="brand" href="index.html">
    <svg class="brand-mark" width="22" height="22" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
      <circle cx="12" cy="12" r="11" fill="#003399"/>
      <g fill="#FFCC00">
        <circle cx="12" cy="4" r="1.15"/><circle cx="18.5" cy="7.8" r="1.15"/><circle cx="18.5" cy="16.2" r="1.15"/>
        <circle cx="12" cy="20" r="1.15"/><circle cx="5.5" cy="16.2" r="1.15"/><circle cx="5.5" cy="7.8" r="1.15"/>
      </g>
    </svg>
    EU Funds Slovakia
  </a>
  <nav class="nav-links">
    <a href="index.html">Programs</a>
    <a href="projects.html">Projects</a>
    <a href="top_projects_chart.html">Top 50 Projects</a>
    <a class="current" href="regional_funding.html">Regional Funding</a>
    <a href="beneficiaries.html">Beneficiaries</a>
    <a href="procurement.html">Procurement</a>
    <a href="disbursements.html">Disbursements</a>
  </nav>
</div>
<div class="card">
  <h1>Regional Funding</h1>
  <span class="title-rule"></span>
  <p class="subtitle">Contracted funding by Slovak region (kraj)</p>
  <p class="updated" id="updated-label">Loading...</p>
  <p class="hint">Tip: click or tap a region to see its projects below. Nationwide programmes (implemented in more than one region) are excluded from the map and shown as a separate total, since attributing them to one region would be misleading.</p>

  <div class="filters" style="grid-template-columns: 1fr;">
    <div class="filter-group">
      <label for="programToggle">Program</label>
      <div class="multiselect" id="programMulti">
        <button type="button" class="ms-toggle" id="programToggle" aria-haspopup="true" aria-expanded="false">
          <span id="programToggleLabel">All programs</span><span class="ms-caret">▾</span>
        </button>
        <div class="ms-panel" id="programPanel" role="dialog" aria-label="Filter by program">
          <div class="ms-actions">
            <button type="button" id="programSelectAll">Select all</button>
            <button type="button" id="programClearAll">Clear</button>
          </div>
          <div class="ms-options" id="programOptions"></div>
        </div>
      </div>
    </div>
  </div>

  <p class="error" style="display:none;" id="error-box"></p>

  <div class="kraj-map-wrap" id="mapWrap"></div>
  <div class="kraj-legend">
    <span id="legendMin">€0</span>
    <span class="kraj-legend-scale"></span>
    <span id="legendMax">€0</span>
  </div>

  <p class="hint" id="nationwideCallout" style="display:none;"></p>

  <h2 class="drilldown-title" id="drilldownTitle" style="display:none;"></h2>
  <div class="table-scroll drilldown-scroll" id="drilldownWrap" style="display:none;">
    <table id="drilldownTable">
      <thead>
        <tr>
          <th>Code</th><th>Project Name</th><th>Program</th><th>Recipient</th><th>Total (€)</th><th>Status</th>
        </tr>
      </thead>
      <tbody id="drilldownBody"></tbody>
    </table>
  </div>
</div>

<script src="projects_logic.js"></script>
<script src="datamart_logic.js"></script>
<script>
const REGIONAL_URL = 'regional_funding_data.json';
const SUMMARY_URL = 'regional_summary_data.json';
const SVG_URL = 'assets/slovakia_kraje.svg';

let allData = [];
let allSummary = [];
let allPrograms = [];
const selectedPrograms = new Set();

function formatIsoDate(iso) {
  const d = new Date(iso);
  return d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
}

function formatEUR(value) {
  if (value == null || Number.isNaN(Number(value))) return '';
  return '€' + Number(value).toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function formatEURShort(value) {
  if (value >= 1e9) return (value / 1e9).toFixed(2) + ' B';
  if (value >= 1e6) return (value / 1e6).toFixed(1) + ' M';
  if (value >= 1e3) return (value / 1e3).toFixed(0) + ' K';
  return Number(value).toLocaleString();
}

function updateProgramToggleLabel() {
  const label = document.getElementById('programToggleLabel');
  const count = selectedPrograms.size;
  if (count === 0) label.textContent = 'All programs';
  else if (count === 1) label.textContent = Array.from(selectedPrograms)[0];
  else label.textContent = count + ' programs selected';
}

function populateProgramDropdown(data) {
  const container = document.getElementById('programOptions');
  allPrograms = ProjectsLogic.distinctPrograms(data);
  container.innerHTML = '';
  allPrograms.forEach(function (p) {
    const id = 'prog_' + p.skratka.replace(/[^A-Za-z0-9_-]/g, '');
    const wrap = document.createElement('label');
    wrap.className = 'ms-option';
    wrap.title = p.nazov || p.skratka;
    wrap.innerHTML = '<input type="checkbox" id="' + id + '" value="' + p.skratka + '"><span>' + p.skratka + '</span>';
    const cb = wrap.querySelector('input');
    cb.addEventListener('change', function () {
      if (cb.checked) selectedPrograms.add(p.skratka);
      else selectedPrograms.delete(p.skratka);
      updateProgramToggleLabel();
      render();
    });
    container.appendChild(wrap);
  });
  updateProgramToggleLabel();
}

function renderDrilldown(nuts3Id, krajNazov) {
  const rows = allData.filter(function (r) { return r.region_count === 1 && r.kraj_names === krajNazov; });
  const filtered = DatamartLogic.filterByProgram(rows, Array.from(selectedPrograms));
  const sorted = ProjectsLogic.sortData(filtered, 'suma_spolu', 'desc', function () { return 'number'; });

  document.getElementById('drilldownTitle').textContent = 'Projects in ' + krajNazov + ' (' + sorted.length + ')';
  document.getElementById('drilldownTitle').style.display = 'block';
  document.getElementById('drilldownWrap').style.display = 'block';

  const tbody = document.getElementById('drilldownBody');
  tbody.innerHTML = '';
  sorted.forEach(function (row) {
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td>' + (row.project_kod || '') + '</td>' +
      '<td class="wrap">' + (row.project_nazov || '') + '</td>' +
      '<td>' + (row.program_skratka || '') + '</td>' +
      '<td class="wrap">' + (row.prijimatel_nazov || '') + '</td>' +
      '<td class="num">' + formatEUR(row.suma_spolu) + '</td>' +
      '<td>' + (row.stav || '') + '</td>';
    tbody.appendChild(tr);
  });
}

function colorForValue(value, min, max) {
  const t = max > min ? (value - min) / (max - min) : 0;
  const lo = [207, 217, 240];
  const hi = [0, 51, 153];
  const rgb = lo.map(function (c, i) { return Math.round(c + (hi[i] - c) * t); });
  return 'rgb(' + rgb.join(',') + ')';
}

function render() {
  const programFilteredRegional = DatamartLogic.filterByProgram(allData, Array.from(selectedPrograms));

  const nationwideProjects = DatamartLogic.dedupeBy(
    programFilteredRegional.filter(function (r) { return r.region_count > 1; }),
    function (r) { return r.project_id; }
  );
  const nationwideTotal = nationwideProjects.reduce(function (sum, r) { return sum + (Number(r.suma_spolu) || 0); }, 0);
  const callout = document.getElementById('nationwideCallout');
  if (nationwideProjects.length > 0) {
    callout.style.display = 'block';
    callout.textContent = nationwideProjects.length + ' nationwide project(s) (implemented in more than one region) ' +
      'total €' + formatEURShort(nationwideTotal) + ', not shown on the map above.';
  } else {
    callout.style.display = 'none';
  }

  // When no programs are selected (the default), the pre-aggregated
  // regional_summary export is used directly. Selecting programs
  // re-aggregates from the per-project regional_funding rows client-side.
  let byRegion;
  if (selectedPrograms.size === 0) {
    byRegion = allSummary.filter(function (r) { return r.nuts3_id != null; });
  } else {
    const singleRegion = programFilteredRegional.filter(function (r) { return r.region_count === 1; });
    byRegion = DatamartLogic.groupSum(singleRegion, function (r) { return r.kraj_names; }, ['suma_spolu'])
      .map(function (g) { return { kraj_nazov: g.key, suma_spolu: g.suma_spolu }; });
  }

  const values = byRegion.map(function (r) { return r.suma_spolu; });
  const min = values.length ? Math.min.apply(null, values) : 0;
  const max = values.length ? Math.max.apply(null, values) : 0;
  document.getElementById('legendMin').textContent = '€' + formatEURShort(min);
  document.getElementById('legendMax').textContent = '€' + formatEURShort(max);

  const byNameOrId = new Map();
  allSummary.forEach(function (r) { if (r.nuts3_id != null) byNameOrId.set(String(r.nuts3_id), r.kraj_nazov); });

  document.querySelectorAll('.kraj-shape').forEach(function (shape) {
    const nuts3Id = shape.id.replace('kraj-', '');
    const krajNazov = byNameOrId.get(nuts3Id) || shape.getAttribute('data-name');
    const match = byRegion.find(function (r) { return r.kraj_nazov === krajNazov; });
    const value = match ? match.suma_spolu : 0;
    shape.setAttribute('fill', colorForValue(value, min, max));
    shape.setAttribute('data-value', value);
    shape.onclick = function () { renderDrilldown(nuts3Id, krajNazov); };
    shape.setAttribute('tabindex', '0');
    shape.setAttribute('role', 'button');
    shape.setAttribute('aria-label', krajNazov + ': €' + Number(value).toLocaleString());
    shape.onkeydown = function (evt) { if (evt.key === 'Enter' || evt.key === ' ') shape.onclick(); };
    shape.onmousemove = function (evt) { shape.setAttribute('title', krajNazov + ': €' + Number(value).toLocaleString(undefined, { maximumFractionDigits: 0 })); };
  });
}

const programMulti = document.getElementById('programMulti');
const programToggle = document.getElementById('programToggle');
programToggle.addEventListener('click', function (e) {
  e.stopPropagation();
  programMulti.classList.toggle('open');
  programToggle.setAttribute('aria-expanded', programMulti.classList.contains('open') ? 'true' : 'false');
});
document.addEventListener('click', function (e) {
  if (!programMulti.contains(e.target)) programMulti.classList.remove('open');
});
document.getElementById('programSelectAll').addEventListener('click', function () {
  selectedPrograms.clear();
  allPrograms.forEach(function (p) { selectedPrograms.add(p.skratka); });
  document.querySelectorAll('#programOptions input[type="checkbox"]').forEach(function (cb) { cb.checked = true; });
  updateProgramToggleLabel();
  render();
});
document.getElementById('programClearAll').addEventListener('click', function () {
  selectedPrograms.clear();
  document.querySelectorAll('#programOptions input[type="checkbox"]').forEach(function (cb) { cb.checked = false; });
  updateProgramToggleLabel();
  render();
});

Promise.all([
  fetch(SVG_URL).then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status + ' loading ' + SVG_URL); return r.text(); }),
  fetch(REGIONAL_URL).then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status + ' loading ' + REGIONAL_URL); return r.json(); }),
  fetch(SUMMARY_URL).then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status + ' loading ' + SUMMARY_URL); return r.json(); }),
]).then(function (results) {
  document.getElementById('mapWrap').innerHTML = results[0];
  allData = results[1].regional_funding;
  allSummary = results[2].regions;
  document.getElementById('updated-label').textContent = 'Last updated: ' + formatIsoDate(results[1].generated_at);
  populateProgramDropdown(allData);
  render();
}).catch(function (err) {
  document.getElementById('updated-label').textContent = '';
  const box = document.getElementById('error-box');
  box.style.display = 'block';
  box.textContent = 'Could not load region data: ' + err.message;
});
</script>
</body>
</html>
```

- [ ] **Step 4: Serve the site locally and verify**

Run (in background): `python -m http.server --directory docs 8000`

Using the Playwright browser tool, navigate to `http://localhost:8000/regional_funding.html`. Confirm: the "Last updated" label shows a real timestamp, the 8-tile map renders with distinct colors, hovering a tile shows a tooltip with its amount, clicking a tile shows a "Projects in `<kraj>`" drill-down table below, and the nationwide-projects hint shows a nonzero count. Resize the viewport to ~390px and confirm the map and legend still fit without horizontal page overflow.

- [ ] **Step 5: Commit**

```bash
git add docs/regional_funding.html docs/styles.css docs/assets/slovakia_kraje.svg
git commit -m "Add regional_funding.html with SVG choropleth of the 8 kraje"
```

---

### Task 9: Rebuild `docs/index.html` on `program_summary`

**Files:**
- Modify: `docs/index.html`

**Interfaces:**
- Consumes: `docs/program_summary_data.json` (`{generated_at, programs: [...]}`).

- [ ] **Step 1: Replace the table's `<thead>` columns**

Replace lines 62-70 (the `<thead>` block):

```html
        <thead>
          <tr>
            <th data-key="skratka" data-type="string">Code</th>
            <th data-key="nazov_sk" data-type="string">Program</th>
            <th data-key="typ_programu" data-type="string">Type</th>
            <th data-key="project_count" data-type="number">Projects</th>
            <th data-key="zazmluvnene_eu" data-type="number">Contracted EU (€)</th>
            <th data-key="zazmluvnene_sr" data-type="number">Contracted SR (€)</th>
            <th data-key="zazmluvnene_spolu" data-type="number">Contracted Total (€)</th>
            <th data-key="subjekt_nazov" data-type="string">Managing Body</th>
          </tr>
        </thead>
```

- [ ] **Step 2: Add a chart-mode toggle next to the existing scale toggle**

Replace lines 77-85 (the `.chart-section` block):

```html
  <div class="chart-section">
    <div class="scale-toggle">
      <label><input type="radio" name="metric" value="volume" checked> Funding volume (€)</label>
      <label><input type="radio" name="metric" value="count"> Project count</label>
    </div>
    <div class="scale-toggle">
      <label><input type="radio" name="scale" value="linear" checked> Linear scale</label>
      <label><input type="radio" name="scale" value="logarithmic"> Log scale (recommended — PSK dwarfs the rest)</label>
    </div>
    <div class="chart-wrap">
      <canvas id="fundingChart"></canvas>
    </div>
  </div>
```

- [ ] **Step 3: Update the script block**

Replace the entire `<script>` block (lines 89-283) with:

```html
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script>
const HAS_CHART = typeof Chart !== 'undefined';
if (HAS_CHART) {
  Chart.defaults.font.family = "'Montserrat', Arial, Helvetica, sans-serif";
}

const JSON_URL = 'program_summary_data.json';
let currentData = [];
let sortKey = 'zazmluvnene_spolu';
let sortDir = 'desc';
let fundingChart = null;
let chartMetric = 'volume';

function formatEUR(value) {
  if (value == null) return '';
  return '€' + Number(value).toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function formatDate(iso) {
  const d = new Date(iso);
  return d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
}

function goToProjects(skratka) {
  window.location.href = 'projects.html?program=' + encodeURIComponent(skratka);
}

function renderTable(data) {
  const tbody = document.getElementById('table-body');
  tbody.innerHTML = '';
  data.forEach(function(row) {
    const tr = document.createElement('tr');
    tr.title = 'Click to view projects for ' + row.skratka;
    tr.addEventListener('click', function() { goToProjects(row.skratka); });
    tr.innerHTML =
      '<td>' + (row.skratka || '') + '</td>' +
      '<td>' + (row.nazov_sk || '') + '</td>' +
      '<td>' + (row.typ_programu || '') + '</td>' +
      '<td class="num">' + (row.project_count || 0) + '</td>' +
      '<td class="num">' + formatEUR(row.zazmluvnene_eu) + '</td>' +
      '<td class="num">' + formatEUR(row.zazmluvnene_sr) + '</td>' +
      '<td class="num">' + formatEUR(row.zazmluvnene_spolu) + '</td>' +
      '<td>' + (row.subjekt_nazov || '') + '</td>';
    tbody.appendChild(tr);
  });
}

function formatEURShort(value) {
  if (value >= 1e9) return (value / 1e9).toFixed(2) + ' B';
  if (value >= 1e6) return (value / 1e6).toFixed(1) + ' M';
  return Number(value).toLocaleString();
}

function renderChart(data) {
  if (!HAS_CHART) {
    document.querySelector('.chart-section').style.display = 'none';
    return;
  }
  const sortField = chartMetric === 'count' ? 'project_count' : 'zazmluvnene_spolu';
  const sorted = [...data].sort(function(a, b) { return b[sortField] - a[sortField]; });

  const labels = sorted.map(function(d) { return d.skratka; });
  const fullNames = sorted.map(function(d) { return d.nazov_sk; });

  const ctx = document.getElementById('fundingChart').getContext('2d');
  if (fundingChart) fundingChart.destroy();

  const datasets = chartMetric === 'count'
    ? [{ label: 'Projects', data: sorted.map(function(d) { return d.project_count; }), backgroundColor: '#003399', borderRadius: 3 }]
    : [
        { label: 'EU contribution', data: sorted.map(function(d) { return d.zazmluvnene_eu; }), backgroundColor: '#003399', borderRadius: 3 },
        { label: 'National (SR) contribution', data: sorted.map(function(d) { return d.zazmluvnene_sr; }), backgroundColor: '#7C93C7', borderRadius: 3 }
      ];

  fundingChart = new Chart(ctx, {
    type: 'bar',
    data: { labels: labels, datasets: datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      onClick: function(evt, elements) {
        if (elements.length > 0) goToProjects(labels[elements[0].index]);
      },
      plugins: {
        legend: { position: 'top' },
        tooltip: {
          callbacks: {
            title: function(items) { return fullNames[items[0].dataIndex]; },
            label: function(item) {
              return chartMetric === 'count'
                ? item.dataset.label + ': ' + item.raw
                : item.dataset.label + ': €' + item.raw.toLocaleString(undefined, { maximumFractionDigits: 0 });
            }
          }
        }
      },
      scales: {
        x: { stacked: chartMetric !== 'count' },
        y: {
          stacked: chartMetric !== 'count',
          type: chartMetric === 'count' ? 'linear' : document.querySelector('input[name="scale"]:checked').value,
          beginAtZero: true,
          ticks: {
            callback: function(value) {
              return chartMetric === 'count' ? value : '€' + formatEURShort(value);
            }
          },
          title: { display: true, text: chartMetric === 'count' ? 'Projects' : 'EUR' }
        }
      }
    }
  });
}

document.querySelectorAll('input[name="metric"]').forEach(function(radio) {
  radio.addEventListener('change', function(e) {
    chartMetric = e.target.value;
    renderChart(currentData);
  });
});

document.querySelectorAll('input[name="scale"]').forEach(function(radio) {
  radio.addEventListener('change', function(e) {
    if (!fundingChart || chartMetric === 'count') return;
    fundingChart.options.scales.y.type = e.target.value;
    fundingChart.options.scales.y.min = e.target.value === 'logarithmic' ? 1e6 : undefined;
    fundingChart.update();
  });
});

function sortData(data, key, dir) {
  const type = document.querySelector('th[data-key="' + key + '"]').dataset.type;
  const sorted = [...data].sort(function(a, b) {
    let av = a[key], bv = b[key];
    if (type === 'number') {
      av = Number(av) || 0;
      bv = Number(bv) || 0;
      return dir === 'asc' ? av - bv : bv - av;
    }
    av = (av || '').toString().toLowerCase();
    bv = (bv || '').toString().toLowerCase();
    if (av < bv) return dir === 'asc' ? -1 : 1;
    if (av > bv) return dir === 'asc' ? 1 : -1;
    return 0;
  });
  return sorted;
}

function applySortAndFilter() {
  const query = document.getElementById('searchBox').value.trim().toLowerCase();
  let filtered = currentData;
  if (query) {
    filtered = currentData.filter(function(row) {
      return (row.nazov_sk || '').toLowerCase().includes(query) ||
             (row.skratka || '').toLowerCase().includes(query);
    });
  }
  const sorted = sortData(filtered, sortKey, sortDir);
  renderTable(sorted);
}

document.querySelectorAll('th').forEach(function(th) {
  th.addEventListener('click', function() {
    const key = th.dataset.key;
    if (sortKey === key) {
      sortDir = sortDir === 'asc' ? 'desc' : 'asc';
    } else {
      sortKey = key;
      sortDir = 'asc';
    }
    document.querySelectorAll('th').forEach(function(t) {
      t.classList.remove('sorted-asc', 'sorted-desc');
    });
    th.classList.add(sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc');
    applySortAndFilter();
  });
});

document.getElementById('searchBox').addEventListener('input', applySortAndFilter);

fetch(JSON_URL)
  .then(function(response) {
    if (!response.ok) throw new Error('HTTP ' + response.status);
    return response.json();
  })
  .then(function(payload) {
    currentData = payload.programs;
    document.getElementById('updated-label').textContent =
      'Last updated: ' + formatDate(payload.generated_at);
    document.getElementById('dataTable').style.display = 'table';
    renderChart(currentData);
    applySortAndFilter();
  })
  .catch(function(err) {
    document.getElementById('updated-label').textContent = '';
    const box = document.getElementById('error-box');
    box.style.display = 'block';
    box.textContent = 'Could not load ' + JSON_URL + ': ' + err.message;
  });
</script>
```

- [ ] **Step 4: Serve locally and verify in a browser**

With `python -m http.server --directory docs 8000` running, navigate to `http://localhost:8000/index.html`. Confirm: the table shows programmes with a "Projects" column, the chart defaults to "Funding volume" (stacked EU/SR bars), switching to "Project count" redraws as a single-series bar chart, and clicking a bar/row navigates to `projects.html?program=<skratka>`.

- [ ] **Step 5: Commit**

```bash
git add docs/index.html
git commit -m "Rebuild index.html on datamart.program_summary, add project-count chart mode"
```

---

### Task 10: Rebuild `docs/projects.html` on `regional_funding`

**Files:**
- Modify: `docs/projects.html`

**Interfaces:**
- Consumes: `docs/regional_funding_data.json` (`{generated_at, regional_funding: [...]}`), `ProjectsLogic.distinctPrograms/applyFilters/sortData/paginate` (field names updated in Task 7).

- [ ] **Step 1: Update the table header**

Replace lines 116-135 (the `<thead>` row) with (renamed `kod`→`project_kod`, `nazov`→`project_nazov`, added a "Regions" column from `kraj_names`, dropped nothing else):

```html
          <tr>
            <th data-key="project_kod" data-type="string">Code</th>
            <th data-key="project_nazov" data-type="string">Project Name</th>
            <th data-key="program_skratka" data-type="string">Program</th>
            <th data-key="program_nazov" data-type="string">Program Name</th>
            <th data-key="prijimatel_nazov" data-type="string">Recipient</th>
            <th data-key="prijimatel_ico" data-type="string">ICO</th>
            <th data-key="kraj_names" data-type="string">Regions</th>
            <th data-key="stav" data-type="string">Status</th>
            <th data-key="vrealizacii" data-type="bool">In Progress</th>
            <th data-key="ukonceny" data-type="bool">Completed</th>
            <th data-key="suma_eu" data-type="number">EU (€)</th>
            <th data-key="suma_sr" data-type="number">SR (€)</th>
            <th data-key="suma_spolu" data-type="number">Total (€)</th>
            <th data-key="celkova_zazmluvnena_suma" data-type="number">Contracted (€)</th>
            <th data-key="poskytnute_prostriedky" data-type="number">Disbursed (€)</th>
            <th data-key="planovany_zaciatok" data-type="date">Planned Start</th>
            <th data-key="planovany_koniec" data-type="date">Planned End</th>
            <th data-key="created_at" data-type="date">Created</th>
            <th data-key="updated_at" data-type="date">Updated</th>
          </tr>
```

- [ ] **Step 2: Update `renderTableRows` for the renamed/new fields**

Replace lines 265-291 (`renderTableRows`):

```javascript
function renderTableRows(rows) {
  const tbody = document.getElementById('table-body');
  tbody.innerHTML = '';
  rows.forEach(function (row) {
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td>' + (row.project_kod || '') + '</td>' +
      '<td class="wrap">' + (row.project_nazov || '') + '</td>' +
      '<td>' + (row.program_skratka || '') + '</td>' +
      '<td class="wrap">' + (row.program_nazov || '') + '</td>' +
      '<td class="wrap">' + (row.prijimatel_nazov || '') + '</td>' +
      '<td>' + formatIcoLink(row.prijimatel_ico) + '</td>' +
      '<td class="wrap">' + (row.kraj_names || '') + '</td>' +
      '<td>' + (row.stav || '') + '</td>' +
      '<td>' + (row.vrealizacii ? 'Yes' : 'No') + '</td>' +
      '<td>' + (row.ukonceny ? 'Yes' : 'No') + '</td>' +
      '<td class="num">' + formatEUR(row.suma_eu) + '</td>' +
      '<td class="num">' + formatEUR(row.suma_sr) + '</td>' +
      '<td class="num">' + formatEUR(row.suma_spolu) + '</td>' +
      '<td class="num">' + formatEUR(row.celkova_zazmluvnena_suma) + '</td>' +
      '<td class="num">' + formatEUR(row.poskytnute_prostriedky) + '</td>' +
      '<td>' + formatDateValue(row.planovany_zaciatok) + '</td>' +
      '<td>' + formatDateValue(row.planovany_koniec) + '</td>' +
      '<td>' + formatDateValue(row.created_at) + '</td>' +
      '<td>' + formatDateValue(row.updated_at) + '</td>';
    tbody.appendChild(tr);
  });
}
```

- [ ] **Step 3: Update the JSON URL, payload key, and `nazov`-based URL param**

Change line 152 (`JSON_URL`):

```javascript
const JSON_URL = 'regional_funding_data.json';
```

Change line 452 (`allData = payload.projects;`):

```javascript
    allData = payload.regional_funding;
```

In `applyFiltersFromUrl` (line 428, `document.getElementById('activeFilterLabel').textContent = params.get('nazov') || kod;`) — no change needed, this reads the URL query param `nazov` set by `top_projects_chart.html`'s link, unrelated to the row field rename.

- [ ] **Step 4: Serve locally and verify in a browser**

With the local server running, navigate to `http://localhost:8000/projects.html`. Confirm: the table loads with a "Regions" column, sorting by "Code"/"Project Name" works, the Finstat ICO link still opens `finstat.sk/<ico>` in a new tab, and the date-range and program filters still work.

- [ ] **Step 5: Commit**

```bash
git add docs/projects.html
git commit -m "Rebuild projects.html on datamart.regional_funding"
```

---

### Task 11: Repoint `docs/top_projects_chart.html` to `regional_funding`

**Files:**
- Modify: `docs/top_projects_chart.html`

**Interfaces:**
- Consumes: `docs/regional_funding_data.json` (`{generated_at, regional_funding: [...]}`), `ProjectsLogic.topNByAmount`.

- [ ] **Step 1: Update `JSON_URL` and the payload/field accesses**

Change line 58:

```javascript
const JSON_URL = 'regional_funding_data.json';
```

In `renderTopChart` (around lines 90-93), change:

```javascript
  const codes = ordered.map(function (p) { return p.kod || ''; });

  const labels = ordered.map(function (p) {
    const name = p.nazov || p.kod || ('#' + p.project_id);
    return name.length > 55 ? name.slice(0, 52) + '…' : name;
  });
  const fullNames = ordered.map(function (p) { return p.nazov || ''; });
```

to:

```javascript
  const codes = ordered.map(function (p) { return p.project_kod || ''; });

  const labels = ordered.map(function (p) {
    const name = p.project_nazov || p.project_kod || ('#' + p.project_id);
    return name.length > 55 ? name.slice(0, 52) + '…' : name;
  });
  const fullNames = ordered.map(function (p) { return p.project_nazov || ''; });
```

In the final `fetch(JSON_URL)` handler (line 167), change:

```javascript
    const data = payload.projects;
```

to:

```javascript
    const data = payload.regional_funding;
```

- [ ] **Step 2: Serve locally and verify in a browser**

With the local server running, navigate to `http://localhost:8000/top_projects_chart.html`. Confirm: the horizontal bar chart renders 50 bars sorted by contracted amount descending, and clicking a bar navigates to `projects.html?kod=...&nazov=...` with that project shown.

- [ ] **Step 3: Commit**

```bash
git add docs/top_projects_chart.html
git commit -m "Repoint top_projects_chart.html to datamart.regional_funding"
```

---

### Task 12: `docs/beneficiaries.html` with Finstat linking

**Files:**
- Create: `docs/beneficiaries.html`

**Interfaces:**
- Consumes: `docs/beneficiary_funding_data.json` (`{generated_at, beneficiaries: [...]}`), `ProjectsLogic.sortData/paginate`, `DatamartLogic.groupSum`. Links out to `projects.html?recipient=<name>` and to `https://finstat.sk/<ico>`.

- [ ] **Step 1: Write `docs/beneficiaries.html`**

```html
<!DOCTYPE html>
<html lang="sk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Beneficiaries — Slovakia 2021–2027</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="styles.css?v=4">
<style>
  .card { max-width: 1300px; }
  table { min-width: 1100px; }
  .chart-wrap { height: 320px; margin-bottom: 28px; }
  @media (max-width: 480px) { table { min-width: 900px; } }
</style>
</head>
<body>
<div class="topbar">
  <a class="brand" href="index.html">
    <svg class="brand-mark" width="22" height="22" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
      <circle cx="12" cy="12" r="11" fill="#003399"/>
      <g fill="#FFCC00">
        <circle cx="12" cy="4" r="1.15"/><circle cx="18.5" cy="7.8" r="1.15"/><circle cx="18.5" cy="16.2" r="1.15"/>
        <circle cx="12" cy="20" r="1.15"/><circle cx="5.5" cy="16.2" r="1.15"/><circle cx="5.5" cy="7.8" r="1.15"/>
      </g>
    </svg>
    EU Funds Slovakia
  </a>
  <nav class="nav-links">
    <a href="index.html">Programs</a>
    <a href="projects.html">Projects</a>
    <a href="top_projects_chart.html">Top 50 Projects</a>
    <a href="regional_funding.html">Regional Funding</a>
    <a class="current" href="beneficiaries.html">Beneficiaries</a>
    <a href="procurement.html">Procurement</a>
    <a href="disbursements.html">Disbursements</a>
  </nav>
</div>
<div class="card">
  <h1>Beneficiaries</h1>
  <span class="title-rule"></span>
  <p class="subtitle">Every entity that has received EU funding, with legal form and sector</p>
  <p class="updated" id="updated-label">Loading...</p>
  <p class="hint">Tip: click a beneficiary's name to see their full project list, or their ICO to open their FinStat company profile in a new tab.</p>

  <div class="chart-wrap"><canvas id="legalFormChart"></canvas></div>

  <div class="filters" style="grid-template-columns: 1fr 1fr;">
    <div class="filter-group">
      <label for="filterName">Name or ICO</label>
      <input type="text" id="filterName" placeholder="Search...">
    </div>
    <div class="filter-group">
      <label for="filterLegalForm">Legal form</label>
      <select id="filterLegalForm"><option value="">All legal forms</option></select>
    </div>
  </div>

  <p class="error" style="display:none;" id="error-box"></p>

  <div class="table-scroll">
    <table id="dataTable" style="display:none;">
      <thead>
        <tr>
          <th data-key="nazov" data-type="string">Name</th>
          <th data-key="ico" data-type="string">ICO</th>
          <th data-key="pravnaforma_nazov" data-type="string">Legal Form</th>
          <th data-key="primary_sector" data-type="string">Primary Sector</th>
          <th data-key="project_count" data-type="number">Projects</th>
          <th data-key="suma_eu" data-type="number">EU (€)</th>
          <th data-key="suma_sr" data-type="number">SR (€)</th>
          <th data-key="suma_spolu" data-type="number">Total (€)</th>
          <th data-key="first_project_year" data-type="number">First Year</th>
          <th data-key="last_project_year" data-type="number">Last Year</th>
        </tr>
      </thead>
      <tbody id="table-body"></tbody>
    </table>
    <p class="empty" id="emptyMessage" style="display:none;">No beneficiaries match the current filters.</p>
  </div>

  <div class="pagination" id="pagination" style="display:none;">
    <button id="prevPage">← Previous</button>
    <span id="pageInfo"></span>
    <button id="nextPage">Next →</button>
  </div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="projects_logic.js"></script>
<script src="datamart_logic.js"></script>
<script>
const HAS_CHART = typeof Chart !== 'undefined';
if (HAS_CHART) Chart.defaults.font.family = "'Montserrat', Arial, Helvetica, sans-serif";

const JSON_URL = 'beneficiary_funding_data.json';
const PAGE_SIZE = 25;
let allData = [];
let currentPage = 1;
let sortKey = 'suma_spolu';
let sortDir = 'desc';

function typeOf(key) {
  const th = document.querySelector('th[data-key="' + key + '"]');
  return th ? th.dataset.type : 'string';
}

function formatEUR(value) {
  if (value == null || Number.isNaN(Number(value))) return '';
  return '€' + Number(value).toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function formatEURShort(value) {
  if (value >= 1e9) return (value / 1e9).toFixed(2) + ' B';
  if (value >= 1e6) return (value / 1e6).toFixed(1) + ' M';
  if (value >= 1e3) return (value / 1e3).toFixed(0) + ' K';
  return Number(value).toLocaleString();
}

function formatIsoDate(iso) {
  const d = new Date(iso);
  return d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
}

function escapeHtml(value) {
  return String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function recipientLink(nazov) {
  return '<a href="projects.html?recipient=' + encodeURIComponent(nazov) + '">' + escapeHtml(nazov) + '</a>';
}

function icoLink(ico) {
  if (!ico) return '';
  const safe = escapeHtml(ico);
  return '<a href="https://finstat.sk/' + encodeURIComponent(ico) + '" target="_blank" rel="noopener noreferrer">' + safe + '</a>';
}

function getFilters() {
  return {
    name: document.getElementById('filterName').value.trim().toLowerCase(),
    legalForm: document.getElementById('filterLegalForm').value,
  };
}

function applyFilters(data, filters) {
  return data.filter(function (row) {
    if (filters.name) {
      const hay = ((row.nazov || '') + ' ' + (row.ico || '')).toLowerCase();
      if (!hay.includes(filters.name)) return false;
    }
    if (filters.legalForm && row.pravnaforma_nazov !== filters.legalForm) return false;
    return true;
  });
}

function populateLegalFormFilter(data) {
  const select = document.getElementById('filterLegalForm');
  const forms = Array.from(new Set(data.map(function (r) { return r.pravnaforma_nazov; }).filter(Boolean))).sort();
  forms.forEach(function (f) {
    const opt = document.createElement('option');
    opt.value = f;
    opt.textContent = f;
    select.appendChild(opt);
  });
}

function renderChart(data) {
  if (!HAS_CHART) return;
  const grouped = DatamartLogic.groupSum(data, function (r) { return r.pravnaforma_nazov || 'Unknown'; }, ['suma_spolu']).slice(0, 10);
  new Chart(document.getElementById('legalFormChart').getContext('2d'), {
    type: 'bar',
    data: {
      labels: grouped.map(function (g) { return g.key; }),
      datasets: [{ label: 'Total funding (€)', data: grouped.map(function (g) { return g.suma_spolu; }), backgroundColor: '#003399', borderRadius: 3 }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true, ticks: { callback: function (v) { return '€' + formatEURShort(v); } } } }
    }
  });
}

function renderTableRows(rows) {
  const tbody = document.getElementById('table-body');
  tbody.innerHTML = '';
  rows.forEach(function (row) {
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td class="wrap">' + recipientLink(row.nazov || '') + '</td>' +
      '<td>' + icoLink(row.ico) + '</td>' +
      '<td>' + escapeHtml(row.pravnaforma_nazov || '') + '</td>' +
      '<td class="wrap">' + escapeHtml(row.primary_sector || '') + '</td>' +
      '<td class="num">' + (row.project_count || 0) + '</td>' +
      '<td class="num">' + formatEUR(row.suma_eu) + '</td>' +
      '<td class="num">' + formatEUR(row.suma_sr) + '</td>' +
      '<td class="num">' + formatEUR(row.suma_spolu) + '</td>' +
      '<td class="num">' + (row.first_project_year || '') + '</td>' +
      '<td class="num">' + (row.last_project_year || '') + '</td>';
    tbody.appendChild(tr);
  });
}

function renderPaginationControls(pageResult) {
  const pagination = document.getElementById('pagination');
  const table = document.getElementById('dataTable');
  const emptyMsg = document.getElementById('emptyMessage');
  if (pageResult.totalRows === 0) {
    table.style.display = 'none';
    pagination.style.display = 'none';
    emptyMsg.style.display = 'block';
    return;
  }
  table.style.display = 'table';
  emptyMsg.style.display = 'none';
  pagination.style.display = 'flex';
  document.getElementById('pageInfo').textContent =
    'Showing ' + pageResult.start + '–' + pageResult.end + ' of ' + pageResult.totalRows +
    ' (page ' + pageResult.page + ' of ' + pageResult.totalPages + ')';
  document.getElementById('prevPage').disabled = pageResult.page <= 1;
  document.getElementById('nextPage').disabled = pageResult.page >= pageResult.totalPages;
}

function render() {
  const filtered = applyFilters(allData, getFilters());
  const sorted = ProjectsLogic.sortData(filtered, sortKey, sortDir, typeOf);
  const pageResult = ProjectsLogic.paginate(sorted, currentPage, PAGE_SIZE);
  currentPage = pageResult.page;
  renderTableRows(pageResult.rows);
  renderPaginationControls(pageResult);
}

document.querySelectorAll('th').forEach(function (th) {
  th.addEventListener('click', function () {
    const key = th.dataset.key;
    if (sortKey === key) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
    else { sortKey = key; sortDir = 'asc'; }
    document.querySelectorAll('th').forEach(function (t) { t.classList.remove('sorted-asc', 'sorted-desc'); });
    th.classList.add(sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc');
    currentPage = 1;
    render();
  });
});

['filterName', 'filterLegalForm'].forEach(function (id) {
  document.getElementById(id).addEventListener('input', function () { currentPage = 1; render(); });
  document.getElementById(id).addEventListener('change', function () { currentPage = 1; render(); });
});
document.getElementById('prevPage').addEventListener('click', function () { currentPage -= 1; render(); });
document.getElementById('nextPage').addEventListener('click', function () { currentPage += 1; render(); });

fetch(JSON_URL)
  .then(function (response) {
    if (!response.ok) throw new Error('HTTP ' + response.status);
    return response.json();
  })
  .then(function (payload) {
    allData = payload.beneficiaries;
    document.getElementById('updated-label').textContent = 'Last updated: ' + formatIsoDate(payload.generated_at);
    populateLegalFormFilter(allData);
    renderChart(allData);
    render();
  })
  .catch(function (err) {
    document.getElementById('updated-label').textContent = '';
    const box = document.getElementById('error-box');
    box.style.display = 'block';
    box.textContent = 'Could not load ' + JSON_URL + ': ' + err.message;
  });
</script>
</body>
</html>
```

- [ ] **Step 2: Serve locally and verify in a browser**

With the local server running, navigate to `http://localhost:8000/beneficiaries.html`. Confirm: the legal-form bar chart renders, the table shows rows sorted by total descending, clicking a beneficiary name navigates to `projects.html?recipient=...`, and clicking an ICO opens `finstat.sk/<ico>` in a new tab.

- [ ] **Step 3: Commit**

```bash
git add docs/beneficiaries.html
git commit -m "Add beneficiaries.html with Finstat linking and project drill-through"
```

---

### Task 13: `docs/procurement.html` with per-supplier VO rollup

**Files:**
- Create: `docs/procurement.html`

**Interfaces:**
- Consumes: `docs/procurement_contracts_data.json` (`{generated_at, contracts: [...]}`), `ProjectsLogic.sortData/paginate`, `DatamartLogic.groupSum/dedupeBy`.

- [ ] **Step 1: Write `docs/procurement.html`**

```html
<!DOCTYPE html>
<html lang="sk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Procurement — Slovakia 2021–2027</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="styles.css?v=4">
<style>
  .card { max-width: 1300px; }
  table { min-width: 1200px; }
  .chart-wrap { height: 420px; margin-bottom: 28px; }
  @media (max-width: 480px) { table { min-width: 900px; } }
</style>
</head>
<body>
<div class="topbar">
  <a class="brand" href="index.html">
    <svg class="brand-mark" width="22" height="22" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
      <circle cx="12" cy="12" r="11" fill="#003399"/>
      <g fill="#FFCC00">
        <circle cx="12" cy="4" r="1.15"/><circle cx="18.5" cy="7.8" r="1.15"/><circle cx="18.5" cy="16.2" r="1.15"/>
        <circle cx="12" cy="20" r="1.15"/><circle cx="5.5" cy="16.2" r="1.15"/><circle cx="5.5" cy="7.8" r="1.15"/>
      </g>
    </svg>
    EU Funds Slovakia
  </a>
  <nav class="nav-links">
    <a href="index.html">Programs</a>
    <a href="projects.html">Projects</a>
    <a href="top_projects_chart.html">Top 50 Projects</a>
    <a href="regional_funding.html">Regional Funding</a>
    <a href="beneficiaries.html">Beneficiaries</a>
    <a class="current" href="procurement.html">Procurement</a>
    <a href="disbursements.html">Disbursements</a>
  </nav>
</div>
<div class="card">
  <h1>Procurement &amp; Suppliers</h1>
  <span class="title-rule"></span>
  <p class="subtitle">Public-procurement contracts and who wins them</p>
  <p class="updated" id="updated-label">Loading...</p>
  <p class="hint">Tip: click a supplier bar (or search by name) to see every procurement procedure they've won below.</p>

  <div class="chart-wrap"><canvas id="supplierChart"></canvas></div>

  <div class="filters" style="grid-template-columns: 1fr 1fr;">
    <div class="filter-group">
      <label for="filterSupplier">Supplier</label>
      <input type="text" id="filterSupplier" placeholder="Search by supplier...">
    </div>
    <div class="filter-group">
      <label for="filterStatus">Procedure status</label>
      <select id="filterStatus"><option value="">All statuses</option></select>
    </div>
    <div class="filters-actions">
      <button class="btn-secondary" id="resetFilters">Reset filters</button>
    </div>
  </div>

  <p class="error" style="display:none;" id="error-box"></p>

  <h2 class="drilldown-title" id="supplierVoTitle" style="display:none;"></h2>
  <div class="table-scroll drilldown-scroll" id="supplierVoWrap" style="display:none;">
    <table id="supplierVoTable">
      <thead>
        <tr><th>Procedure</th><th>Status</th><th>CPV Category</th><th>Project</th><th>Program</th></tr>
      </thead>
      <tbody id="supplierVoBody"></tbody>
    </table>
  </div>

  <div class="table-scroll">
    <table id="dataTable" style="display:none;">
      <thead>
        <tr>
          <th data-key="contract_nazov" data-type="string">Contract</th>
          <th data-key="supplier_nazov" data-type="string">Supplier</th>
          <th data-key="celkovasumazmluvy" data-type="number">Value (€)</th>
          <th data-key="procurement_nazov" data-type="string">Procedure</th>
          <th data-key="procurement_stav" data-type="string">Status</th>
          <th data-key="project_nazov" data-type="string">Project</th>
          <th data-key="program_skratka" data-type="string">Program</th>
          <th data-key="url_zmluva" data-type="string">Document</th>
        </tr>
      </thead>
      <tbody id="table-body"></tbody>
    </table>
    <p class="empty" id="emptyMessage" style="display:none;">No contracts match the current filters.</p>
  </div>

  <div class="pagination" id="pagination" style="display:none;">
    <button id="prevPage">← Previous</button>
    <span id="pageInfo"></span>
    <button id="nextPage">Next →</button>
  </div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="projects_logic.js"></script>
<script src="datamart_logic.js"></script>
<script>
const HAS_CHART = typeof Chart !== 'undefined';
if (HAS_CHART) Chart.defaults.font.family = "'Montserrat', Arial, Helvetica, sans-serif";

const JSON_URL = 'procurement_contracts_data.json';
const PAGE_SIZE = 25;
let allData = [];
let currentPage = 1;
let sortKey = 'celkovasumazmluvy';
let sortDir = 'desc';

function typeOf(key) {
  const th = document.querySelector('th[data-key="' + key + '"]');
  return th ? th.dataset.type : 'string';
}

function formatEUR(value) {
  if (value == null || Number.isNaN(Number(value))) return '';
  return '€' + Number(value).toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function formatEURShort(value) {
  if (value >= 1e9) return (value / 1e9).toFixed(2) + ' B';
  if (value >= 1e6) return (value / 1e6).toFixed(1) + ' M';
  if (value >= 1e3) return (value / 1e3).toFixed(0) + ' K';
  return Number(value).toLocaleString();
}

function formatIsoDate(iso) {
  const d = new Date(iso);
  return d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
}

function escapeHtml(value) {
  return String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function getFilters() {
  return {
    supplier: document.getElementById('filterSupplier').value.trim().toLowerCase(),
    status: document.getElementById('filterStatus').value,
  };
}

function applyFilters(data, filters) {
  return data.filter(function (row) {
    if (filters.supplier && !((row.supplier_nazov || '').toLowerCase().includes(filters.supplier))) return false;
    if (filters.status && row.procurement_stav !== filters.status) return false;
    return true;
  });
}

function populateStatusFilter(data) {
  const select = document.getElementById('filterStatus');
  const statuses = Array.from(new Set(data.map(function (r) { return r.procurement_stav; }).filter(Boolean))).sort();
  statuses.forEach(function (s) {
    const opt = document.createElement('option');
    opt.value = s;
    opt.textContent = s;
    select.appendChild(opt);
  });
}

function renderChart(data) {
  if (!HAS_CHART) return;
  const grouped = DatamartLogic.groupSum(
    data, function (r) { return r.supplier_ico || r.supplier_nazov; }, ['celkovasumazmluvy'],
    function (r) { return r.supplier_nazov; }
  ).slice(0, 20);

  new Chart(document.getElementById('supplierChart').getContext('2d'), {
    type: 'bar',
    data: {
      labels: grouped.map(function (g) { return (g.label || '').length > 40 ? g.label.slice(0, 37) + '…' : g.label; }),
      datasets: [{ label: 'Total contract value (€)', data: grouped.map(function (g) { return g.celkovasumazmluvy; }), backgroundColor: '#003399', borderRadius: 3 }]
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      onClick: function (evt, elements) {
        if (elements.length > 0) {
          document.getElementById('filterSupplier').value = grouped[elements[0].index].label || '';
          currentPage = 1;
          render();
        }
      },
      onHover: function (evt, elements) { evt.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default'; },
      plugins: { legend: { display: false } },
      scales: { x: { beginAtZero: true, ticks: { callback: function (v) { return '€' + formatEURShort(v); } } } }
    }
  });
}

function renderSupplierVoRollup(filtered, supplierQuery) {
  const wrap = document.getElementById('supplierVoWrap');
  const title = document.getElementById('supplierVoTitle');
  const tbody = document.getElementById('supplierVoBody');

  if (!supplierQuery) {
    wrap.style.display = 'none';
    title.style.display = 'none';
    return;
  }

  const vos = DatamartLogic.dedupeBy(filtered, function (r) { return r.procurement_id; });
  title.textContent = 'Procurement procedures for "' + supplierQuery + '" (' + vos.length + ')';
  title.style.display = 'block';
  wrap.style.display = 'block';

  tbody.innerHTML = '';
  vos.forEach(function (row) {
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td class="wrap">' + escapeHtml(row.procurement_nazov || '') + '</td>' +
      '<td>' + escapeHtml(row.procurement_stav || '') + '</td>' +
      '<td class="wrap">' + escapeHtml(row.cpv_nazov || '') + '</td>' +
      '<td class="wrap">' + escapeHtml(row.project_nazov || '') + '</td>' +
      '<td>' + escapeHtml(row.program_skratka || '') + '</td>';
    tbody.appendChild(tr);
  });
}

function renderTableRows(rows) {
  const tbody = document.getElementById('table-body');
  tbody.innerHTML = '';
  rows.forEach(function (row) {
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td class="wrap">' + escapeHtml(row.contract_nazov || '') + '</td>' +
      '<td class="wrap">' + escapeHtml(row.supplier_nazov || '') + '</td>' +
      '<td class="num">' + formatEUR(row.celkovasumazmluvy) + '</td>' +
      '<td class="wrap">' + escapeHtml(row.procurement_nazov || '') + '</td>' +
      '<td>' + escapeHtml(row.procurement_stav || '') + '</td>' +
      '<td class="wrap">' + escapeHtml(row.project_nazov || '') + '</td>' +
      '<td>' + escapeHtml(row.program_skratka || '') + '</td>' +
      '<td>' + (row.url_zmluva ? '<a href="' + escapeHtml(row.url_zmluva) + '" target="_blank" rel="noopener noreferrer">View</a>' : '') + '</td>';
    tbody.appendChild(tr);
  });
}

function renderPaginationControls(pageResult) {
  const pagination = document.getElementById('pagination');
  const table = document.getElementById('dataTable');
  const emptyMsg = document.getElementById('emptyMessage');
  if (pageResult.totalRows === 0) {
    table.style.display = 'none';
    pagination.style.display = 'none';
    emptyMsg.style.display = 'block';
    return;
  }
  table.style.display = 'table';
  emptyMsg.style.display = 'none';
  pagination.style.display = 'flex';
  document.getElementById('pageInfo').textContent =
    'Showing ' + pageResult.start + '–' + pageResult.end + ' of ' + pageResult.totalRows +
    ' (page ' + pageResult.page + ' of ' + pageResult.totalPages + ')';
  document.getElementById('prevPage').disabled = pageResult.page <= 1;
  document.getElementById('nextPage').disabled = pageResult.page >= pageResult.totalPages;
}

function render() {
  const filters = getFilters();
  const filtered = applyFilters(allData, filters);
  renderSupplierVoRollup(filtered, filters.supplier);
  const sorted = ProjectsLogic.sortData(filtered, sortKey, sortDir, typeOf);
  const pageResult = ProjectsLogic.paginate(sorted, currentPage, PAGE_SIZE);
  currentPage = pageResult.page;
  renderTableRows(pageResult.rows);
  renderPaginationControls(pageResult);
}

document.querySelectorAll('th').forEach(function (th) {
  th.addEventListener('click', function () {
    const key = th.dataset.key;
    if (sortKey === key) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
    else { sortKey = key; sortDir = 'asc'; }
    document.querySelectorAll('th').forEach(function (t) { t.classList.remove('sorted-asc', 'sorted-desc'); });
    th.classList.add(sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc');
    currentPage = 1;
    render();
  });
});

['filterSupplier', 'filterStatus'].forEach(function (id) {
  document.getElementById(id).addEventListener('input', function () { currentPage = 1; render(); });
  document.getElementById(id).addEventListener('change', function () { currentPage = 1; render(); });
});
document.getElementById('resetFilters').addEventListener('click', function () {
  document.getElementById('filterSupplier').value = '';
  document.getElementById('filterStatus').value = '';
  currentPage = 1;
  render();
});
document.getElementById('prevPage').addEventListener('click', function () { currentPage -= 1; render(); });
document.getElementById('nextPage').addEventListener('click', function () { currentPage += 1; render(); });

fetch(JSON_URL)
  .then(function (response) {
    if (!response.ok) throw new Error('HTTP ' + response.status);
    return response.json();
  })
  .then(function (payload) {
    allData = payload.contracts;
    document.getElementById('updated-label').textContent = 'Last updated: ' + formatIsoDate(payload.generated_at);
    populateStatusFilter(allData);
    renderChart(allData);
    render();
  })
  .catch(function (err) {
    document.getElementById('updated-label').textContent = '';
    const box = document.getElementById('error-box');
    box.style.display = 'block';
    box.textContent = 'Could not load ' + JSON_URL + ': ' + err.message;
  });
</script>
</body>
</html>
```

- [ ] **Step 2: Serve locally and verify in a browser**

With the local server running, navigate to `http://localhost:8000/procurement.html`. Confirm: the top-20-suppliers chart renders, clicking a supplier bar fills the search box AND shows a "Procurement procedures for..." rollup table above the contract list with that supplier's distinct VOs (deduped by procedure), and the main contract table narrows to just that supplier.

- [ ] **Step 3: Commit**

```bash
git add docs/procurement.html
git commit -m "Add procurement.html with per-supplier VO rollup"
```

---

### Task 14: `docs/disbursements.html`

**Files:**
- Create: `docs/disbursements.html`

**Interfaces:**
- Consumes: `docs/payment_disbursements_data.json` (`{generated_at, payments: [...]}`), `ProjectsLogic.sortData/paginate/distinctPrograms`, `DatamartLogic.groupSum/withCumulative/filterByProgram/monthKey`.

- [ ] **Step 1: Write `docs/disbursements.html`**

```html
<!DOCTYPE html>
<html lang="sk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Disbursements — Slovakia 2021–2027</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="styles.css?v=4">
<style>
  .card { max-width: 1300px; }
  table { min-width: 1100px; }
  .chart-wrap { height: 420px; margin-bottom: 28px; }
  @media (max-width: 480px) { table { min-width: 900px; } }
</style>
</head>
<body>
<div class="topbar">
  <a class="brand" href="index.html">
    <svg class="brand-mark" width="22" height="22" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
      <circle cx="12" cy="12" r="11" fill="#003399"/>
      <g fill="#FFCC00">
        <circle cx="12" cy="4" r="1.15"/><circle cx="18.5" cy="7.8" r="1.15"/><circle cx="18.5" cy="16.2" r="1.15"/>
        <circle cx="12" cy="20" r="1.15"/><circle cx="5.5" cy="16.2" r="1.15"/><circle cx="5.5" cy="7.8" r="1.15"/>
      </g>
    </svg>
    EU Funds Slovakia
  </a>
  <nav class="nav-links">
    <a href="index.html">Programs</a>
    <a href="projects.html">Projects</a>
    <a href="top_projects_chart.html">Top 50 Projects</a>
    <a href="regional_funding.html">Regional Funding</a>
    <a href="beneficiaries.html">Beneficiaries</a>
    <a href="procurement.html">Procurement</a>
    <a class="current" href="disbursements.html">Disbursements</a>
  </nav>
</div>
<div class="card">
  <h1>Payment Disbursements</h1>
  <span class="title-rule"></span>
  <p class="subtitle">Payment requests claimed over time, monthly and cumulative</p>
  <p class="updated" id="updated-label">Loading...</p>
  <p class="hint">Amounts are claimed amounts (narokovanaSuma), not necessarily yet paid — see the "Unpaid" column below for requests still awaiting payment.</p>

  <div class="filters" style="grid-template-columns: 1fr 1fr;">
    <div class="filter-group">
      <label for="programToggle">Program</label>
      <div class="multiselect" id="programMulti">
        <button type="button" class="ms-toggle" id="programToggle" aria-haspopup="true" aria-expanded="false">
          <span id="programToggleLabel">All programs</span><span class="ms-caret">▾</span>
        </button>
        <div class="ms-panel" id="programPanel" role="dialog" aria-label="Filter by program">
          <div class="ms-actions">
            <button type="button" id="programSelectAll">Select all</button>
            <button type="button" id="programClearAll">Clear</button>
          </div>
          <div class="ms-options" id="programOptions"></div>
        </div>
      </div>
    </div>
    <div class="filter-group">
      <label for="filterType">Payment type</label>
      <select id="filterType"><option value="">All types</option></select>
    </div>
  </div>

  <p class="error" style="display:none;" id="error-box"></p>

  <div class="chart-wrap"><canvas id="timelineChart"></canvas></div>

  <div class="table-scroll">
    <table id="dataTable" style="display:none;">
      <thead>
        <tr>
          <th data-key="kod" data-type="string">Code</th>
          <th data-key="typ" data-type="string">Type</th>
          <th data-key="project_nazov" data-type="string">Project</th>
          <th data-key="program_skratka" data-type="string">Program</th>
          <th data-key="prijimatel_nazov" data-type="string">Recipient</th>
          <th data-key="narokovanasuma" data-type="number">Claimed (€)</th>
          <th data-key="event_date" data-type="date">Date</th>
          <th data-key="neuhradena" data-type="bool">Unpaid</th>
        </tr>
      </thead>
      <tbody id="table-body"></tbody>
    </table>
    <p class="empty" id="emptyMessage" style="display:none;">No payment requests match the current filters.</p>
  </div>

  <div class="pagination" id="pagination" style="display:none;">
    <button id="prevPage">← Previous</button>
    <span id="pageInfo"></span>
    <button id="nextPage">Next →</button>
  </div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="projects_logic.js"></script>
<script src="datamart_logic.js"></script>
<script>
const HAS_CHART = typeof Chart !== 'undefined';
if (HAS_CHART) Chart.defaults.font.family = "'Montserrat', Arial, Helvetica, sans-serif";

const JSON_URL = 'payment_disbursements_data.json';
const PAGE_SIZE = 25;
let allData = [];
let allPrograms = [];
const selectedPrograms = new Set();
let currentPage = 1;
let sortKey = 'event_date';
let sortDir = 'desc';

function typeOf(key) {
  const th = document.querySelector('th[data-key="' + key + '"]');
  return th ? th.dataset.type : 'string';
}

function formatEUR(value) {
  if (value == null || Number.isNaN(Number(value))) return '';
  return '€' + Number(value).toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function formatEURShort(value) {
  if (value >= 1e9) return (value / 1e9).toFixed(2) + ' B';
  if (value >= 1e6) return (value / 1e6).toFixed(1) + ' M';
  if (value >= 1e3) return (value / 1e3).toFixed(0) + ' K';
  return Number(value).toLocaleString();
}

function formatIsoDate(iso) {
  const d = new Date(iso);
  return d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
}

function formatDateValue(value) {
  if (value == null || value === '') return '';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return '';
  return d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
}

function escapeHtml(value) {
  return String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function getFilters() {
  return { type: document.getElementById('filterType').value };
}

function currentFiltered() {
  let rows = DatamartLogic.filterByProgram(allData, Array.from(selectedPrograms));
  const filters = getFilters();
  if (filters.type) rows = rows.filter(function (r) { return r.typ === filters.type; });
  return rows;
}

function updateProgramToggleLabel() {
  const label = document.getElementById('programToggleLabel');
  const count = selectedPrograms.size;
  if (count === 0) label.textContent = 'All programs';
  else if (count === 1) label.textContent = Array.from(selectedPrograms)[0];
  else label.textContent = count + ' programs selected';
}

function populateProgramDropdown(data) {
  const container = document.getElementById('programOptions');
  allPrograms = ProjectsLogic.distinctPrograms(data);
  container.innerHTML = '';
  allPrograms.forEach(function (p) {
    const id = 'prog_' + p.skratka.replace(/[^A-Za-z0-9_-]/g, '');
    const wrap = document.createElement('label');
    wrap.className = 'ms-option';
    wrap.title = p.nazov || p.skratka;
    wrap.innerHTML = '<input type="checkbox" id="' + id + '" value="' + p.skratka + '"><span>' + p.skratka + '</span>';
    const cb = wrap.querySelector('input');
    cb.addEventListener('change', function () {
      if (cb.checked) selectedPrograms.add(p.skratka);
      else selectedPrograms.delete(p.skratka);
      updateProgramToggleLabel();
      currentPage = 1;
      render();
    });
    container.appendChild(wrap);
  });
  updateProgramToggleLabel();
}

function populateTypeFilter(data) {
  const select = document.getElementById('filterType');
  const types = Array.from(new Set(data.map(function (r) { return r.typ; }).filter(Boolean))).sort();
  types.forEach(function (t) {
    const opt = document.createElement('option');
    opt.value = t;
    opt.textContent = t;
    select.appendChild(opt);
  });
}

let chartInstance = null;

function renderChart(rows) {
  if (!HAS_CHART) return;
  const withDates = rows.filter(function (r) { return r.event_date; });
  const grouped = DatamartLogic.groupSum(withDates, function (r) { return DatamartLogic.monthKey(r.event_date); }, ['narokovanasuma']);
  grouped.sort(function (a, b) { return a.key < b.key ? -1 : a.key > b.key ? 1 : 0; });
  const withCumulative = DatamartLogic.withCumulative(grouped, 'narokovanasuma', 'cumulative');

  if (chartInstance) chartInstance.destroy();
  chartInstance = new Chart(document.getElementById('timelineChart').getContext('2d'), {
    data: {
      labels: withCumulative.map(function (g) { return g.key; }),
      datasets: [
        { type: 'bar', label: 'Monthly claimed (€)', data: withCumulative.map(function (g) { return g.narokovanasuma; }), backgroundColor: '#7C93C7', yAxisID: 'y' },
        { type: 'line', label: 'Cumulative (€)', data: withCumulative.map(function (g) { return g.cumulative; }), borderColor: '#003399', backgroundColor: '#003399', yAxisID: 'y1', tension: 0.15 }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: { position: 'left', beginAtZero: true, ticks: { callback: function (v) { return '€' + formatEURShort(v); } } },
        y1: { position: 'right', beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { callback: function (v) { return '€' + formatEURShort(v); } } }
      }
    }
  });
}

function renderTableRows(rows) {
  const tbody = document.getElementById('table-body');
  tbody.innerHTML = '';
  rows.forEach(function (row) {
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td>' + escapeHtml(row.kod || '') + '</td>' +
      '<td>' + escapeHtml(row.typ || '') + '</td>' +
      '<td class="wrap">' + escapeHtml(row.project_nazov || '') + '</td>' +
      '<td>' + escapeHtml(row.program_skratka || '') + '</td>' +
      '<td class="wrap">' + escapeHtml(row.prijimatel_nazov || '') + '</td>' +
      '<td class="num">' + formatEUR(row.narokovanasuma) + '</td>' +
      '<td>' + formatDateValue(row.event_date) + '</td>' +
      '<td>' + (row.neuhradena ? 'Yes' : 'No') + '</td>';
    tbody.appendChild(tr);
  });
}

function renderPaginationControls(pageResult) {
  const pagination = document.getElementById('pagination');
  const table = document.getElementById('dataTable');
  const emptyMsg = document.getElementById('emptyMessage');
  if (pageResult.totalRows === 0) {
    table.style.display = 'none';
    pagination.style.display = 'none';
    emptyMsg.style.display = 'block';
    return;
  }
  table.style.display = 'table';
  emptyMsg.style.display = 'none';
  pagination.style.display = 'flex';
  document.getElementById('pageInfo').textContent =
    'Showing ' + pageResult.start + '–' + pageResult.end + ' of ' + pageResult.totalRows +
    ' (page ' + pageResult.page + ' of ' + pageResult.totalPages + ')';
  document.getElementById('prevPage').disabled = pageResult.page <= 1;
  document.getElementById('nextPage').disabled = pageResult.page >= pageResult.totalPages;
}

function render() {
  const filtered = currentFiltered();
  renderChart(filtered);
  const sorted = ProjectsLogic.sortData(filtered, sortKey, sortDir, typeOf);
  const pageResult = ProjectsLogic.paginate(sorted, currentPage, PAGE_SIZE);
  currentPage = pageResult.page;
  renderTableRows(pageResult.rows);
  renderPaginationControls(pageResult);
}

document.querySelectorAll('th').forEach(function (th) {
  th.addEventListener('click', function () {
    const key = th.dataset.key;
    if (sortKey === key) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
    else { sortKey = key; sortDir = 'asc'; }
    document.querySelectorAll('th').forEach(function (t) { t.classList.remove('sorted-asc', 'sorted-desc'); });
    th.classList.add(sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc');
    currentPage = 1;
    render();
  });
});

document.getElementById('filterType').addEventListener('change', function () { currentPage = 1; render(); });

const programMulti = document.getElementById('programMulti');
const programToggle = document.getElementById('programToggle');
programToggle.addEventListener('click', function (e) {
  e.stopPropagation();
  programMulti.classList.toggle('open');
  programToggle.setAttribute('aria-expanded', programMulti.classList.contains('open') ? 'true' : 'false');
});
document.addEventListener('click', function (e) {
  if (!programMulti.contains(e.target)) programMulti.classList.remove('open');
});
document.getElementById('programSelectAll').addEventListener('click', function () {
  selectedPrograms.clear();
  allPrograms.forEach(function (p) { selectedPrograms.add(p.skratka); });
  document.querySelectorAll('#programOptions input[type="checkbox"]').forEach(function (cb) { cb.checked = true; });
  updateProgramToggleLabel();
  currentPage = 1;
  render();
});
document.getElementById('programClearAll').addEventListener('click', function () {
  selectedPrograms.clear();
  document.querySelectorAll('#programOptions input[type="checkbox"]').forEach(function (cb) { cb.checked = false; });
  updateProgramToggleLabel();
  currentPage = 1;
  render();
});
document.getElementById('prevPage').addEventListener('click', function () { currentPage -= 1; render(); });
document.getElementById('nextPage').addEventListener('click', function () { currentPage += 1; render(); });

fetch(JSON_URL)
  .then(function (response) {
    if (!response.ok) throw new Error('HTTP ' + response.status);
    return response.json();
  })
  .then(function (payload) {
    allData = payload.payments;
    document.getElementById('updated-label').textContent = 'Last updated: ' + formatIsoDate(payload.generated_at);
    populateProgramDropdown(allData);
    populateTypeFilter(allData);
    render();
  })
  .catch(function (err) {
    document.getElementById('updated-label').textContent = '';
    const box = document.getElementById('error-box');
    box.style.display = 'block';
    box.textContent = 'Could not load ' + JSON_URL + ': ' + err.message;
  });
</script>
</body>
</html>
```

- [ ] **Step 2: Serve locally and verify in a browser**

With the local server running, navigate to `http://localhost:8000/disbursements.html`. Confirm: the combo bar+line chart renders with a rising cumulative line, the payment-type dropdown is populated, selecting a type narrows both the chart and the table.

- [ ] **Step 3: Commit**

```bash
git add docs/disbursements.html
git commit -m "Add disbursements.html explorer page"
```

---

### Task 15: Delete `top_recipients_chart.html`, finalize shared nav, full end-to-end + cross-device verification

**Files:**
- Delete: `docs/top_recipients_chart.html`
- Modify: `docs/index.html`, `docs/projects.html`, `docs/top_projects_chart.html` (nav block only — the other 4 pages already ship with the final nav from their own tasks)

**Interfaces:** none (pure markup change).

- [ ] **Step 1: Delete the retired page**

```bash
git rm docs/top_recipients_chart.html
```

- [ ] **Step 2: Update the nav block on the 3 pages that still have the old 4-link nav**

In `docs/index.html`, `docs/projects.html`, and `docs/top_projects_chart.html`, replace the `<nav class="nav-links">...</nav>` block (each page keeps its own `class="current"` on its own link) with:

```html
  <nav class="nav-links">
    <a href="index.html">Programs</a>
    <a href="projects.html">Projects</a>
    <a href="top_projects_chart.html">Top 50 Projects</a>
    <a href="regional_funding.html">Regional Funding</a>
    <a href="beneficiaries.html">Beneficiaries</a>
    <a href="procurement.html">Procurement</a>
    <a href="disbursements.html">Disbursements</a>
  </nav>
```

- [ ] **Step 3: Grep to confirm every remaining page has exactly 7 nav links and none reference the deleted page**

Run:
```bash
for f in docs/index.html docs/projects.html docs/top_projects_chart.html docs/regional_funding.html docs/beneficiaries.html docs/procurement.html docs/disbursements.html; do
  echo "$f: $(grep -c '<a ' "$f" | head -1) — checking nav count:"
  grep -A8 'class="nav-links"' "$f" | grep -c '<a '
done
grep -rl "top_recipients_chart" docs/*.html
```
Expected: every page's nav count prints `7`; the final `grep -rl` finds no matches (empty output, exit code 1) since no remaining page links to the deleted file.

- [ ] **Step 4: Full end-to-end browser walkthrough across breakpoints**

With `python -m http.server --directory docs 8000` running, use the Playwright browser tool at three viewport widths (~1440px, ~768px, ~390px):

1. Navigate to `index.html` — confirm all 7 nav links resolve (click each in turn, confirm no 404 and each page highlights its own `.current` link), the programs table and chart render, and the "Project count" toggle works.
2. `projects.html` — confirm the Regions column populates, ICO links open Finstat, program/date filters still work.
3. `top_projects_chart.html` — confirm the top-50 bar chart renders and clicking a bar opens the matching project in `projects.html`.
4. `regional_funding.html` — confirm the map renders and recolors, clicking a tile shows its drill-down table, and at ~390px width the map/legend don't cause page-level horizontal scroll.
5. `beneficiaries.html` — confirm sorting, the Finstat link, and the recipient project-drill link all work.
6. `procurement.html` — confirm the supplier chart, the VO rollup on supplier click, and the contract table filter all work.
7. `disbursements.html` — confirm the combo chart and type filter work.

- [ ] **Step 5: Commit**

```bash
git add docs/index.html docs/projects.html docs/top_projects_chart.html
git commit -m "Finalize 7-link shared nav, remove top_recipients_chart.html"
```

---

## Self-Review Notes

- **Spec coverage:** every section of `2026-09-21-eufundsslovakia-rebuild-design.md` maps to a task — schema/JSON retirement (Tasks 2-5), all 6 datamart tables (Task 1), all 7 pages (Tasks 8-14), nav + cross-device final pass (Task 15). The Finstat link, per-supplier VO rollup, dated `regional_funding`, and SVG choropleth called out in the design doc each have their own step.
- **Placeholder scan:** no TBD/TODO; every step has runnable code, exact file edits with surrounding context, or an exact verification command.
- **Type/name consistency checked:** `MARTS` tuple shape `(table, sql, json_filename, json_key, order_by, epoch_ms_columns)` is identical between its definition and use in `build_marts`/`export_mart`/`main` (Task 1); JSON payload keys (`regional_funding`, `regions`, `programs`, `beneficiaries`, `contracts`, `payments`) match exactly between each mart's `MARTS` entry and the corresponding page's `payload.<key>` access; `DatamartLogic.groupSum(data, keyFn, sumFields, labelFn)` signature is identical across Task 6's definition and its uses in Tasks 8/12/13/14; `ProjectsLogic.applyFilters/sortData/paginate` signatures are unchanged from the existing file except the Task 7 field rename, which is threaded consistently into Tasks 9-11 (`project_kod`/`project_nazov` used in `projects.html`, `top_projects_chart.html`, and nowhere else that needed it).
- All verified-runnable SQL in Task 1 was actually executed against `data/eufunds.duckdb` during planning (not just written) — row counts and reconciliation checks (4,375 projects, 2,121 beneficiaries, 7,714 contracts, 22,970 payments, region/program counts reconciling to the full project total) are real, observed values baked into the verification script's assertions.
