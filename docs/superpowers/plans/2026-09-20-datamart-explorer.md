# EU Funds Datamart + Interactive Explorer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 4 public-interest datamart tables to `eufunds.duckdb` (regional funding, beneficiary transparency, procurement/supplier transparency, payment disbursements) and 4 new interactive static pages under `docs/` that chart and let visitors explore them.

**Architecture:** One new script `scripts/build_datamart.py` builds a `datamart` schema with plain SQL over the already-fetched `slovakia`/`website` tables (no API calls), and exports each table to a `docs/*_data.json` file. Four new HTML pages consume those JSON files client-side with Chart.js + vanilla JS, following the exact pattern `projects.html`/`top_projects_chart.html` already use: fetch one flat JSON array, filter/sort/aggregate/paginate in the browser, no backend.

**Tech Stack:** Python 3.12, `duckdb`, `pandas` (already in `requirements.txt`); vanilla JS + Chart.js 4.4.1 (CDN) on the frontend, no build step.

Design spec: `docs/superpowers/specs/2026-09-20-datamart-explorer-design.md`

## Global Constraints

- No new Python dependencies — use only `duckdb`, `pandas`, `json`, `pathlib`, `datetime` (already used by every fetch script).
- `scripts/build_datamart.py` makes **zero** HTTP requests — it only reads `slovakia`/`website` tables and writes `datamart` tables + JSON exports.
- Every new DuckDB table and column gets `COMMENT ON` documentation, matching every existing table in this DB (see `scripts/fetch_programs.py`'s `apply_comments` pattern).
- `itms21_ciselniky_detail` must always be joined on the **composite** `(ciselnik_kod, id)` — a bare `id` is not unique across code-list categories.
- New pages must reuse the existing visual system verbatim: `docs/styles.css` classes (`.topbar`, `.card`, `.filters`/`.filter-group`, `.multiselect`/`.ms-*`, `table`/`.table-scroll`, `.pagination`, `.hint`, `.chart-wrap`), Montserrat font, EU-blue/gold palette. Do not introduce new colors or fonts.
- New pages must reuse `docs/projects_logic.js`'s generic helpers (`sortData`, `paginate`, `topNByAmount`, `distinctPrograms`) via a second `<script src="projects_logic.js">` tag rather than reimplementing sort/paginate.
- Every new page must be added to the shared nav (`.nav-links`) on **every** page in `docs/`, existing and new.

---

### Task 1: `regional_funding` datamart table + JSON export

**Files:**
- Create: `scripts/build_datamart.py`
- Create: `docs/regional_funding_data.json` (generated, not hand-written)

**Interfaces:**
- Produces: `datamart.regional_funding` table (columns: `project_id, project_kod, project_nazov, program_id, program_skratka, program_nazov, prijimatel_nazov, nuts3_id, kraj_nazov, region_count, suma_eu, suma_sr, suma_spolu, stav, ukonceny, vrealizacii`).
- Produces: module-level `MARTS: list[tuple[str, str, str, str, str]]` (table_name, create_sql, json_filename, json_key, order_by) — Task 2/3/4 append to this list.
- Produces: `apply_comments(con, table)`, `build_marts(con)`, `export_mart(con, table, json_filename, json_key, order_by)`, `main()` — reused unchanged by Tasks 2-4.

- [ ] **Step 1: Write a verification script that expects the table to exist**

Create `scripts/_verify_datamart.py` (temporary, deleted in Task 4's last step) with:

```python
"""Ad-hoc verification for scripts/build_datamart.py, run manually during
implementation. Not part of the monthly pipeline."""
import json
import duckdb

con = duckdb.connect("data/eufunds.duckdb", read_only=True)

total, distinct_projects = con.execute(
    "SELECT count(*), count(DISTINCT project_id) FROM datamart.regional_funding"
).fetchone()
single_region = con.execute(
    "SELECT count(*) FROM datamart.regional_funding WHERE region_count = 1"
).fetchone()[0]
print(f"regional_funding: {total} rows, {distinct_projects} distinct projects, "
      f"{single_region} single-region rows")
assert total > 9000, "expected roughly 9,300 project-region rows"
assert distinct_projects <= 4375, "distinct projects must not exceed total projects"
assert single_region == 3722, "single-region project count should match itms21_projekt_miestorealizaciefull"

with open("docs/regional_funding_data.json", encoding="utf-8") as f:
    payload = json.load(f)
assert "generated_at" in payload
assert len(payload["regional_funding"]) == total
print("regional_funding JSON export OK:", len(payload["regional_funding"]), "rows")
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `python scripts/_verify_datamart.py`
Expected: FAIL with `Catalog Error: Table with name regional_funding does not exist` (the `datamart` schema doesn't exist yet).

- [ ] **Step 3: Write `scripts/build_datamart.py`**

```python
"""
Builds the `datamart` schema inside the shared eufunds.duckdb: denormalized,
public-interest tables computed with plain SQL over the already-fetched
`slovakia`/`website` tables (no API calls). Each table is fully rebuilt every
run (CREATE OR REPLACE TABLE) since these are cheap aggregation queries, not
incremental syncs like the fetch scripts. Every table is also exported to a
docs/*_data.json file for its corresponding explorer page.

Depends on slovakia.itms21_projekt, itms21_subjekt, itms21_program,
itms21_ciselniky_detail, itms21_zmluvaverejneobstaravanie,
itms21_verejneobstaravanie_detail, itms21_zop, and
website.itms21_projects_current all being populated — run after every fetch
script in .github/workflows/monthly.yml.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DB_PATH = Path("data/eufunds.duckdb")
SLOVAKIA_SCHEMA = "slovakia"
WEBSITE_SCHEMA = "website"
DATAMART_SCHEMA = "datamart"
DOCS_DIR = Path("docs")


def _esc(value: str) -> str:
    """Escape a string for embedding in a single-quoted SQL literal."""
    return value.replace("'", "''")


REGIONAL_FUNDING_SQL = f"""
    CREATE OR REPLACE TABLE {DATAMART_SCHEMA}.regional_funding AS
    SELECT
        p.id AS project_id,
        p.kod AS project_kod,
        p.nazov AS project_nazov,
        p.program_id,
        prog.skratka AS program_skratka,
        prog.nazovsk AS program_nazov,
        wp.prijimatel_nazov,
        m.nuts3_id,
        nuts.nazovsk AS kraj_nazov,
        rc.region_count,
        wp.suma_eu,
        wp.suma_sr,
        wp.suma_spolu,
        p.stav,
        p.ukonceny,
        p.vrealizacii
    FROM {SLOVAKIA_SCHEMA}.itms21_projekt p
    JOIN {SLOVAKIA_SCHEMA}.itms21_projekt_miestorealizaciefull m
        ON m.project_id = p.id AND m.stat_id = 210 AND m.nuts3_id IS NOT NULL
    JOIN {SLOVAKIA_SCHEMA}.itms21_ciselniky_detail nuts
        ON nuts.ciselnik_kod = '1006' AND nuts.id = m.nuts3_id
    JOIN (
        SELECT project_id, count(DISTINCT nuts3_id) AS region_count
        FROM {SLOVAKIA_SCHEMA}.itms21_projekt_miestorealizaciefull
        WHERE stat_id = 210 AND nuts3_id IS NOT NULL
        GROUP BY 1
    ) rc ON rc.project_id = p.id
    LEFT JOIN {SLOVAKIA_SCHEMA}.itms21_program prog ON prog.id = p.program_id
    LEFT JOIN {WEBSITE_SCHEMA}.itms21_projects_current wp ON wp.project_id = p.id
"""

MART_TABLE_COMMENTS: dict[str, str] = {
    "regional_funding": (
        "One row per (project x Slovak region it is implemented in). Fully "
        "rebuilt on every run from slovakia.itms21_projekt and "
        "itms21_projekt_miestorealizaciefull. CAVEAT: about 15% of projects "
        "run in all 8 regions at once (nationwide programmes) and get one "
        "row per region here, so SUM(suma_spolu) across all rows overstates "
        "the true national budget - filter to region_count = 1 for a "
        "reliable per-region breakdown, and treat region_count > 1 rows as "
        "a separate 'nationwide' bucket."
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
        "nuts3_id": "NUTS3 region id (itms21_ciselniky_detail, ciselnik_kod 1006).",
        "kraj_nazov": "Decoded Slovak region (kraj) name.",
        "region_count": "Number of distinct NUTS3 regions this project is implemented in.",
        "suma_eu": "EU-fund contribution, in euro.",
        "suma_sr": "Slovak national co-financing contribution, in euro.",
        "suma_spolu": "Total contribution (EU + national), in euro.",
        "stav": "Project status.",
        "ukonceny": "Whether the project is completed.",
        "vrealizacii": "Whether the project is currently being implemented.",
    },
}

# (table_name, create_sql, json_filename, json_key, order_by, epoch_ms_columns)
MARTS: list[tuple[str, str, str, str, str, list[str]]] = [
    ("regional_funding", REGIONAL_FUNDING_SQL, "regional_funding_data.json", "regional_funding", "suma_spolu DESC", []),
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
        json.dump(export, f, ensure_ascii=False, indent=2)
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

Note: `export_mart` uses `pd` (pandas) — add `import pandas as pd` next to the other imports at the top of the file.

- [ ] **Step 4: Run the build script, then re-run verification**

Run: `python scripts/build_datamart.py && python scripts/_verify_datamart.py`
Expected: both commands print success output ending in `regional_funding JSON export OK: <n> rows` with no assertion errors.

- [ ] **Step 5: Commit**

```bash
git add scripts/build_datamart.py scripts/_verify_datamart.py docs/regional_funding_data.json
git commit -m "Add regional_funding datamart table + JSON export"
```

---

### Task 2: `beneficiary_funding` datamart table + JSON export

**Files:**
- Modify: `scripts/build_datamart.py`
- Modify: `scripts/_verify_datamart.py`
- Create: `docs/beneficiary_funding_data.json` (generated)

**Interfaces:**
- Consumes: `MARTS` list, `MART_TABLE_COMMENTS`, `MART_COLUMN_COMMENTS`, `apply_comments`, `build_marts`, `export_mart`, `main` from Task 1 — unchanged.
- Produces: `datamart.beneficiary_funding` table (columns: `subjekt_id, nazov, ico, pravnaforma_nazov, adresa_obec, primary_sector, project_count, suma_eu, suma_sr, suma_spolu, first_project_year, last_project_year`).

- [ ] **Step 1: Extend the verification script to expect it**

Add to `scripts/_verify_datamart.py` (after the `regional_funding` block):

```python
total_ben = con.execute("SELECT count(*) FROM datamart.beneficiary_funding").fetchone()[0]
print(f"beneficiary_funding: {total_ben} rows")
assert total_ben == 2121, "expected 2,121 distinct beneficiaries (subjekt.prijimatel_id count)"

with open("docs/beneficiary_funding_data.json", encoding="utf-8") as f:
    payload = json.load(f)
assert len(payload["beneficiaries"]) == total_ben
print("beneficiary_funding JSON export OK:", len(payload["beneficiaries"]), "rows")
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `python scripts/_verify_datamart.py`
Expected: FAIL with `Catalog Error: Table with name beneficiary_funding does not exist`.

- [ ] **Step 3: Add the mart to `scripts/build_datamart.py`**

Insert before the `MARTS` list definition:

```python
BENEFICIARY_FUNDING_SQL = f"""
    CREATE OR REPLACE TABLE {DATAMART_SCHEMA}.beneficiary_funding AS
    WITH proj AS (
        SELECT p.id AS project_id, p.prijimatel_id, p.planovanarealizaciazaciatok,
               wp.suma_eu, wp.suma_sr, wp.suma_spolu
        FROM {SLOVAKIA_SCHEMA}.itms21_projekt p
        LEFT JOIN {WEBSITE_SCHEMA}.itms21_projects_current wp ON wp.project_id = p.id
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

MART_TABLE_COMMENTS["beneficiary_funding"] = (
    "One row per beneficiary (subjekt) that is the recipient on at least one "
    "project. Fully rebuilt on every run. primary_sector is the most "
    "frequent hospodarskaCinnost across the beneficiary's projects, not an "
    "exhaustive list of all sectors they operate in."
)

MART_COLUMN_COMMENTS["beneficiary_funding"] = {
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
}
```

Add the new tuple to `MARTS`:

```python
MARTS.append(
    ("beneficiary_funding", BENEFICIARY_FUNDING_SQL, "beneficiary_funding_data.json", "beneficiaries", "suma_spolu DESC", [])
)
```

(Replace the list-literal form of `MARTS` from Task 1 with an initial assignment holding only the `regional_funding` tuple, followed by this `.append(...)` call — keeps every mart's definition next to its own SQL/comments instead of one growing literal.)

- [ ] **Step 4: Run the build script, then re-run verification**

Run: `python scripts/build_datamart.py && python scripts/_verify_datamart.py`
Expected: both `regional_funding` and `beneficiary_funding` sections print success, ending in `beneficiary_funding JSON export OK: 2121 rows`.

- [ ] **Step 5: Commit**

```bash
git add scripts/build_datamart.py scripts/_verify_datamart.py docs/beneficiary_funding_data.json
git commit -m "Add beneficiary_funding datamart table + JSON export"
```

---

### Task 3: `procurement_contracts` datamart table + JSON export

**Files:**
- Modify: `scripts/build_datamart.py`
- Modify: `scripts/_verify_datamart.py`
- Create: `docs/procurement_contracts_data.json` (generated)

**Interfaces:**
- Consumes: same shared helpers as Task 2.
- Produces: `datamart.procurement_contracts` table (columns: `contract_id, contract_kod, contract_nazov, cislozmluvy, celkovasumazmluvy, sumabezdph, datumucinnosti, datumplatnosti, url_zmluva, supplier_nazov, supplier_ico, procurement_id, procurement_kod, procurement_nazov, procurement_stav, cpv_nazov, project_id, project_kod, project_nazov, program_skratka`).

- [ ] **Step 1: Extend the verification script**

Add to `scripts/_verify_datamart.py`:

```python
total_contracts = con.execute("SELECT count(*) FROM datamart.procurement_contracts").fetchone()[0]
print(f"procurement_contracts: {total_contracts} rows")
assert total_contracts == 7714, "expected 7,714 contract-project link rows"
no_supplier = con.execute(
    "SELECT count(*) FROM datamart.procurement_contracts WHERE supplier_nazov IS NULL"
).fetchone()[0]
assert no_supplier == 0, "every contract must resolve a supplier via subjekt or dodavatelobstaravatel"

with open("docs/procurement_contracts_data.json", encoding="utf-8") as f:
    payload = json.load(f)
assert len(payload["contracts"]) == total_contracts
print("procurement_contracts JSON export OK:", len(payload["contracts"]), "rows")
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `python scripts/_verify_datamart.py`
Expected: FAIL with `Catalog Error: Table with name procurement_contracts does not exist`.

- [ ] **Step 3: Add the mart to `scripts/build_datamart.py`**

```python
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

MART_TABLE_COMMENTS["procurement_contracts"] = (
    "One row per (public-procurement contract x project it serves) - a "
    "contract with no linked project, or a procedure that funds multiple "
    "projects, both appear here (LEFT JOIN), so a contract can have more "
    "than one row. Fully rebuilt on every run. supplier_nazov/supplier_ico "
    "resolve the two mutually-exclusive supplier FKs on "
    "itms21_zmluvaverejneobstaravanie into a single pair of columns."
)

MART_COLUMN_COMMENTS["procurement_contracts"] = {
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
}

MARTS.append(
    ("procurement_contracts", PROCUREMENT_CONTRACTS_SQL, "procurement_contracts_data.json",
     "contracts", "celkovasumazmluvy DESC", ["datumucinnosti", "datumplatnosti"])
)
```

- [ ] **Step 4: Run the build script, then re-run verification**

Run: `python scripts/build_datamart.py && python scripts/_verify_datamart.py`
Expected: all three mart sections print success, ending in `procurement_contracts JSON export OK: 7714 rows`.

- [ ] **Step 5: Commit**

```bash
git add scripts/build_datamart.py scripts/_verify_datamart.py docs/procurement_contracts_data.json
git commit -m "Add procurement_contracts datamart table + JSON export"
```

---

### Task 4: `payment_disbursements` datamart table + JSON export, remove scratch verification script

**Files:**
- Modify: `scripts/build_datamart.py`
- Delete: `scripts/_verify_datamart.py` (last step, after final use)
- Create: `docs/payment_disbursements_data.json` (generated)

**Interfaces:**
- Produces: `datamart.payment_disbursements` table (columns: `zop_id, kod, typ, projekt_id, project_kod, project_nazov, program_id, program_skratka, prijimatel_nazov, narokovanasuma, event_date, neuhradena, zopjezaverecna`).

- [ ] **Step 1: Extend the verification script one last time**

Add to `scripts/_verify_datamart.py`:

```python
total_zop = con.execute("SELECT count(*) FROM datamart.payment_disbursements").fetchone()[0]
print(f"payment_disbursements: {total_zop} rows")
assert total_zop == 22970, "expected 22,970 payment requests (itms21_zop row count)"
null_project = con.execute(
    "SELECT count(*) FROM datamart.payment_disbursements WHERE projekt_id IS NULL"
).fetchone()[0]
assert null_project == 0, "itms21_zop.projekt_id is populated on every row"

with open("docs/payment_disbursements_data.json", encoding="utf-8") as f:
    payload = json.load(f)
assert len(payload["payments"]) == total_zop
print("payment_disbursements JSON export OK:", len(payload["payments"]), "rows")
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `python scripts/_verify_datamart.py`
Expected: FAIL with `Catalog Error: Table with name payment_disbursements does not exist`.

- [ ] **Step 3: Add the mart to `scripts/build_datamart.py`**

```python
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

MART_TABLE_COMMENTS["payment_disbursements"] = (
    "One row per payment request (itms21_zop). Fully rebuilt on every run. "
    "event_date is COALESCE(datumuhrady, datumprijatia, createdat) normalized "
    "to a single epoch-millisecond value, since the source zop table stores "
    "these as VARCHAR text containing epoch-ms digits, not ISO date strings."
)

MART_COLUMN_COMMENTS["payment_disbursements"] = {
    "zop_id": "Id of the payment request (itms21_zop.id).",
    "kod": "Payment request code.",
    "typ": "Payment request type, e.g. ZALOHA (advance), REFUNDACIA (reimbursement), PREDFINANCOVANIE (pre-financing).",
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
}

MARTS.append(
    ("payment_disbursements", PAYMENT_DISBURSEMENTS_SQL, "payment_disbursements_data.json",
     "payments", "event_date DESC NULLS LAST", ["event_date"])
)
```

- [ ] **Step 4: Run the build script, then re-run verification**

Run: `python scripts/build_datamart.py && python scripts/_verify_datamart.py`
Expected: all four mart sections print success, ending in `payment_disbursements JSON export OK: 22970 rows`.

- [ ] **Step 5: Delete the scratch verification script and commit**

```bash
git rm scripts/_verify_datamart.py
git add scripts/build_datamart.py docs/payment_disbursements_data.json
git commit -m "Add payment_disbursements datamart table + JSON export"
```

---

### Task 5: Wire `build_datamart.py` into the monthly workflow

**Files:**
- Modify: `.github/workflows/monthly.yml`

**Interfaces:**
- Consumes: `scripts/build_datamart.py`'s `main()` (no arguments, no return value used).

- [ ] **Step 1: Add the build step after every fetch script**

In `.github/workflows/monthly.yml`, modify the `Run fetch scripts` step (currently ending at `python scripts/fetch_dodavatelobstaravatel.py`, `.github/workflows/monthly.yml:61`) by appending one line:

```yaml
          python scripts/fetch_dodavatelobstaravatel.py
          python scripts/build_datamart.py
```

- [ ] **Step 2: Add the 4 new JSON files to the commit step**

Modify the `git add` line in the `Commit and push updated data` step (`.github/workflows/monthly.yml:77`):

```yaml
          git add docs/program_data.json docs/project_data.json docs/regional_funding_data.json docs/beneficiary_funding_data.json docs/procurement_contracts_data.json docs/payment_disbursements_data.json
```

- [ ] **Step 3: Verify the YAML is well-formed**

Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/monthly.yml', encoding='utf-8'))"`
Expected: no output, exit code 0 (no `pip install pyyaml` needed if already available; if `ModuleNotFoundError: No module named 'yaml'`, run `pip install pyyaml` first — it's a throwaway check, not a new project dependency).

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/monthly.yml
git commit -m "Wire build_datamart.py into the monthly workflow"
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
// Shared logic for the 4 datamart explorer pages — pure functions, testable
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

Then: `rm scripts/_verify_datamart_logic.js` (Windows: `del scripts\_verify_datamart_logic.js` — this repo's Bash tool runs Git Bash, so `rm` works).

- [ ] **Step 5: Commit**

```bash
git add docs/datamart_logic.js
git commit -m "Add shared datamart_logic.js helpers for the explorer pages"
```

---

### Task 7: `docs/regional_funding.html`

**Files:**
- Create: `docs/regional_funding.html`
- Modify: `docs/styles.css` (add `.drilldown-title` and `.drilldown-scroll`)

**Interfaces:**
- Consumes: `docs/regional_funding_data.json` (`{generated_at, regional_funding: [...]}`), `ProjectsLogic.distinctPrograms/sortData/paginate` (`docs/projects_logic.js`), `DatamartLogic.groupSum/dedupeBy/filterByProgram` (`docs/datamart_logic.js`).

- [ ] **Step 1: Add two small CSS rules**

Append to `docs/styles.css` (after the `.pagination` rule, `docs/styles.css:377-380`):

```css
.drilldown-title {
  font-family: var(--font-display);
  color: var(--navy-base);
  font-size: 16px;
  font-weight: 600;
  margin: 28px 0 12px 0;
}
.drilldown-scroll { max-height: 420px; overflow-y: auto; }
```

- [ ] **Step 2: Write `docs/regional_funding.html`**

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
<link rel="stylesheet" href="styles.css?v=3">
<style>
  .chart-wrap { height: 500px; }
  @media (max-width: 768px) { .chart-wrap { height: 420px; } }
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
    <a href="top_recipients_chart.html">Top 25 Recipients</a>
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
  <p class="hint">Tip: click a region bar to see its projects below. Nationwide programmes (implemented in all 8 regions) are excluded from the bars and shown as a separate total, since attributing them to one region would be misleading.</p>

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

  <div class="chart-wrap">
    <canvas id="regionChart"></canvas>
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

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="projects_logic.js"></script>
<script src="datamart_logic.js"></script>
<script>
const HAS_CHART = typeof Chart !== 'undefined';
if (HAS_CHART) {
  Chart.defaults.font.family = "'Montserrat', Arial, Helvetica, sans-serif";
}

const JSON_URL = 'regional_funding_data.json';
let allData = [];
let allPrograms = [];
const selectedPrograms = new Set();
let chartInstance = null;

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

function renderDrilldown(kraj) {
  const rows = allData.filter(function (r) { return r.region_count === 1 && r.kraj_nazov === kraj; });
  const filtered = DatamartLogic.filterByProgram(rows, Array.from(selectedPrograms));
  const sorted = ProjectsLogic.sortData(filtered, 'suma_spolu', 'desc', function () { return 'number'; });

  document.getElementById('drilldownTitle').textContent = 'Projects in ' + kraj + ' (' + sorted.length + ')';
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

function render() {
  const programFiltered = DatamartLogic.filterByProgram(allData, Array.from(selectedPrograms));

  const singleRegion = programFiltered.filter(function (r) { return r.region_count === 1; });
  const grouped = DatamartLogic.groupSum(singleRegion, function (r) { return r.kraj_nazov; }, ['suma_spolu']);

  const nationwideProjects = DatamartLogic.dedupeBy(
    programFiltered.filter(function (r) { return r.region_count > 1; }),
    function (r) { return r.project_id; }
  );
  const nationwideTotal = nationwideProjects.reduce(function (sum, r) { return sum + (Number(r.suma_spolu) || 0); }, 0);
  const callout = document.getElementById('nationwideCallout');
  if (nationwideProjects.length > 0) {
    callout.style.display = 'block';
    callout.textContent = nationwideProjects.length + ' nationwide project(s) (implemented in more than one region) ' +
      'total €' + formatEURShort(nationwideTotal) + ', not shown in the bars above.';
  } else {
    callout.style.display = 'none';
  }

  if (!HAS_CHART) {
    document.getElementById('error-box').style.display = 'block';
    document.getElementById('error-box').textContent = 'Chart library failed to load. Please check your connection and reload.';
    return;
  }

  const labels = grouped.map(function (g) { return g.key; });
  const amounts = grouped.map(function (g) { return g.suma_spolu; });

  if (chartInstance) chartInstance.destroy();
  const ctx = document.getElementById('regionChart').getContext('2d');
  chartInstance = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{ label: 'Total funding (€)', data: amounts, backgroundColor: '#003399', borderRadius: 3 }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      onClick: function (evt, elements) {
        if (elements.length > 0) renderDrilldown(labels[elements[0].index]);
      },
      onHover: function (evt, elements) {
        evt.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function (item) { return '€' + item.raw.toLocaleString(undefined, { maximumFractionDigits: 0 }); }
          }
        }
      },
      scales: {
        y: { beginAtZero: true, ticks: { callback: function (v) { return '€' + formatEURShort(v); } } }
      }
    }
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

fetch(JSON_URL)
  .then(function (response) {
    if (!response.ok) throw new Error('HTTP ' + response.status);
    return response.json();
  })
  .then(function (payload) {
    allData = payload.regional_funding;
    document.getElementById('updated-label').textContent = 'Last updated: ' + formatIsoDate(payload.generated_at);
    populateProgramDropdown(allData);
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

- [ ] **Step 3: Serve the site locally**

Run (in background): `python -m http.server --directory docs 8000`

- [ ] **Step 4: Verify in a browser**

Using the Playwright browser tool: navigate to `http://localhost:8000/regional_funding.html`, take a snapshot, and confirm: the "Last updated" label shows a real timestamp (not "Loading..."), a canvas bar chart with 8 region bars is visible, and the nationwide-projects hint text shows a nonzero count. Click one of the bars and confirm a "Projects in `<kraj>`" table appears below with rows.

- [ ] **Step 5: Commit**

```bash
git add docs/regional_funding.html docs/styles.css
git commit -m "Add regional_funding.html explorer page"
```

---

### Task 8: `docs/beneficiaries.html`

**Files:**
- Create: `docs/beneficiaries.html`

**Interfaces:**
- Consumes: `docs/beneficiary_funding_data.json` (`{generated_at, beneficiaries: [...]}`), `ProjectsLogic.sortData/paginate`, `DatamartLogic.groupSum`. Links out to `projects.html?recipient=<name>` (existing query-param support in `docs/projects.html:438-443`).

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
<link rel="stylesheet" href="styles.css?v=3">
<style>
  .card { max-width: 1300px; }
  table { min-width: 1100px; }
  .chart-wrap { height: 320px; margin-bottom: 28px; }
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
    <a href="top_recipients_chart.html">Top 25 Recipients</a>
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
  <p class="hint">Tip: click a beneficiary's name to see their full project list.</p>

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
      '<td>' + escapeHtml(row.ico || '') + '</td>' +
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

With the local server from Task 7 still running (or restart it: `python -m http.server --directory docs 8000`), use the Playwright browser tool to navigate to `http://localhost:8000/beneficiaries.html`. Confirm: the legal-form bar chart renders, the table shows rows sorted by total descending, typing into the name filter narrows the table, and clicking a beneficiary name navigates to `projects.html?recipient=...` with that recipient's projects shown.

- [ ] **Step 3: Commit**

```bash
git add docs/beneficiaries.html
git commit -m "Add beneficiaries.html explorer page"
```

---

### Task 9: `docs/procurement.html`

**Files:**
- Create: `docs/procurement.html`

**Interfaces:**
- Consumes: `docs/procurement_contracts_data.json` (`{generated_at, contracts: [...]}`), `ProjectsLogic.sortData/paginate`, `DatamartLogic.groupSum`.

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
<link rel="stylesheet" href="styles.css?v=3">
<style>
  .card { max-width: 1300px; }
  table { min-width: 1200px; }
  .chart-wrap { height: 420px; margin-bottom: 28px; }
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
    <a href="top_recipients_chart.html">Top 25 Recipients</a>
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
  <p class="hint">Tip: click a supplier bar to filter the contract table below to just their contracts.</p>

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

With the local server running, use the Playwright browser tool to navigate to `http://localhost:8000/procurement.html`. Confirm: the top-20-suppliers horizontal bar chart renders, the contract table below is populated and sortable, and clicking a supplier bar fills the "Supplier" text filter and narrows the table to that supplier's contracts.

- [ ] **Step 3: Commit**

```bash
git add docs/procurement.html
git commit -m "Add procurement.html explorer page"
```

---

### Task 10: `docs/disbursements.html`

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
<link rel="stylesheet" href="styles.css?v=3">
<style>
  .card { max-width: 1300px; }
  table { min-width: 1100px; }
  .chart-wrap { height: 420px; margin-bottom: 28px; }
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
    <a href="top_recipients_chart.html">Top 25 Recipients</a>
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

With the local server running, use the Playwright browser tool to navigate to `http://localhost:8000/disbursements.html`. Confirm: the combo bar+line chart renders with a rising cumulative line, the payment-type dropdown is populated, selecting a type narrows both the chart and the table, and the table is sortable/paginated.

- [ ] **Step 3: Commit**

```bash
git add docs/disbursements.html
git commit -m "Add disbursements.html explorer page"
```

---

### Task 11: Update nav on existing pages + final end-to-end verification

**Files:**
- Modify: `docs/index.html`
- Modify: `docs/projects.html`
- Modify: `docs/top_projects_chart.html`
- Modify: `docs/top_recipients_chart.html`

**Interfaces:** none (pure markup change, no new functions).

- [ ] **Step 1: Update the nav block in all 4 existing pages**

In each of `docs/index.html`, `docs/projects.html` (`docs/projects.html:29-34`), `docs/top_projects_chart.html` (`docs/top_projects_chart.html:29-34`), and `docs/top_recipients_chart.html` (`docs/top_recipients_chart.html:29-34`), replace the `<nav class="nav-links">...</nav>` block with (keeping each page's own `class="current"` on its own link, unchanged):

```html
  <nav class="nav-links">
    <a href="index.html">Programs</a>
    <a href="projects.html">Projects</a>
    <a href="top_projects_chart.html">Top 50 Projects</a>
    <a href="top_recipients_chart.html">Top 25 Recipients</a>
    <a href="regional_funding.html">Regional Funding</a>
    <a href="beneficiaries.html">Beneficiaries</a>
    <a href="procurement.html">Procurement</a>
    <a href="disbursements.html">Disbursements</a>
  </nav>
```

- [ ] **Step 2: Grep to confirm every page now has 8 nav links**

Run: `grep -c '<a href="regional_funding.html"' docs/*.html`
Expected: every one of the 8 `docs/*.html` pages prints `1` (the 4 new pages already had this link from Tasks 7-10; the 4 existing pages now have it too from Step 1).

- [ ] **Step 3: Full end-to-end browser walkthrough**

With `python -m http.server --directory docs 8000` running, use the Playwright browser tool to:
1. Navigate to `http://localhost:8000/index.html` and confirm all 8 nav links are present and none are broken (click each one in turn, confirm the page loads without a 404 and its own nav link is highlighted as `.current`).
2. On `regional_funding.html`, apply a program filter and confirm the chart and nationwide callout update.
3. On `beneficiaries.html`, sort by "Total (€)" ascending and confirm the smallest beneficiary appears first.
4. On `procurement.html`, use the supplier search box directly (typing a known supplier substring) and confirm the table narrows.
5. On `disbursements.html`, filter by one payment type and confirm the chart's month buckets change.

- [ ] **Step 4: Commit**

```bash
git add docs/index.html docs/projects.html docs/top_projects_chart.html docs/top_recipients_chart.html
git commit -m "Add datamart explorer pages to the shared site navigation"
```

---

## Self-Review Notes

- **Spec coverage:** all 4 marts (Task 1-4), workflow wiring (Task 5), shared JS helpers (Task 6), all 4 pages with their specified chart/filter/drill-down behavior (Tasks 7-10), and shared nav (Task 11) are each covered by a task.
- **Placeholder scan:** no TBD/TODO; every step has runnable code or an exact grep/verification command.
- **Type/name consistency checked:** `DatamartLogic.groupSum(data, keyFn, sumFields, labelFn)` signature is identical across Task 6's definition and its uses in Tasks 7/8/9/10; `ProjectsLogic.sortData(data, key, dir, typeOf)`/`paginate(data, page, pageSize)` signatures match `docs/projects_logic.js`'s existing implementation exactly; JSON top-level keys (`regional_funding`, `beneficiaries`, `contracts`, `payments`) match between each mart's `MARTS` tuple in `build_datamart.py` and the corresponding page's `payload.<key>` access.
