# EU Funds Datamart + Interactive Explorer Pages

Status: Approved. Date: 2026-09-20.

## Context

`data/eufunds.duckdb` (`slovakia` schema) holds ~170 normalized tables synced
from the ITMS21 public API — projects, grant applications, calls, payment
requests, procurement procedures/contracts, and legal entities — but almost
none of it is exposed on the public `docs/` GitHub Pages site today. Only
programmes and projects have a flattened "current" table and a JSON export
(`docs/program_data.json`, `docs/project_data.json`), consumed by
`index.html`, `projects.html`, and two chart pages.

This project adds a `datamart` schema of purpose-built, denormalized tables
answering four public-interest "follow the money" questions the raw schema
can't answer directly, plus four new static pages that let a visitor explore
those tables interactively (filter, chart, drill down) — the same way
`projects.html` / `top_projects_chart.html` already work, just for new
topics: regional distribution, who receives the money, who wins the
contracts, and when money actually gets paid out.

## Approach

One new script, `scripts/build_datamart.py`, builds the 4 datamart tables
with plain SQL joins/aggregations over the existing `slovakia` tables (no new
API calls) and exports each straight to a `docs/*_data.json` file. Four new
HTML pages consume those JSON files client-side with Chart.js + vanilla JS,
following the established pattern: ship one flat JSON array, filter/sort/
aggregate in the browser (this repo already does that at 4,375 rows / 3.6MB
for `project_data.json`, so all four new exports — the largest being ~23,000
rows of `payment_disbursements` — are within precedent).

Alternatives considered and rejected:
- **DuckDB views instead of materialized tables** — rejected; the ask is for
  inspectable datamart *tables* (e.g. via DBeaver), and the whole file is
  rebuilt monthly regardless, so materialized-vs-view staleness isn't a real
  concern.
- **Skip the DuckDB layer, compute JSON directly from `slovakia` tables in
  Python** — rejected; loses the queryable datamart tables that were
  explicitly requested.

## Data model (`datamart` schema)

All 4 tables are fully rebuilt (`CREATE OR REPLACE TABLE ... AS SELECT`)
every run — cheap aggregation queries, not incremental syncs like the fetch
scripts. Every table/column gets `COMMENT ON` documentation, matching every
other table in this DB.

### 1. `datamart.regional_funding`

Grain: one row per (project × Slovak region it's implemented in), sourced
from `itms21_projekt` joined to `itms21_projekt_miestorealizaciefull`
(filtered to `stat_id = 210` / Slovakia, non-null `nuts3_id`) and
`itms21_ciselniky_detail` (`ciselnik_kod='1006'`, joined on `(ciselnik_kod,
id)` together — a bare `id` is not unique across code-list categories).

| Column | Notes |
|---|---|
| `project_id`, `project_kod`, `project_nazov` | |
| `program_id`, `program_skratka`, `program_nazov` | |
| `prijimatel_nazov` | beneficiary name, denormalized for drill-down display |
| `nuts3_id`, `kraj_nazov` | region id + decoded name |
| `region_count` | number of distinct regions this project spans |
| `suma_eu`, `suma_sr`, `suma_spolu` | contracted funding, same derivation as `website.itms21_projects_current` |
| `stav`, `ukonceny`, `vrealizacii` | for status filtering |

**Caveat (documented in the table comment)**: ~15% of projects (349 of
4,375) run in all 8 regions at once ("nationwide" programmes) and get one row
per region — summing `suma_spolu` across all rows overstates the true
national budget. The webpage treats `region_count = 1` projects (3,722 of
them) as the reliable per-region view, and shows multi-region projects as a
separate "nationwide" total rather than silently double-counting them into a
region's bar.

**Fund granularity scope cut**: fund (EFRR/ESF+/KF/...) is *not* broken out
per project in v1 — the umbrella programme (`program_skratka`) is used
instead, since Slovakia's main programme ("PSK") mixes multiple funds and
true per-project fund attribution would require parsing
`itms21_projekt_financnyplan.zdroj` name strings. Flagged as a possible
future refinement, out of scope here.

### 2. `datamart.beneficiary_funding`

Grain: one row per beneficiary (`itms21_subjekt`) that is `prijimatel_id` on
≥1 project.

| Column | Notes |
|---|---|
| `subjekt_id`, `nazov`, `ico` | |
| `pravnaforma_nazov` | decoded via ciselnik `1009` |
| `adresa_obec` | municipality, as stored (free text, not geocoded) |
| `primary_sector` | most frequent `hospodarskaCinnost` (ciselnik `1038`) across the beneficiary's projects |
| `project_count` | |
| `suma_eu`, `suma_sr`, `suma_spolu` | summed across all their projects |
| `first_project_year`, `last_project_year` | derived from project planned-start dates |

Adds ICO-based identity, legal form, and sector on top of what
`top_recipients_chart.html` currently derives from project names alone in
client-side JS.

### 3. `datamart.procurement_contracts`

Grain: one row per (contract × linked project) — a contract can serve
multiple projects, sourced from `itms21_zmluvaverejneobstaravanie` →
`itms21_verejneobstaravanie_detail` → `itms21_verejneobstaravanie_detail_projekty`.

| Column | Notes |
|---|---|
| `contract_id`, `contract_kod`, `contract_nazov`, `cislozmluvy` | |
| `celkovasumazmluvy`, `sumabezdph` | |
| `datumucinnosti`, `datumplatnosti` | |
| `supplier_nazov`, `supplier_ico` | resolved via `COALESCE` across the two mutually-exclusive supplier FKs (`hlavnydodavatelsubjekt_id` → `itms21_subjekt`, `hlavnydodavateldodavatelobstaravatel_id` → `itms21_dodavatelobstaravatel`; confirmed no overlap: 1,014 contracts use the former, 6,200 the latter, summing to all 7,214) |
| `procurement_id`, `procurement_kod`, `procurement_nazov`, `procurement_stav` | parent procedure |
| `cpv_nazov` | main CPV category, decoded via ciselnik `1049` |
| `project_id`, `project_kod`, `project_nazov`, `program_skratka` | |
| `url_zmluva` | link to the published contract document |

### 4. `datamart.payment_disbursements`

Grain: one row per payment request (`itms21_zop`), 22,970 rows.

| Column | Notes |
|---|---|
| `zop_id`, `kod`, `typ` | e.g. ZÁLOHA, REFUNDÁCIA, PREDFINANCOVANIE |
| `projekt_id`, `project_kod`, `project_nazov` | `zop.projekt_id` is populated on 100% of rows |
| `program_id`, `program_skratka` | |
| `prijimatel_nazov` | |
| `narokovanasuma` | claimed amount |
| `event_date` | `COALESCE(datumuhrady, datumprijatia, createdat)`, normalized to epoch-ms — source columns inconsistently mix VARCHAR and BIGINT |
| `neuhradena`, `zopjezaverecna` | unpaid / final-settlement flags |

## Build & wiring

New `scripts/build_datamart.py`:
- Connects to `data/eufunds.duckdb`.
- For each of the 4 marts: `CREATE OR REPLACE TABLE datamart.<name> AS SELECT ...`, then `COMMENT ON` for table + every column.
- Exports each table to `docs/regional_funding_data.json`,
  `docs/beneficiary_funding_data.json`, `docs/procurement_contracts_data.json`,
  `docs/payment_disbursements_data.json` (same `export_to_json`-style helper
  pattern already used in `fetch_programs.py`/`fetch_projects.py`).
- Wired into `.github/workflows/monthly.yml` as the last step, after every
  existing fetch script (it depends on `slovakia.itms21_projekt`,
  `itms21_subjekt`, `itms21_zmluvaverejneobstaravanie`,
  `itms21_verejneobstaravanie_detail`, `itms21_zop`, and `itms21_ciselniky_detail`
  all being populated).
- Also added to `docs/*.json`'s `git add` list in the workflow's commit step.

## Webpages

Four new pages, matching the existing pill-nav / frosted-glass card style
(built per the `eurofondy-website-brand` skill), added to the shared nav
across every `docs/` page. Shared filter/aggregate helpers factored into a
new `docs/datamart_logic.js`, mirroring `projects_logic.js`'s
framework-free, pure-function style (usable from both a `<script>` tag and
Node).

- **`regional_funding.html`** — bar chart of contracted funding by kraj
  (`region_count = 1` projects only), plus a separate "nationwide
  programmes" stat tile; filterable by programme. Clicking a region bar
  shows a drill-down table of that region's projects.
- **`beneficiaries.html`** — searchable/sortable directory table (like
  `projects.html`) of beneficiaries ranked by total funding, plus a
  legal-form breakdown chart. Clicking a beneficiary shows their project
  list.
- **`procurement.html`** — top-suppliers-by-contract-value chart + sortable
  contract table, filterable by programme / CPV category / procedure status.
  Clicking a supplier shows all their contracts.
- **`disbursements.html`** — monthly and cumulative disbursed-amount line
  chart, filterable by programme and payment type. Includes a drill-down
  table of individual payment requests.

## Verification

- Run `python scripts/build_datamart.py` against the local DuckDB file and
  confirm all 4 tables + JSON exports are created without error.
- Serve `docs/` locally (`python -m http.server --directory docs 8000`) and
  click through all 4 pages: filters update charts, drill-downs populate,
  nav links work from every page.
- Sanity-check `regional_funding` and `beneficiary_funding` aggregate sums
  against the known per-programme totals in `docs/program_data.json`.
