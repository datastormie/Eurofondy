# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A data pipeline + static dashboard for Slovak EU structural funds (ITMS21,
2021–2027 programming period). It has three parts:

- `scripts/fetch_*.py` — Python scripts that pull data from the public
  `api.itms21.sk` API into a single shared DuckDB file.
- `scripts/build_datamart.py` — reads the raw `slovakia` schema tables and
  builds a derived `datamart` schema (6 tables: `regional_funding`,
  `regional_summary`, `program_summary`, `beneficiary_funding`,
  `procurement_contracts`, `payment_disbursements`), then exports each of
  those tables to a `docs/*_data.json` file for the website.
- `data/eufunds.duckdb` — the DuckDB store. Almost all raw fetched tables
  live in the `slovakia` schema (not the default `main`); the derived
  `datamart` schema (see above) holds the tables that back the website.
  **Not the source of truth in git**: the GitHub Actions workflow restores it
  from a GitHub Release asset (`gh release download data-store ...`) before
  each run and re-uploads it after, so the committed copy in the working tree
  can be stale. Don't assume the local file reflects the latest fetched data.
- `docs/` — a static GitHub Pages site (`index.html`, `projects.html`,
  `top_projects_chart.html`, `regional_funding.html`, `beneficiaries.html`,
  `procurement.html`, `disbursements.html`) that reads `docs/*_data.json`
  files exported by `scripts/build_datamart.py` (not the fetch scripts
  themselves).

There is no build step, package manager, or test suite — this is intentionally
minimal: plain Python scripts and plain HTML/CSS/vanilla JS with no bundler.

## Commands

```bash
pip install -r requirements.txt        # requests, pandas, duckdb — the only deps

# Run any single fetch script (each is independent and safe to re-run):
python scripts/fetch_programs.py
python scripts/fetch_projects.py
python scripts/fetch_ciselniky.py
# ...etc — see .github/workflows/monthly.yml for the full, ordered list

# Inspect the DuckDB store directly:
python -c "import duckdb; print(duckdb.connect('data/eufunds.duckdb').execute(\"SET schema='slovakia'\").execute('SHOW TABLES').fetchall())"

# Serve docs/ locally to check the dashboard:
python -m http.server --directory docs 8000
```

No linter, formatter, or test runner is configured. There's nothing to build.

## Architecture

### Fetch scripts (`scripts/fetch_*.py`)

Every script talks to one ITMS21 endpoint and follows one of a small number of
established sync patterns (full-overwrite, incremental-additive flat table,
incremental-additive normalized/decomposed schema, two-tier code list, or
incremental-additive with the detail already embedded in the list response so
there's no separate per-id call), sharing the same retry/backoff, dotted-path
JSON getters, and `itms21_`-prefixed lowercase table naming (scripts that do
make per-id detail calls also share `ThreadPoolExecutor(max_workers=8)`
concurrency). **Use the `eurofondy-itms21-fetch` skill** before writing or
modifying any fetch script — it documents which pattern to use and the exact
conventions to reuse rather than reinvent.

Key invariant across almost every script: once a row's primary key is known to
be stored, it is never re-fetched, updated, or deleted — sync is purely
additive.

All scripts are orchestrated in a fixed order by `.github/workflows/monthly.yml`,
which runs on the 1st of each month (`cron: "0 5 1 * *"`) and also supports
manual dispatch. That workflow is the authoritative list of which scripts
exist and in what order they run.

### Website (`docs/`)

Static pages fetch pre-generated JSON — `regional_funding_data.json`,
`regional_summary_data.json`, `program_summary_data.json`,
`beneficiary_funding_data.json`, `procurement_contracts_data.json`,
`payment_disbursements_data.json` — at page load and do all
filtering/sorting/pagination client-side; there is no backend for the site
itself. Those six JSON files are produced by `scripts/build_datamart.py` from
the `datamart` schema tables, not by the fetch scripts. `docs/projects_logic.js`
holds the pure, framework-free filter/sort/paginate functions shared across
pages (written to be usable from both a `<script>` tag and Node, though no
test harness currently exercises the Node path); `docs/datamart_logic.js`
holds additional shared helpers (grouping, program filtering, cumulative
sums) used by the newer datamart-driven pages. Charts use Chart.js loaded
from a CDN.

**Use the `eurofondy-website-brand` skill** before touching any file under
`docs/` — it defines the color palette, typography, and component patterns
(frosted-glass cards over a fixed EU-flag background, pill nav, gold accents)
so new pages/charts stay visually consistent with the existing ones instead of
introducing one-off styling.

### Data flow

`api.itms21.sk` → fetch script → `data/eufunds.duckdb` (`slovakia` schema) → (for a few tables)
`export_to_json()` → `docs/*_data.json` → static page renders it client-side.
Most fetched entities (vyzva, zonfp, ciselniky, aktivitaprojekt, etc.) are
DuckDB-only with no website page — they exist so the data is queryable, not
because they're displayed.

There's a second, derived fork of that flow for the website: `slovakia`
schema tables → `scripts/build_datamart.py` → `datamart` schema tables
(`regional_funding`, `regional_summary`, `program_summary`,
`beneficiary_funding`, `procurement_contracts`, `payment_disbursements`) →
JSON exports → `regional_funding.html`, `beneficiaries.html`,
`procurement.html`, `disbursements.html` (plus `projects.html` and
`top_projects_chart.html`, which also read `regional_funding_data.json`).
