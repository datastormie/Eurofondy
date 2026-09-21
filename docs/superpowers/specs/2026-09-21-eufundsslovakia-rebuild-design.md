# EU Funds Slovakia — datamart-only site rebuild

Status: Approved. Date: 2026-09-21.

Supersedes `2026-09-20-datamart-explorer-design.md` for the 4 marts it
describes (they still exist, with amendments below) but replaces its "add 4
pages alongside the existing site" approach with a full rebuild of every
`docs/` page onto the `datamart` schema, and retires the `website` schema
entirely.

## Context

The site currently has two data sources feeding `docs/`:

- `website.itms21_programs_current` / `website.itms21_projects_current` →
  `docs/program_data.json` / `docs/project_data.json` → `index.html`,
  `projects.html`, `top_projects_chart.html`, `top_recipients_chart.html`.
- `datamart.*` (this project, in progress) → not yet wired to any page.

Keeping both is redundant — `datamart.regional_funding` already recomputes
everything `website.itms21_projects_current` provided (verified to match
exactly, see `docs/superpowers/specs/project_funding.sql`), and nothing
downstream needs the `website` schema anymore. This project drops it and
rebuilds every page on `datamart`, using the existing brand system unchanged
(no new visual design — same `docs/styles.css`, Montserrat, frosted-glass
cards, pill nav from the `eurofondy-website-brand` skill).

## Approach

1. **Retire `website` schema.** Strip the "current table + JSON export" half
   out of `fetch_programs.py` and `fetch_projects.py` (keep the normalized
   `slovakia`-schema sync in both — `datamart` depends on it). Drop the
   `website` schema from the DuckDB file. Delete `docs/program_data.json` and
   `docs/project_data.json`.
2. **Add one new datamart table**, `datamart.program_summary`, to replace
   what `website.itms21_programs_current` provided for `index.html` (no
   existing datamart table aggregates at the programme level).
3. **Amend `datamart.regional_funding`** to add planned start/end dates, so
   it can double as the general project-summary source for `projects.html`
   and `top_projects_chart.html` (replacing `project_data.json`).
4. **Rebuild all `docs/` pages** against `datamart` exports; retire
   `top_recipients_chart.html` (superseded by the richer `beneficiaries.html`
   below — same underlying idea, better data).
5. **Add 4 new pages** essentially as scoped in the superseded design, with
   3 amendments: `regional_funding.html` gets an SVG choropleth map (not just
   a bar chart), `beneficiaries.html` links out to Finstat, `procurement.html`
   gets a per-supplier VO rollup.

## Data model changes

### `datamart.program_summary` (new)

Grain: one row per programme.

| Column | Notes |
|---|---|
| `program_id, kod, skratka, nazov_sk, nazov_en` | from `slovakia.itms21_program` |
| `riadiaci_organ, subjekt_nazov` | managing authority, denormalized for display |
| `project_count` | `count(*)` from `itms21_projekt` grouped by `program_id` |
| `suma_eu, suma_sr, suma_spolu` | summed from `datamart.regional_funding` per programme (already-correct EU/SR split) — **not** `itms21_program.sumaeu/sumasr/sumaspolu`, which are the programme's total *allocation* ceiling, not what's actually contracted to projects; both are useful so both get exposed under distinct names: `alokacia_eu/sr/spolu` (from `itms21_program`) vs `zazmluvnene_eu/sr/spolu` (summed from `regional_funding`) |

### `datamart.regional_funding` (amended)

Add two columns: `planovany_zaciatok`, `planovany_koniec` (epoch ms, from
`itms21_projekt.planovanarealizaciazaciatok` / `...koniec`) — the same
source fields `website.itms21_projects_current` used, now sourced directly
from `slovakia`. No other columns change.

### `datamart.beneficiary_funding`, `procurement_contracts`,
### `payment_disbursements`, `regional_summary`

Unchanged from the current `project_funding.sql` definitions.

## Page-by-page plan

| Page | Source(s) | Change |
|---|---|---|
| `index.html` | `program_summary_data.json` | Rebuilt: programme table + new bar chart (projects-per-programme, volume-per-programme, toggleable) |
| `projects.html` | `regional_funding_data.json` | Rebuilt on the new source; same filter/sort/paginate UX |
| `top_projects_chart.html` | `regional_funding_data.json` | Repointed only, no UX change |
| `top_recipients_chart.html` | — | **Deleted**, nav links removed |
| `beneficiaries.html` | `beneficiary_funding_data.json` | New. ICO renders as a link to `https://www.finstat.sk/{ico}` (`target="_blank" rel="noopener"`), hidden when `ico` is null. Clicking a beneficiary row navigates to `projects.html?recipient=<nazov>` (existing query-param filter support). |
| `procurement.html` | `procurement_contracts_data.json` | New. Contract table + top-suppliers-by-value chart. Per-supplier drill-down aggregates that supplier's rows client-side (via `datamart_logic.js` `groupSum`) into a VO list — each entry shows `procurement_nazov`/`procurement_stav` and links to that VO's linked project(s); no new DB table needed, 7,714 rows is well within client-side aggregation precedent. |
| `disbursements.html` | `payment_disbursements_data.json` | New, as originally scoped: monthly + cumulative disbursement chart, filterable by programme/type, drill-down table. |
| `regional_funding.html` | `regional_funding_data.json` + `regional_summary_data.json` | New. SVG choropleth of the 8 kraje (see below) colored by `suma_spolu` from `regional_summary`, plus the nationwide-projects callout and per-region project drill-down table, as originally scoped. |

Shared nav (`.nav-links`) updated on every page: Programs, Projects, Top
Projects, Regional Funding, Beneficiaries, Procurement, Disbursements — 7
links, `top_recipients_chart.html` removed.

## Slovakia choropleth map

A small public-domain SVG of the 8 kraj boundaries (sourced from Wikimedia
Commons, simplified) is added as `docs/assets/slovakia_kraje.svg`, inlined
directly in `regional_funding.html` (not `<img>`, so JS can reach into it).
Each region `<path>` gets `id="kraj-{nuts3_id}"`; on data load, JS sets
`fill` per path via a sequential color scale (reusing the existing gold/navy
brand palette — light-to-dark navy) driven by `regional_summary.suma_spolu`,
with a legend and hover tooltip showing the exact amount + project count.
Clicking a region behaves like the existing bar-chart click handler from the
superseded design (shows that region's project drill-down table). The
8-region requirement is fixed (Slovakia's kraje don't change), so the SVG is
a one-time static asset, not generated.

## Migration & cleanup

- `scripts/fetch_programs.py`: remove `WEBSITE_SCHEMA`, `TABLE_CURRENT*`,
  `ensure_table`, `migrate_to_website_schema`, `upsert_row`,
  `flatten_program`, and `export_to_json` — all of these exist solely to
  feed the website table/JSON and have no other caller. Keep
  `fetch_programs`, `fetch_detail`, and everything from `ensure_full_schema`
  down (the normalized `itms21_program` / `itms21_program_graf` sync into
  `slovakia`, which `datamart` depends on) untouched.
- `scripts/fetch_projects.py`: same shape of removal (website-schema current
  table + `export_to_json`), keep the normalized `itms21_projekt` sync.
- One-time migration statement (run once, not part of the regular script):
  `DROP SCHEMA website CASCADE;`
- `git rm docs/program_data.json docs/project_data.json docs/top_recipients_chart.html`.
- `.github/workflows/monthly.yml`: remove any step ordering assumptions
  tied to `website` schema; update the final `git add` list to the new
  datamart JSON filenames (`program_summary_data.json`,
  `regional_funding_data.json`, `regional_summary_data.json`,
  `beneficiary_funding_data.json`, `procurement_contracts_data.json`,
  `payment_disbursements_data.json`); `scripts/build_datamart.py` runs last,
  after every fetch script.

## Verification

- `python scripts/build_datamart.py` runs clean against the local DB and
  produces all 6 JSON exports.
- `SELECT * FROM information_schema.schemata` no longer lists `website`.
- Serve `docs/` locally and click through all 7 remaining pages: nav links
  resolve, every chart renders, every drill-down populates, the Finstat link
  opens `finstat.sk/<ico>` in a new tab, the choropleth recolors on load and
  is clickable.
- Sanity-check `program_summary.zazmluvnene_spolu` summed across all
  programmes against `regional_funding`'s grand total (should match exactly,
  same source).
