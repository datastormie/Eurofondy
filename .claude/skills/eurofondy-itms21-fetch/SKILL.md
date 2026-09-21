---
name: eurofondy-itms21-fetch
description: Conventions for writing or modifying a scripts/fetch_*.py script that pulls data from the ITMS21 public API (api.itms21.sk) into data/eufunds.duckdb (the `slovakia` schema; the website-facing `datamart` schema is built separately by scripts/build_datamart.py, not by fetch scripts). Use this whenever adding a new ITMS21 endpoint, adding a new field/child-table to an existing fetch script, debugging why a fetch script re-fetches or duplicates rows, or wiring a new script into .github/workflows/monthly.yml. Also use when the user mentions ITMS21, DuckDB sync scripts, or "fetch script" in this repo, even if they don't name a specific file.
---

# Eurofondy ITMS21 fetch scripts

All `scripts/fetch_*.py` files pull from `https://api.itms21.sk/public/v1/...` into
one shared DuckDB file, `data/eufunds.duckdb`. Every table a fetch script
creates lives in the `slovakia` schema (each script sets `DB_SCHEMA =
"slovakia"` and runs `CREATE SCHEMA IF NOT EXISTS` + `SET schema = ...` right
after connecting — copy this on every new script too). The website-facing
`datamart` schema (`regional_funding`, `regional_summary`, `program_summary`,
`beneficiary_funding`, `procurement_contracts`, `payment_disbursements`) is
built separately by `scripts/build_datamart.py` from the `slovakia` tables —
fetch scripts never write to `datamart` and never need a "current state"
table of their own. They are run monthly by
`.github/workflows/monthly.yml`, in a fixed order (roughly: programs → projects →
ciselniky → vyzvy → planovanavyzvy → priorita → specifickycielprogramu → opatrenie
→ typakcieprogramu → zonfp → zop → aktivitaprojekt). Every script is independent
and idempotent — re-running it must never duplicate or corrupt data.

Before writing a new script, decide which of the five archetypes below fits the
API endpoint, then reuse the shared building blocks that every script already
relies on. Don't invent a new sync strategy — consistency across scripts is what
keeps this maintainable by future-you.

**Before committing to archetype #2/#3 (i.e. any script that plans a separate
`/id/{id}` detail call), quickly check two things with a one-off `curl`:**
whether the `/id/{id}` endpoint actually reaches the origin, and whether the
list endpoint's `?limit=-1` response already embeds full nested detail per
record (open one record and look for nested objects/arrays beyond just
`id`/`kod`/`nazov`, not just bare refs). If the detail endpoint returns a
body like `{"supportID": "..."}` with none of the normal app response headers
(`Content-Security-Policy`, `Cross-Origin-Embedder-Policy`, an `imsIngressApi`
cookie — present on every real response including ordinary 404s) that's an
F5 ASM/BIG-IP WAF block page, not an app-level error, and retrying harder
won't help. As of writing, the detail endpoints for `projektovyzamerius`,
`strategiaius`, `zmluvyverejnehoobstaravania`, and `polozkarozpocetprojekt`
are WAF-blocked for at least some clients/IPs — don't assume a 403 there is a
script bug. If the list response already has everything, use archetype #5
instead of fighting the block.

## Pick the archetype

1. **Full-overwrite, flat table** — small list endpoint (tens/hundreds of rows),
   no separate detail endpoint, whole list re-fetched every run.
   Example: `fetch_organimplementaciefondov.py` (`itms21_organimplementaciefondov`).
   Existing rows are always **UPDATE**d with fresh data; nothing is ever deleted.
   DuckDB-only — see the JSON-export note below.

2. **Incremental additive, flat detail table** — list endpoint returns ids only;
   a separate `/id/{id}` detail endpoint returns one flat record (no nested
   list fields worth normalizing).
   Example: `fetch_aktivitaprojekt.py` (`itms21_aktivitaprojekt`).
   Detail is fetched **only for ids not already in the table** and INSERTed
   once — never updated, never re-fetched, never deleted.

3. **Incremental additive, normalized/decomposed schema** — same list+detail
   shape as #2, but the detail JSON has many nested list fields (arrays of
   objects, sometimes two levels deep) that are worth querying individually.
   Examples: `fetch_projects.py`, `fetch_zonfp.py`, `fetch_vyzvy.py`.
   One parent table (`itms21_<entity>`) plus one child table per nested list
   field (`itms21_<ENTITY>_<FIELD>`), each carrying `<entity>_id` back to the
   parent. A field nested two levels deep (e.g. a "dokument" list inside a
   child row) becomes a grandchild table carrying both the parent id and,
   where the API provides it, the child row's id.
   This is the most common shape for "detail" endpoints in this API — when in
   doubt, this is probably the right one.

4. **Two-tier code list (ciselniky-style)** — a list of *categories*, each of
   which has its own detail list of *items*.
   Example: `fetch_ciselniky.py` (`itms21_ciselnik` + `itms21_ciselniky_detail`).
   Categories are upserted (full overwrite per category); items within a
   category are additive, keyed on `(category_kod, item_id)`.

5. **Incremental additive, list-embedded detail (no detail call)** — the list
   endpoint's `?limit=-1` response already embeds each record's full nested
   detail inline (single-object refs *and* one-to-many list refs), so there
   is no separate `/id/{id}` call at all — one list request replaces both
   the list-for-ids step and the per-id detail step used in #2/#3.
   Examples: `fetch_projektovyzamerius.py`, `fetch_strategiaius.py` (both
   adopted this after their `/id/{id}` detail endpoints turned out to be
   WAF-blocked — see the check above — but the pattern applies any time the
   list response is already rich enough, WAF or not).
   Same normalization rule as #3 (single-object fields → flat `<name>_id`
   columns on the parent; one-to-many list fields → child tables carrying
   `<entity>_id`), same insert-once gating via `get_known_ids()`, just
   sourced from `fetch_list()`'s records directly instead of a per-id
   `fetch_detail()`. Because there's no per-id network call, these scripts
   skip `ThreadPoolExecutor`/`MAX_WORKERS`/`REQUEST_TIMEOUT` entirely — only
   `RETRY_ATTEMPTS`/`RETRY_BACKOFF_SECONDS`/`LIST_REQUEST_TIMEOUT` apply — and
   `main()`'s summary drops the "failed" count (a single list call either
   succeeds or raises; print `"Done. {total} total in list, {stored} newly
   stored."` instead). Child tables here only need to carry the bare `id`
   (and `href` if present) of the referenced record — the referenced
   entity's own detail lives in *its* fetch script's table, so don't
   duplicate the nested payload.
   **Sanity-check cardinality before picking flat-column vs. child-table**:
   a field that looks like a single nested object in one sample record can
   still be an array in the raw JSON (e.g. `"vyzva": [...]` with one item)
   — `_get()` silently returns `None` for a list instead of raising, so a
   wrong assumption here fails quietly (every value in that column is
   `NULL`) rather than with an error. Pull a handful of real records
   (`curl ".../entity?limit=20"`) and check `isinstance(value, list)`
   for every nested field before writing the column mapping, don't infer
   the shape from a single example.

No fetch script produces a `docs/*_data.json` export or website page anymore
— every fetch script is **DuckDB-only**, writing purely-additive tables into
the `slovakia` schema. The website is instead fed by `scripts/build_datamart.py`,
which reads from `slovakia` tables (including `itms21_program` and
`itms21_projekt`) and builds the derived `datamart` schema + its own JSON
exports. Don't add a JSON export to a fetch script — that's build_datamart.py's
job, not any individual fetch script's.

## Shared building blocks (copy these, don't reinvent)

- **Retry helper**: every network call wraps `requests.get(..., timeout=...)` in
  a loop of `RETRY_ATTEMPTS = 3` with `time.sleep(RETRY_BACKOFF_SECONDS * attempt)`
  backoff (`RETRY_BACKOFF_SECONDS = 2`). List-endpoint calls use a longer
  `LIST_REQUEST_TIMEOUT = 180` (the `limit=-1` list response can be large);
  detail calls use `REQUEST_TIMEOUT = 30`. Archetype #5 scripts have no
  detail call, so they only define `RETRY_ATTEMPTS`/`RETRY_BACKOFF_SECONDS`/
  `LIST_REQUEST_TIMEOUT` — don't add `REQUEST_TIMEOUT` for a call that
  doesn't exist.
- **Concurrency**: detail fetches (archetypes #2/#3) run through
  `ThreadPoolExecutor(max_workers=MAX_WORKERS)` with `MAX_WORKERS = 8` — keep it
  at 8 unless there's a reason to change it; it's a deliberate cap to avoid
  hammering the API. Archetype #5 scripts have nothing to parallelize (one
  list call, then in-memory decomposition) and correctly have no
  `ThreadPoolExecutor`/`MAX_WORKERS` at all.
- **Dotted-path getter**: every script defines the same `_get(d, "a.b.c")`
  helper to walk nested dicts safely (returns `None` on any missing/None
  segment). Copy it verbatim.
- **Table naming**: `TABLE_PREFIX = "itms21_"`, all table and column names
  lowercase. Every script that predates the prefix/lowercase convention
  carries a one-time, idempotent migration (`migrate_table_prefix` /
  `_rename_columns`, built on `ALTER TABLE IF EXISTS ... RENAME TO/COLUMN ...`)
  that runs on every invocation and is a no-op once already migrated. A new
  script doesn't need this migration machinery — just create tables with the
  final lowercase, prefixed names from the start.
- **Column mapping tables**: nested schemas are declared as
  `list[tuple[column_name, dotted_json_path, duckdb_type]]` module-level
  constants (e.g. `PROJEKT_COLUMNS`, `ZONFP_COLUMNS`), and child tables as
  `dict[bare_table_name, tuple[source_list_field, list[(column, path, type)]]]`
  (`CHILD_TABLES`). This makes `ensure_full_schema`/`store_full_detail` fully
  generic — write the mapping, not bespoke INSERT logic, for every new field.
- **Gating / no re-fetch**: `get_known_ids(con)` reads existing primary keys
  before fetching; only ids missing from that set are fetched. This is what
  makes re-running a script safe and cheap. Never add an UPDATE path to an
  archetype-#3-style script — those tables are insert-once by design (the API
  data for a closed project/call doesn't change). Archetype #1 tables
  (`ensure_table` + `ON CONFLICT (...) DO UPDATE`) are the only ones meant to
  be overwritten on every run.
- **Periodic commit**: commit every 50 processed detail records (20 for
  ciselniky's per-category loop) *and* after the final one, so an interrupted
  run doesn't lose all progress: `if i % 50 == 0 or i == len(to_fetch): con.commit()`.
- **JSON export** (archetype #1 / #3-with-summary only): a separate
  `export_to_json()` reads the table back with `duckdb` + `pandas`, converts
  epoch-millisecond BIGINT columns to ISO datetimes with
  `pd.to_datetime(df[col], unit="ms", errors="coerce")`, and writes
  `{"generated_at": ..., "<entity>s": [...]}"` to `docs/<entity>_data.json`.
- **`main()`** always does the sync, prints a one-line summary
  (`"Done. {total} total in list, {fetched} newly fetched, {failed} failed."`
  for archetypes #1-#4; archetype #5 has no per-id failure mode, so it prints
  `"Done. {total} total in list, {stored} newly stored."` instead), then
  calls `export_to_json()` if applicable.
- **Table/column documentation**: every table and column carries an English
  `COMMENT ON` description, stored in-database (queryable via
  `duckdb_tables()` / `duckdb_columns()`), not in a separate doc file. Each
  script declares `TABLE_COMMENTS: dict[str, str]` (table name → one-sentence
  description of what the table holds and its sync semantics — additive vs.
  overwritten) and `COLUMN_COMMENTS: dict[str, dict[str, str]]` (table name →
  {column name → one-sentence description}) right before its `ensure_table`/
  `ensure_full_schema` function, plus the shared `_esc()` (single-quote
  escaping) and `apply_comments(con)` helpers (copy verbatim — same shape in
  every script). `apply_comments(con)` is called once, right after
  `ensure_table`/`ensure_full_schema`, in every function that opens a
  connection (both the main sync function and `export_to_json()`, when
  present). `COMMENT ON` simply overwrites, so this is idempotent and safe to
  run on every invocation — it's what keeps a brand-new table/column
  documented automatically the moment a script adds one, with no separate
  backfill step required. When adding a new column to an existing table (or a
  new child table), add its description to the relevant dict in the same
  change.

## Wiring in a new script

1. Add the new `python scripts/fetch_<entity>.py` line to the `Run fetch
   scripts` step in `.github/workflows/monthly.yml`, in the same relative
   position as similar entities (detail-heavy scripts near their siblings).
2. If it needs new Python packages, add them to `requirements.txt` (currently
   just `requests`, `pandas`, `duckdb` — most new scripts won't need more).
3. If it adds a `docs/*_data.json` export, also add that file path to the
   `git add` line in the workflow's "Commit and push updated data" step, and
   see [[eurofondy-website-brand]] before touching any `docs/*.html`/`*.js`/
   `*.css` to add a page or nav link for it.
4. Don't touch `data/eufunds.duckdb` locally in a way that fights the
   workflow's release-based persistence (`gh release download/upload
   data-store`) — that file is restored from a GitHub Release asset at the
   start of each monthly run, not committed to git.