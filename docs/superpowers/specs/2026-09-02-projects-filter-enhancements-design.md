# Projects filter enhancements — design

Date: 2026-09-02

## Summary

Two independent UX changes to the static site in `docs/`:

1. The Program filter on the Projects table (`docs/projects.html`) becomes a multi-select checkbox dropdown instead of a single-select `<select>`.
2. The Top 50 Projects chart (`docs/top_projects_chart.html`) becomes clickable: clicking a bar navigates to the Projects table pre-filtered to that project's code, with an explanatory note added near the chart.

These ship together but touch different files/functions and can be reviewed/tested independently.

## 1. Program filter → multi-select checkbox dropdown

**Files:** `docs/projects.html`, `docs/projects_logic.js`, `docs/styles.css`

**Markup change** (`projects.html`): replace

```html
<select id="filterProgram">
  <option value="">All programs</option>
</select>
```

with a custom dropdown:

```html
<div class="multi-select" id="filterProgramGroup">
  <button type="button" class="multi-select-trigger" id="filterProgramTrigger" aria-haspopup="listbox" aria-expanded="false">
    All programs
  </button>
  <div class="multi-select-panel" id="filterProgramPanel" hidden>
    <div class="multi-select-actions">
      <button type="button" id="filterProgramSelectAll">Select all</button>
      <button type="button" id="filterProgramClear">Clear</button>
    </div>
    <div class="multi-select-options" id="filterProgramOptions"></div>
  </div>
</div>
```

Checkboxes for each program are generated into `#filterProgramOptions`, one `<label><input type="checkbox" value="SKRATKA"> SKRATKA</label>` per entry from `ProjectsLogic.distinctPrograms(data)` (same data source as today).

**Behavior** (`projects.html` inline script):
- Trigger button toggles the panel (`hidden` attribute) and updates `aria-expanded`.
- Clicking outside the panel or pressing Escape closes it.
- Trigger label updates on every change: `"All programs"` (0 selected), the program code itself (1 selected), or `"N programs selected"` (2+).
- "Select all" checks every checkbox; "Clear" unchecks all. Both trigger a re-render.
- Each checkbox's `change` event resets `currentPage = 1` and calls `render()`, matching the existing filter wiring pattern.

**Data flow** (`getFilters()` in `projects.html`):
- `program` becomes an array: `Array.from(document.querySelectorAll('#filterProgramOptions input:checked')).map(cb => cb.value)`.

**Filter logic** (`applyFilters` in `projects_logic.js`):
- Change the `program` check from `row.program_skratka !== filters.program` to: if `filters.program` is a non-empty array, keep the row only if `filters.program.includes(row.program_skratka)`. An empty array means no filter (same semantic as the current `""`).

**Deep-link compatibility** (`applyFiltersFromUrl()` in `projects.html`):
- The existing `?program=X` link from `index.html`'s program chart continues to work: it finds and checks the single matching checkbox (instead of setting `select.value`), then updates the trigger label and banner as today.

**Reset filters / clear active filter:**
- `resetFilters` and `clearActiveFilter` handlers uncheck all program checkboxes and reset the trigger label to "All programs", in addition to their existing behavior.

**New CSS** (`styles.css`): `.multi-select`, `.multi-select-trigger` (styled like existing `.filter-group select`), `.multi-select-panel` (absolutely positioned dropdown panel, scrollable if the program list is long), `.multi-select-options label` (checkbox rows), `.multi-select-actions` (small text-button pair). Follows the existing color tokens (`--eu-blue`, `--hairline`, etc.) and control sizing already used by `.filter-group`.

## 2. Top 50 chart → click-through to filtered table

**Files:** `docs/top_projects_chart.html`, `docs/projects.html`

**Chart click handler** (`top_projects_chart.html`):
- Capture a `kods` array in parallel with the existing `labels`/`fullNames`/`programs` arrays: `const kods = ordered.map(function (p) { return p.kod || ''; });`.
- Add `onClick` to the Chart.js `options`:
  ```js
  onClick: function (evt, elements) {
    if (!elements.length) return;
    const kod = kods[elements[0].index];
    if (kod) window.location.href = 'projects.html?kod=' + encodeURIComponent(kod);
  }
  ```
- Add `options.onHover` that sets `evt.native.target.style.cursor = elements.length ? 'pointer' : 'default'`, since a canvas chart gives no other affordance that a bar is clickable.

**Note near the chart** (`top_projects_chart.html`):
- Add a `<p class="hint">Tip: click a bar to view that project's full details in the Projects table.</p>` directly under `.updated`, before `.chart-wrap` — same `.hint` component already used on `projects.html`.

**URL param handling** (`applyFiltersFromUrl()` in `projects.html`):
- Add a `kod` branch alongside the existing `program`/`recipient` branches:
  ```js
  const kod = params.get('kod');
  ...
  } else if (kod) {
    document.getElementById('filterKod').value = kod;
    document.getElementById('activeFilterText').textContent = 'Showing project with code:';
    document.getElementById('activeFilterLabel').textContent = kod;
    document.getElementById('activeFilterBanner').style.display = 'block';
  }
  ```
- `clearActiveFilter` handler additionally clears `filterKod` when a `kod` filter is active (it currently clears `filterProgram`/`filterRecipient`; extend to also blank `filterKod`).

## Out of scope

- No changes to `top_recipients_chart.html` or `index.html` beyond what's already there.
- No changes to sorting, pagination, or other filters.
- No automated test suite exists for this static site; verification is manual in-browser (see plan).