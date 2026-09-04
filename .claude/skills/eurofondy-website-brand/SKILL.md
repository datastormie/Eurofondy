---
name: eurofondy-website-brand
description: Brand book / visual design system for the "EU Funds Slovakia" website in docs/ (index.html, projects.html, top_projects_chart.html, top_recipients_chart.html, styles.css). Use whenever creating or editing any file under docs/, adding a new dashboard/chart page, styling a table or filter UI, choosing colors or fonts for this project, or picking Chart.js colors for a new visualization — so new pages look like they belong to the same site instead of introducing an inconsistent one-off style.
---

# EU Funds Slovakia — website brand book

This is a data-portal for EU structural funds in Slovakia (2021–2027), published
from `docs/` as a static GitHub Pages site fed by `docs/*_data.json` (see
[[eurofondy-itms21-fetch]] for how that data is produced). The design language is
**official but modern**: EU flag navy/gold, glass panels over a fixed background
photo, one typeface everywhere. Reuse `docs/styles.css` — don't hand-roll new
colors, fonts, radii, or component patterns in a page's own `<style>` block
unless it's genuinely page-specific (e.g. a chart's height at different
breakpoints, which the existing chart pages already do inline).

## Colors

All defined as CSS custom properties in `docs/styles.css` `:root` — reference
the variable, not the literal hex, in new CSS:

| Variable | Hex | Use |
|---|---|---|
| `--navy-deep` | `#071630` | background gradient start, darkest tone |
| `--navy-base` | `#0b2454` | headings (`h1`), the darker "ink navy" |
| `--eu-blue` | `#003399` | primary action color: table headers, links, focus borders, chart "EU contribution" bars, nav underline hover state |
| `--eu-blue-hover` | `#002670` | hover state for `--eu-blue` elements |
| `--eu-gold` | `#ffcc00` | accent only: title underline rule, active nav underline, sort-arrow glyphs, focus outline, hint-callout border |
| `--ink` | `#1f2937` | body text |
| `--muted` | `#5b6472` | secondary text (subtitles, hints) |
| `--panel` | `rgba(255,255,255,0.94)` | the frosted "card" background |
| `--panel-border` | `rgba(255,255,255,0.55)` | card border |
| `--hairline` | `#e4e7ee` | table row dividers |
| chart secondary | `#7C93C7` | "national (SR) contribution" bars — a desaturated blue, never gold, so gold stays reserved for interactive/accent use |

Gold is an **accent color, never a fill/background** for large areas — it marks
the one thing that should draw the eye (active nav item, title rule, sort
arrow, focus ring). Using it for body backgrounds or large buttons would break
the pattern established across every existing page.

## Typography

Single family everywhere: **Montserrat** (weights 400/500/600/700), loaded via
Google Fonts preconnect + stylesheet link, falling back to `Arial, Helvetica,
sans-serif`. Both `--font-display` and `--font-body` resolve to the same
family — there is deliberately no serif/secondary display font. When adding a
new page, copy the exact `<link>` block from an existing page (`index.html`
lines 7–9) rather than re-picking a font.

If a page renders a Chart.js chart, set `Chart.defaults.font.family` to the
same Montserrat stack so chart labels don't visually clash with the rest of
the page (every existing chart page does this).

## Layout & components

- **Background**: fixed, full-viewport EU flag photo (`docs/assets/eu-flag-background.jpg`)
  with a dark navy gradient overlay (`body::before`/`body::after`), so content
  scrolls over a static image rather than the image scrolling with the page.
- **Topbar**: pill-shaped (`border-radius: 999px`), frosted glass
  (`backdrop-filter: blur(10px)`) nav bar, centered, max-width 1300px. Current
  page gets a gold underline (`class="current"`); other links get one on
  hover/focus only.
- **Logo mark**: a 22×22 SVG — navy circle (`#003399`) with 6 small gold
  (`#FFCC00`) dots arranged in a ring, evoking the EU flag's stars without
  reproducing the full 12-star circle. Reuse the existing inline SVG verbatim
  (see `index.html` lines 30–36) rather than redrawing it.
- **Card**: the main content container — frosted glass
  (`backdrop-filter: blur(16px)`), white-ish panel (`--panel`), `16px` radius,
  soft dark shadow. `.card--narrow` (max-width 1100px) for single-focus pages
  like a chart or the program table; the default 1300px width for the wider
  projects table.
- **Title block**: centered `h1` in `--navy-base`, followed by a short gold
  `.title-rule` bar (56×3px) and a muted `.subtitle` — this three-part header
  pattern opens every page.
- **Hint callout**: `.hint` — pale gold background, gold left border, used for
  a single short usage tip under the header (e.g. "click a row to see its
  projects"). Don't overuse; one hint per page.
- **Filters**: light-blue-tinted panel (`rgba(0,51,153,0.045)` background) in a
  responsive grid (4 columns desktop → 2 → 1), uppercase 10.5px labels in
  `--eu-blue`. Multi-select dropdowns (`.multiselect`) expand as a floating
  panel on desktop and inline (static, no overlay) on mobile — a card with
  `backdrop-filter` becomes a containing block for `position: fixed`, which
  breaks floating overlays, so mobile intentionally avoids that.
- **Tables**: sticky `--eu-blue` header row with white text, gold ▲/▼ sort
  indicators, first two columns frozen on desktop/tablet (unfrozen on mobile —
  screen too narrow to spare the space), row hover tint `#f2f5fb`, numeric
  columns right-aligned with `font-variant-numeric: tabular-nums`.
- **Buttons**: `.btn-secondary` — white background, `--eu-blue` text/border,
  light-blue hover fill. There is no filled/primary button style in this
  system; actions are secondary-styled or plain links/rows.
- **Radii**: `16px` cards, `8px` controls/inputs, `999px` pills (topbar only).
- **Charts**: Chart.js bar charts. EU contribution = `--eu-blue` (`#003399`),
  SR/national contribution = the secondary blue `#7C93C7`. Stacked horizontal
  bars for "top N" rankings, stacked vertical for category comparisons. Money
  axis labels use a shared `formatEURShort` (`€1.2 M`, `€350 K`) — reuse that
  function rather than writing a new formatter.
- **Focus/accessibility floor**: every interactive element gets a `2px` gold
  `outline` on `:focus-visible`. Keep this on any new interactive element.

## Responsive breakpoints

`1024px` (filters collapse to 2 columns), `768px` (card padding shrinks, table
columns unfreeze, multi-select goes inline), `480px` (smallest type-size step).
Reuse these exact breakpoints for new components instead of picking new ones.

## Voice

Labels are short, bilingual-aware (Slovak data, English UI chrome — see page
titles like "EU Funding Programs — Slovakia 2021–2027"), and functional: this
is a data tool, not a marketing site. No taglines, no hero copy — just a title,
a one-line subtitle, a last-updated timestamp, and the data. Keep new pages to
that same restraint.
