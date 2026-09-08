#!/bin/sh
# Stop hook gate: only block the stop (to prompt an inline Montserrat check)
# when a docs/ html, css, or js file has uncommitted changes this session.
if git status --porcelain -- docs 2>/dev/null | grep -qE '\.(html|css|js)$'; then
  python -c "
import json
reason = (
    'Before stopping, verify Montserrat remains the single typeface used '
    'consistently across ALL html, css, and js files under docs/ (not only '
    'the ones changed this turn), per this project brand system: '
    '(1) every HTML page under docs/ that renders visible text loads '
    'Montserrat via the same Google Fonts link pattern used in '
    'docs/index.html; (2) docs/styles.css defines --font-display and '
    '--font-body as Montserrat, Arial, Helvetica, sans-serif; '
    '(3) every font-family declaration in styles.css or inline style '
    'blocks uses var(--font-display) or var(--font-body) or that exact '
    'stack (font-family: inherit is fine); (4) any Chart.js page sets '
    'Chart.defaults.font.family to that same stack; (5) only font '
    'weights 400, 500, 600, and 700 are used. Fix any violations found, '
    'then finish.'
)
print(json.dumps({'decision': 'block', 'reason': reason}))
"
fi
