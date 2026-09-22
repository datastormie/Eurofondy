// Shared column-resize behavior for data tables across the site.
// Call TableResize.makeResizable(tableEl) once a table's <thead> is in the
// DOM and the table is actually visible (not display:none) — resizing needs
// each <th>'s rendered width as the starting point. Safe to call more than
// once per table; it no-ops after the first successful init.

(function (root) {
  function updateFrozenOffset(table, firstColWidth) {
    // The second column is pinned with a hardcoded `left` matching the
    // first column's default width (see styles.css) — keep it in sync
    // when the first column is resized so the two don't overlap/gap.
    table.querySelectorAll('thead th:nth-child(2), tbody td:nth-child(2)').forEach(function (cell) {
      cell.style.left = firstColWidth + 'px';
    });
  }

  function makeResizable(table) {
    if (!table || table.dataset.resizableInit === '1') return;
    const headRow = table.querySelector('thead tr');
    if (!headRow) return;
    const ths = Array.from(headRow.children);
    if (!ths.length) return;
    // A hidden table reports 0 width for every column — bail out rather
    // than freezing every column at 0px.
    if (table.offsetWidth === 0) return;

    table.dataset.resizableInit = '1';
    // .resizable-th reserves right-padding for the drag handle — add it
    // before measuring, so the frozen width already has room for that
    // padding instead of stealing space from the header text (which would
    // otherwise get ellipsis-truncated the moment table-layout goes fixed).
    ths.forEach(function (th) { th.classList.add('resizable-th'); });
    ths.forEach(function (th) { th.style.width = th.getBoundingClientRect().width + 'px'; });
    table.style.tableLayout = 'fixed';

    ths.forEach(function (th, index) {
      const handle = document.createElement('span');
      handle.className = 'col-resize-handle';
      handle.setAttribute('aria-hidden', 'true');
      th.appendChild(handle);

      let startX = 0;
      let startWidth = 0;

      function onMove(e) {
        const clientX = e.touches ? e.touches[0].clientX : e.clientX;
        const newWidth = Math.max(60, startWidth + (clientX - startX));
        th.style.width = newWidth + 'px';
        if (index === 0) updateFrozenOffset(table, newWidth);
      }
      function onUp() {
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
        document.removeEventListener('touchmove', onMove);
        document.removeEventListener('touchend', onUp);
        document.body.classList.remove('col-resizing');
      }
      function onDown(e) {
        e.preventDefault();
        e.stopPropagation(); // don't trigger the th's sort-click handler
        startX = e.touches ? e.touches[0].clientX : e.clientX;
        startWidth = th.getBoundingClientRect().width;
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
        document.addEventListener('touchmove', onMove, { passive: false });
        document.addEventListener('touchend', onUp);
        document.body.classList.add('col-resizing');
      }

      handle.addEventListener('mousedown', onDown);
      handle.addEventListener('touchstart', onDown, { passive: false });
      handle.addEventListener('click', function (e) { e.stopPropagation(); });
    });
  }

  const api = { makeResizable };
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  } else {
    root.TableResize = api;
  }
})(typeof window !== 'undefined' ? window : globalThis);
