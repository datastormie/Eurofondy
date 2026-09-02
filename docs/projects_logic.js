// Shared logic for projects.html — pure functions, testable in Node and browser alike.
// In the browser this file is loaded as a plain <script>, exposing window.ProjectsLogic.
// In Node (for testing) it's exported via module.exports.

(function (root) {
  const PAGE_SIZE = 25;

  function toMs(value) {
    if (value == null || value === '') return null;
    const ms = new Date(value).getTime();
    return Number.isNaN(ms) ? null : ms;
  }

  function applyFilters(data, filters) {
    return data.filter(function (row) {
      if (filters.kod) {
        if (!(row.kod || '').toLowerCase().includes(filters.kod.toLowerCase())) return false;
      }
      if (filters.nazov) {
        if (!(row.nazov || '').toLowerCase().includes(filters.nazov.toLowerCase())) return false;
      }
      if (filters.program) {
        if (row.program_skratka !== filters.program) return false;
      }
      if (filters.zaciatokFrom || filters.zaciatokTo) {
        const rowMs = toMs(row.planovany_zaciatok);
        if (rowMs == null) return false;
        if (filters.zaciatokFrom && rowMs < filters.zaciatokFrom) return false;
        if (filters.zaciatokTo && rowMs > filters.zaciatokTo) return false;
      }
      if (filters.koniecFrom || filters.koniecTo) {
        const rowMs = toMs(row.planovany_koniec);
        if (rowMs == null) return false;
        if (filters.koniecFrom && rowMs < filters.koniecFrom) return false;
        if (filters.koniecTo && rowMs > filters.koniecTo) return false;
      }
      return true;
    });
  }

  function sortData(data, key, dir, typeOf) {
    const type = typeOf(key);
    return [...data].sort(function (a, b) {
      let av = a[key];
      let bv = b[key];
      if (type === 'number') {
        av = Number(av);
        bv = Number(bv);
        if (Number.isNaN(av)) av = dir === 'asc' ? Infinity : -Infinity;
        if (Number.isNaN(bv)) bv = dir === 'asc' ? Infinity : -Infinity;
        return dir === 'asc' ? av - bv : bv - av;
      }
      if (type === 'date') {
        av = toMs(av);
        bv = toMs(bv);
        if (av == null) av = dir === 'asc' ? Infinity : -Infinity;
        if (bv == null) bv = dir === 'asc' ? Infinity : -Infinity;
        return dir === 'asc' ? av - bv : bv - av;
      }
      if (type === 'bool') {
        av = av ? 1 : 0;
        bv = bv ? 1 : 0;
        return dir === 'asc' ? av - bv : bv - av;
      }
      av = (av || '').toString().toLowerCase();
      bv = (bv || '').toString().toLowerCase();
      if (av < bv) return dir === 'asc' ? -1 : 1;
      if (av > bv) return dir === 'asc' ? 1 : -1;
      return 0;
    });
  }

  function paginate(data, page, pageSize) {
    pageSize = pageSize || PAGE_SIZE;
    const totalPages = Math.max(1, Math.ceil(data.length / pageSize));
    const clampedPage = Math.min(Math.max(1, page), totalPages);
    const start = (clampedPage - 1) * pageSize;
    const pageRows = data.slice(start, start + pageSize);
    return {
      rows: pageRows,
      page: clampedPage,
      totalPages: totalPages,
      totalRows: data.length,
      start: data.length === 0 ? 0 : start + 1,
      end: Math.min(start + pageSize, data.length),
    };
  }

  function topNByAmount(data, n, amountKey) {
    return [...data]
      .filter(function (r) { return r[amountKey] != null; })
      .sort(function (a, b) { return b[amountKey] - a[amountKey]; })
      .slice(0, n);
  }

  function distinctPrograms(data) {
    const seen = new Map();
    data.forEach(function (row) {
      if (row.program_skratka && !seen.has(row.program_skratka)) {
        seen.set(row.program_skratka, row.program_nazov);
      }
    });
    return Array.from(seen.entries()).map(function (e) {
      return { skratka: e[0], nazov: e[1] };
    }).sort(function (a, b) { return a.skratka.localeCompare(b.skratka); });
  }

  const api = { applyFilters, sortData, paginate, topNByAmount, distinctPrograms, PAGE_SIZE };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  } else {
    root.ProjectsLogic = api;
  }
})(typeof window !== 'undefined' ? window : globalThis);