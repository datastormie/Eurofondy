// Shared logic for the datamart explorer pages -- pure functions, testable
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

  // Recipients are keyed by ICO where available, falling back to name for
  // the handful of rows with no ICO (see regional_funding.html).
  function recipientKey(row) {
    return row.prijimatel_ico || row.prijimatel_nazov;
  }

  function filterByRecipient(data, keys) {
    if (!keys || keys.length === 0) return data;
    const keySet = new Set(keys);
    return data.filter(function (row) { return keySet.has(recipientKey(row)); });
  }

  function monthKey(value) {
    const d = new Date(value);
    const year = d.getUTCFullYear();
    const month = String(d.getUTCMonth() + 1).padStart(2, '0');
    return year + '-' + month;
  }

  const api = { groupSum, withCumulative, dedupeBy, filterByProgram, recipientKey, filterByRecipient, monthKey };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  } else {
    root.DatamartLogic = api;
  }
})(typeof window !== 'undefined' ? window : globalThis);
