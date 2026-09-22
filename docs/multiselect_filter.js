// Shared searchable multi-select dropdown filter, used for every text-based
// filter across the site (Program, Recipient, Code, Supplier, Legal form,
// Status, Payment type, ...) so they all look and behave the same way.
// Option labels are rendered verbatim from the data — never re-cased — so
// values keep their source capitalization.
//
// In the browser this file is loaded as a plain <script>, exposing
// window.MultiselectFilter. Markup contract (see any filter-group in the
// pages under docs/ for a concrete example):
//   <div class="multiselect" id="...Multi">
//     <button class="ms-toggle" id="...Toggle"><span id="...ToggleLabel"></span>...</button>
//     <div class="ms-panel">
//       <div class="ms-search"><input id="...Search"></div>   (optional)
//       <div class="ms-actions">
//         <button id="...SelectAll">Select all</button>
//         <button id="...ClearAll">Clear</button>
//       </div>
//       <div class="ms-options" id="...Options"></div>
//     </div>
//   </div>

(function (root) {
  function init(config) {
    const selected = new Set(config.initialSelected || []);
    const multi = document.getElementById(config.multiId);
    const toggle = document.getElementById(config.toggleId);
    const toggleLabel = document.getElementById(config.toggleLabelId);
    const search = config.searchId ? document.getElementById(config.searchId) : null;
    const selectAllBtn = config.selectAllId ? document.getElementById(config.selectAllId) : null;
    const clearAllBtn = config.clearAllId ? document.getElementById(config.clearAllId) : null;
    const optionsContainer = document.getElementById(config.optionsId);
    let items = [];
    let emptyEl = null;

    function labelForCount(count) {
      if (config.labelForCount) return config.labelForCount(count);
      return count + ' selected';
    }

    function updateToggleLabel() {
      const count = selected.size;
      if (count === 0) {
        toggleLabel.textContent = config.allLabel;
      } else if (count === 1) {
        const only = items.find(function (i) { return i.key === Array.from(selected)[0]; });
        toggleLabel.textContent = only ? only.label : labelForCount(1);
      } else {
        toggleLabel.textContent = labelForCount(count);
      }
    }

    function filterVisible(query) {
      const q = (query || '').trim().toLowerCase();
      const opts = optionsContainer.querySelectorAll('.ms-option');
      let visibleCount = 0;
      opts.forEach(function (opt) {
        const match = !q || opt.dataset.search.indexOf(q) !== -1;
        opt.classList.toggle('ms-option-hidden', !match);
        if (match) visibleCount += 1;
      });
      if (visibleCount === 0) {
        if (!emptyEl) {
          emptyEl = document.createElement('div');
          emptyEl.className = 'ms-empty';
          emptyEl.textContent = config.noMatchLabel || 'No matches.';
          optionsContainer.appendChild(emptyEl);
        }
      } else if (emptyEl) {
        emptyEl.remove();
        emptyEl = null;
      }
    }

    // items: [{ key, label, searchText? }] — label is shown verbatim.
    function setItems(newItems) {
      items = newItems;
      emptyEl = null;
      optionsContainer.innerHTML = '';
      items.forEach(function (item) {
        const wrap = document.createElement('label');
        wrap.className = 'ms-option';
        wrap.title = item.label;
        wrap.dataset.search = (item.searchText || item.label).toLowerCase();
        const cb = document.createElement('input');
        cb.type = 'checkbox';
        cb.value = item.key;
        cb.checked = selected.has(item.key);
        const span = document.createElement('span');
        span.textContent = item.label;
        cb.addEventListener('change', function () {
          if (cb.checked) selected.add(item.key);
          else selected.delete(item.key);
          updateToggleLabel();
          config.onChange(Array.from(selected));
        });
        wrap.appendChild(cb);
        wrap.appendChild(span);
        optionsContainer.appendChild(wrap);
      });
      if (search) filterVisible(search.value);
      updateToggleLabel();
    }

    function clearAll() {
      selected.clear();
      optionsContainer.querySelectorAll('input[type="checkbox"]').forEach(function (cb) { cb.checked = false; });
      updateToggleLabel();
    }

    function selectAllVisible() {
      optionsContainer.querySelectorAll('.ms-option:not(.ms-option-hidden) input[type="checkbox"]').forEach(function (cb) {
        cb.checked = true;
        selected.add(cb.value);
      });
      updateToggleLabel();
    }

    function setSelected(keys) {
      selected.clear();
      (keys || []).forEach(function (k) { if (k) selected.add(k); });
      optionsContainer.querySelectorAll('input[type="checkbox"]').forEach(function (cb) {
        cb.checked = selected.has(cb.value);
      });
      updateToggleLabel();
    }

    toggle.addEventListener('click', function (e) {
      e.stopPropagation();
      multi.classList.toggle('open');
      toggle.setAttribute('aria-expanded', multi.classList.contains('open') ? 'true' : 'false');
    });
    document.addEventListener('click', function (e) {
      if (!multi.contains(e.target)) multi.classList.remove('open');
    });
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && multi.classList.contains('open')) {
        multi.classList.remove('open');
        toggle.setAttribute('aria-expanded', 'false');
      }
    });
    if (search) {
      search.addEventListener('input', function (e) { filterVisible(e.target.value); });
    }
    if (selectAllBtn) {
      selectAllBtn.addEventListener('click', function () {
        selectAllVisible();
        config.onChange(Array.from(selected));
      });
    }
    if (clearAllBtn) {
      clearAllBtn.addEventListener('click', function () {
        clearAll();
        config.onChange(Array.from(selected));
      });
    }

    return {
      setItems: setItems,
      getSelected: function () { return Array.from(selected); },
      setSelected: setSelected,
      reset: function () {
        clearAll();
        if (search) { search.value = ''; filterVisible(''); }
      },
    };
  }

  const api = { init: init };
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  } else {
    root.MultiselectFilter = api;
  }
})(typeof window !== 'undefined' ? window : globalThis);
