// LA County Auction Tracker — static UI.
// Loads data/properties.json (or data/sample.json as fallback), then keeps a
// filtered list and syncs three views of it: the filter chips, the table, and
// the Leaflet map.

(() => {
  const LA_CENTER = [34.05, -118.25];
  const DATA_URLS = ["properties.json", "sample.json"];

  const state = {
    all: [],            // full dataset, never mutated after load
    filtered: [],       // current filtered + sorted view
    categories: [],     // discovered category names
    statuses: [],       // discovered default-status values
    activeCategories: new Set(),
    activeStatuses: new Set(),
    query: "",
    bidMin: null,
    bidMax: null,
    onlyMappable: false,
    sort: { key: "min_bid", dir: "asc" },
    selectedAin: null,
  };

  // ------ data loading -----------------------------------------------------

  async function loadData() {
    for (const url of DATA_URLS) {
      try {
        const r = await fetch(url, { cache: "no-store" });
        if (!r.ok) continue;
        const data = await r.json();
        return { ...data, _source: url };
      } catch (_) { /* try next */ }
    }
    throw new Error("Could not load any data file");
  }

  // ------ map --------------------------------------------------------------

  const map = L.map("map", { zoomControl: true }).setView(LA_CENTER, 9);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: '&copy; <a href="https://openstreetmap.org/copyright">OpenStreetMap</a>',
  }).addTo(map);

  const markersById = new Map();  // ain -> L.Marker
  const markerLayer = L.layerGroup().addTo(map);

  function popupHtml(p) {
    const bid = p.min_bid != null ? `$${p.min_bid.toLocaleString()}` : "—";
    const status = p.default_status || "unknown";

    // Build the "details" block out of whichever assessor fields we have.
    // Each entry is a [label, value] pair; entries with an empty value are
    // dropped so we don't show a grid full of em-dashes for sparse parcels.
    const details = [
      ["Use", p.use_desc || p.use_code],
      ["Building", p.impr_desc],
      ["Year", p.year_built],
      ["Beds / Baths", bedsBathsLabel(p)],
      ["Units", p.units],
      ["Lot sqft", fmtNum(p.sqft_lot)],
      ["Bldg sqft", fmtNum(p.sqft_building)],
      ["Zoning", p.zoning],
      ["Assessed (total)", p.assessed_total != null ? fmtMoney(p.assessed_total) : null],
      ["Last sale", lastSaleLabel(p)],
    ].filter(([, v]) => v != null && v !== "" && v !== "—");

    const detailHtml = details.length
      ? `<div class="popup-details">${details
          .map(([k, v]) => `<div><span>${escapeHtml(k)}</span>${escapeHtml(v)}</div>`)
          .join("")}</div>`
      : "";

    return `
      <strong>${escapeHtml(p.situs || p.ain_formatted || "Parcel")}</strong>
      AIN ${escapeHtml(p.ain_formatted || p.ain)}<br>
      Min bid: ${bid}<br>
      ${escapeHtml(p.category || "")} <span class="badge ${status}">${status}</span>
      ${detailHtml}
      <a href="${escapeHtml(p.assessor_url)}" target="_blank" rel="noopener">Assessor</a>
      &nbsp;·&nbsp;
      <a href="${escapeHtml(p.ttc_url)}" target="_blank" rel="noopener">TTC</a>
    `;
  }

  function bedsBathsLabel(p) {
    // Only show the combined "3 / 2" if we have at least one side.
    if (p.bedrooms == null && p.bathrooms == null) return null;
    return `${p.bedrooms ?? "—"} / ${p.bathrooms ?? "—"}`;
  }

  function lastSaleLabel(p) {
    if (!p.last_sale_date && p.last_sale_price == null) return null;
    const price = p.last_sale_price != null ? fmtMoney(p.last_sale_price) : "—";
    // ArcGIS serializes dates as epoch-milliseconds integers; if we get a
    // number, format it; otherwise show the string we got.
    let date = p.last_sale_date ?? "—";
    if (typeof date === "number") {
      try { date = new Date(date).toLocaleDateString(); } catch (_) { /* leave as number */ }
    }
    return `${price} on ${date}`;
  }

  function rebuildMarkers() {
    markerLayer.clearLayers();
    markersById.clear();
    const bounds = [];
    for (const p of state.filtered) {
      if (p.lat == null || p.lng == null) continue;
      const m = L.marker([p.lat, p.lng]).bindPopup(popupHtml(p));
      m.on("click", () => selectRow(p.ain));
      m.addTo(markerLayer);
      markersById.set(p.ain, m);
      bounds.push([p.lat, p.lng]);
    }
    if (bounds.length) {
      map.fitBounds(bounds, { padding: [30, 30], maxZoom: 14 });
    }
  }

  // ------ table ------------------------------------------------------------

  const tbody = document.querySelector("#tbl tbody");

  function fmtMoney(v) {
    if (v == null || v === "") return "—";
    const n = Number(v);
    if (!Number.isFinite(n)) return "—";
    return `$${n.toLocaleString()}`;
  }
  function fmtNum(v) {
    if (v == null || v === "") return "—";
    const n = Number(v);
    return Number.isFinite(n) ? n.toLocaleString() : "—";
  }
  function escapeHtml(s) {
    return String(s ?? "").replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    }[c]));
  }

  function renderTable() {
    const rows = state.filtered.map((p) => {
      const status = p.default_status || "unknown";
      return `
        <tr data-ain="${escapeHtml(p.ain)}" class="${state.selectedAin === p.ain ? "selected" : ""}">
          <td>${escapeHtml(p.item_no || "")}</td>
          <td>${escapeHtml(p.ain_formatted || p.ain)}</td>
          <td>${fmtMoney(p.min_bid)}</td>
          <td class="addr" title="${escapeHtml(p.situs || "")}">${escapeHtml(p.situs || "—")}</td>
          <td>${escapeHtml(p.category || "")}</td>
          <td>${escapeHtml(p.use_desc || p.use_code || "")}</td>
          <td>${escapeHtml(p.year_built || "")}</td>
          <td>${fmtNum(p.sqft_lot)}</td>
          <td><span class="badge ${status}">${status}</span></td>
          <td>
            <a href="${escapeHtml(p.assessor_url)}" target="_blank" rel="noopener">Assr</a>
            · <a href="${escapeHtml(p.ttc_url)}" target="_blank" rel="noopener">TTC</a>
          </td>
        </tr>
      `;
    });
    tbody.innerHTML = rows.join("");
    document.getElementById("count").textContent =
      `${state.filtered.length.toLocaleString()} of ${state.all.length.toLocaleString()} properties`;
  }

  tbody.addEventListener("click", (e) => {
    const tr = e.target.closest("tr[data-ain]");
    if (!tr) return;
    selectRow(tr.dataset.ain);
  });

  function selectRow(ain) {
    state.selectedAin = ain;
    // update selection styling without full re-render
    tbody.querySelectorAll("tr.selected").forEach((el) => el.classList.remove("selected"));
    const row = tbody.querySelector(`tr[data-ain="${CSS.escape(ain)}"]`);
    if (row) {
      row.classList.add("selected");
      row.scrollIntoView({ block: "nearest" });
    }
    const m = markersById.get(ain);
    if (m) {
      map.setView(m.getLatLng(), Math.max(map.getZoom(), 14), { animate: true });
      m.openPopup();
    }
  }

  // ------ sorting ----------------------------------------------------------

  document.querySelectorAll("#tbl thead th[data-sort]").forEach((th) => {
    th.addEventListener("click", () => {
      const key = th.dataset.sort;
      if (state.sort.key === key) {
        state.sort.dir = state.sort.dir === "asc" ? "desc" : "asc";
      } else {
        state.sort.key = key;
        state.sort.dir = "asc";
      }
      document.querySelectorAll("#tbl thead th").forEach((el) =>
        el.classList.remove("sort-asc", "sort-desc"));
      th.classList.add(state.sort.dir === "asc" ? "sort-asc" : "sort-desc");
      applyFilters();
    });
  });

  function sortRows(rows) {
    const { key, dir } = state.sort;
    const sign = dir === "asc" ? 1 : -1;
    return rows.slice().sort((a, b) => {
      const av = a[key], bv = b[key];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;   // nulls always last
      if (bv == null) return -1;
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * sign;
      return String(av).localeCompare(String(bv), undefined, { numeric: true }) * sign;
    });
  }

  // ------ filters ----------------------------------------------------------

  function applyFilters() {
    const q = state.query.trim().toLowerCase();
    const filtered = state.all.filter((p) => {
      if (state.activeCategories.size && !state.activeCategories.has(p.category || "Unknown")) return false;
      if (state.activeStatuses.size && !state.activeStatuses.has(p.default_status || "unknown")) return false;
      if (state.bidMin != null && (p.min_bid == null || p.min_bid < state.bidMin)) return false;
      if (state.bidMax != null && (p.min_bid == null || p.min_bid > state.bidMax)) return false;
      if (state.onlyMappable && (p.lat == null || p.lng == null)) return false;
      if (q) {
        const hay = [
          p.ain, p.ain_formatted, p.situs, p.use_desc, p.use_code,
          p.category, p.zoning, p.item_no,
        ].map((x) => String(x ?? "").toLowerCase()).join(" ");
        if (!hay.includes(q)) return false;
      }
      return true;
    });
    state.filtered = sortRows(filtered);
    renderTable();
    rebuildMarkers();
  }

  function renderChips(containerId, values, activeSet) {
    const container = document.getElementById(containerId);
    container.innerHTML = "";
    for (const v of values) {
      const el = document.createElement("button");
      el.type = "button";
      el.className = "chip" + (activeSet.has(v) ? " active" : "");
      el.textContent = v;
      el.addEventListener("click", () => {
        if (activeSet.has(v)) activeSet.delete(v); else activeSet.add(v);
        el.classList.toggle("active");
        applyFilters();
      });
      container.appendChild(el);
    }
  }

  document.getElementById("q").addEventListener("input", (e) => {
    state.query = e.target.value;
    applyFilters();
  });
  document.getElementById("bid-min").addEventListener("input", (e) => {
    const v = e.target.value;
    state.bidMin = v === "" ? null : Number(v);
    applyFilters();
  });
  document.getElementById("bid-max").addEventListener("input", (e) => {
    const v = e.target.value;
    state.bidMax = v === "" ? null : Number(v);
    applyFilters();
  });
  document.getElementById("only-mappable").addEventListener("change", (e) => {
    state.onlyMappable = e.target.checked;
    applyFilters();
  });
  document.getElementById("reset").addEventListener("click", () => {
    state.activeCategories.clear();
    state.activeStatuses.clear();
    state.query = "";
    state.bidMin = null;
    state.bidMax = null;
    state.onlyMappable = false;
    document.getElementById("q").value = "";
    document.getElementById("bid-min").value = "";
    document.getElementById("bid-max").value = "";
    document.getElementById("only-mappable").checked = false;
    renderChips("category-filters", state.categories, state.activeCategories);
    renderChips("status-filters", state.statuses, state.activeStatuses);
    applyFilters();
  });

  // ------ view toggle ------------------------------------------------------
  //
  // The header's segmented control switches among three layouts:
  //   - "map":  full-height map, list hidden
  //   - "both": split view (default)
  //   - "list": full-height list, map hidden
  // The choice is persisted in localStorage. When we re-show the map we have
  // to call Leaflet's ``invalidateSize`` because it lazily measures its
  // container and will otherwise render at whatever size it had when hidden.

  const VIEWS = ["map", "both", "list"];
  const STORAGE_KEY = "reauction.view";
  const mainEl = document.getElementById("main");

  function setView(view) {
    if (!VIEWS.includes(view)) view = "both";
    for (const v of VIEWS) mainEl.classList.toggle(`view-${v}`, v === view);
    document.querySelectorAll(".view-toggle button").forEach((b) => {
      b.classList.toggle("active", b.dataset.view === view);
    });
    try { localStorage.setItem(STORAGE_KEY, view); } catch (_) { /* ignore */ }
    // Let the browser apply the layout change, then re-measure the map.
    if (view !== "list") {
      requestAnimationFrame(() => map.invalidateSize());
    }
  }

  document.querySelectorAll(".view-toggle button").forEach((btn) => {
    btn.addEventListener("click", () => setView(btn.dataset.view));
  });

  // Restore the last choice, defaulting to "both".
  let initialView = "both";
  try { initialView = localStorage.getItem(STORAGE_KEY) || "both"; } catch (_) { /* ignore */ }
  setView(initialView);

  // ------ boot -------------------------------------------------------------

  (async function boot() {
    const meta = document.getElementById("meta");
    try {
      const data = await loadData();
      state.all = data.properties || [];

      const cats = new Set();
      const statuses = new Set();
      for (const p of state.all) {
        cats.add(p.category || "Unknown");
        statuses.add(p.default_status || "unknown");
      }
      state.categories = [...cats].sort();
      state.statuses = [...statuses].sort();
      renderChips("category-filters", state.categories, state.activeCategories);
      renderChips("status-filters", state.statuses, state.activeStatuses);

      // Default sort indicator
      document.querySelector('#tbl thead th[data-sort="min_bid"]').classList.add("sort-asc");

      const when = data.generated_at
        ? new Date(data.generated_at * 1000).toLocaleString()
        : "sample data";
      const src = data._source && data._source.includes("sample")
        ? " (sample preview — run the pipeline for real data)"
        : "";
      meta.textContent = `${state.all.length.toLocaleString()} parcels · ${when}${src}`;

      applyFilters();
    } catch (e) {
      meta.textContent = "Failed to load data. Run the pipeline or copy data/sample.json to data/properties.json.";
      console.error(e);
    }
  })();
})();
