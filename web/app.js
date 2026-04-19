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
    statuses: [],       // discovered effective-status values (Assessor tax_status preferred)
    useCodes: [],       // discovered use-code values, with counts
    lienTypes: [],      // discovered lien-type keys (irs/weed/brush/special)
    activeCategories: new Set(),
    activeStatuses: new Set(),
    activeUseCodes: new Set(),
    activeLienTypes: new Set(),
    useCodeQuery: "",   // text filter over the use-code chip list itself
    query: "",
    bidMin: null,
    bidMax: null,
    onlyMappable: false,
    onlyLiens: false,
    onlySpecial: false,
    sort: { key: "min_bid", dir: "asc" },
    selectedAin: null,
  };

  // Effective status prefers the Assessor portal's tax-status label (which we
  // scrape for each parcel) over the TTC vcheck "default_status" heuristic.
  // The two sources use different vocabularies, so normalize the TTC fallback
  // to the same labels the Assessor scrape produces. That way "Tax Defaulted"
  // from one source and "in_default" from the other collapse into one filter
  // chip rather than two.
  const DEFAULT_STATUS_LABELS = {
    in_default: "Tax Defaulted",
    redeemed: "Redeemed",
    unknown: "Unknown",
    skipped: "Unknown",
  };
  function effectiveStatus(p) {
    if (p.tax_status) return p.tax_status;
    const d = p.default_status;
    if (d) return DEFAULT_STATUS_LABELS[d] || d;
    return "Unknown";
  }

  // CSS-safe class name derived from an effective-status label, so "Tax
  // Defaulted" -> "status-tax-defaulted". Keeps the badge palette driven by
  // status text instead of a fixed enum.
  function statusClass(label) {
    return "status-" + String(label || "unknown")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "");
  }

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
    const status = effectiveStatus(p);
    const sClass = statusClass(status);

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

    // Flag blocks: special conditions and city liens. Each only renders when
    // the parcel actually has the data, so clean parcels stay compact.
    const specials = Array.isArray(p.special_conditions) ? p.special_conditions : [];
    const specialHtml = specials.length
      ? `<div class="popup-flags popup-flags-special"><strong>Special conditions</strong>
           <ul>${specials.map((c) => `<li>${escapeHtml(c)}</li>`).join("")}</ul></div>`
      : "";

    const liens = Array.isArray(p.liens) ? p.liens : [];
    const lienHtml = liens.length
      ? `<div class="popup-flags popup-flags-liens">
           <strong>City liens · total ${fmtMoney(p.lien_total)}</strong>
           <ul>${liens.slice(0, 6).map((l) => {
             const type = l && l.lien_type_label ? ` [${escapeHtml(l.lien_type_label)}]` : "";
             return `<li>${escapeHtml(l.desc || "Lien")}${type} — ${fmtMoney(l.amount)}</li>`;
           }).join("")}${liens.length > 6 ? `<li>+ ${liens.length - 6} more</li>` : ""}</ul>
         </div>`
      : "";

    return `
      <strong>${escapeHtml(p.situs || p.ain_formatted || "Parcel")}</strong>
      AIN ${escapeHtml(p.ain_formatted || p.ain)}<br>
      Min bid: ${bid}<br>
      ${escapeHtml(p.category || "")} <span class="badge ${sClass}">${escapeHtml(status)}</span>
      ${detailHtml}
      ${specialHtml}
      ${lienHtml}
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

  // Which lien/SC types get their own pill in the row flags. Kept short so
  // the flags column stays readable; anything else collapses into a generic
  // "Lien <total>" chip.
  const TYPE_FLAG_META = {
    irs:          { css: "flag-irs",          short: "IRS" },
    weed:         { css: "flag-weed",         short: "Weed" },
    brush:        { css: "flag-brush",        short: "Brush" },
    state_tax:    { css: "flag-irs",          short: "State Tax" },
  };

  function flagsCellHtml(p) {
    const parts = [];
    const lienTotal = Number(p.lien_total) || 0;
    const liens = Array.isArray(p.liens) ? p.liens : [];
    const totalsByType = (p.lien_totals_by_type && typeof p.lien_totals_by_type === "object")
      ? p.lien_totals_by_type : {};

    // Per-type lien pills come first so IRS/weed/brush are immediately
    // visible at a glance.
    const renderedTypes = new Set();
    for (const [key, meta] of Object.entries(TYPE_FLAG_META)) {
      const sub = Number(totalsByType[key]) || 0;
      if (sub <= 0) continue;
      renderedTypes.add(key);
      const tip = `${meta.short} lien${sub === 0 ? "" : " " + fmtMoney(sub)}`;
      parts.push(
        `<span class="flag ${meta.css}" title="${escapeHtml(tip)}">${escapeHtml(meta.short)} ${fmtMoney(sub)}</span>`
      );
    }
    // Generic "Lien <total>" only if there's residual lien money not already
    // covered by a per-type pill.
    const typedTotal = [...renderedTypes].reduce((s, k) => s + (Number(totalsByType[k]) || 0), 0);
    const residual = lienTotal - typedTotal;
    if (residual > 0 || (lienTotal === 0 && liens.length > 0)) {
      const tip = liens.length
        ? `${liens.length} lien${liens.length === 1 ? "" : "s"} totaling ${fmtMoney(lienTotal)}`
        : fmtMoney(lienTotal);
      parts.push(
        `<span class="flag flag-lien" title="${escapeHtml(tip)}">Lien ${fmtMoney(residual > 0 ? residual : lienTotal)}</span>`
      );
    }
    const specials = Array.isArray(p.special_conditions) ? p.special_conditions : [];
    const scTypes = Array.isArray(p.special_condition_types) ? p.special_condition_types : [];
    if (specials.length > 0) {
      const tip = specials.join("; ");
      const sub = scTypes.length ? ` (${scTypes.slice(0, 3).map((t) => t.replace(/_/g, " ")).join(", ")}${scTypes.length > 3 ? "…" : ""})` : "";
      parts.push(
        `<span class="flag flag-special-cond" title="${escapeHtml(tip)}">SC${specials.length > 1 ? ` ×${specials.length}` : ""}${escapeHtml(sub)}</span>`
      );
    }
    return parts.join(" ");
  }

  function renderTable() {
    const rows = state.filtered.map((p) => {
      const status = effectiveStatus(p);
      const sClass = statusClass(status);
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
          <td><span class="badge ${sClass}">${escapeHtml(status)}</span></td>
          <td class="flags-cell">${flagsCellHtml(p)}</td>
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

  // Derived sort values — handles columns (like "Status") that don't map to a
  // single raw field on the property record.
  function sortValue(p, key) {
    if (key === "status_effective") return effectiveStatus(p);
    return p[key];
  }

  function sortRows(rows) {
    const { key, dir } = state.sort;
    const sign = dir === "asc" ? 1 : -1;
    return rows.slice().sort((a, b) => {
      const av = sortValue(a, key), bv = sortValue(b, key);
      if (av == null && bv == null) return 0;
      if (av == null) return 1;   // nulls always last
      if (bv == null) return -1;
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * sign;
      return String(av).localeCompare(String(bv), undefined, { numeric: true }) * sign;
    });
  }

  // ------ filters ----------------------------------------------------------

  function propertyUseCodeKey(p) {
    return (p.use_code || "").trim() || "—";
  }

  function propertyLienTypes(p) {
    // Return the union of dollar-lien types and "sc:"-prefixed special
    // condition types, so the single "Lien type" filter can gate on either
    // source of flags without the caller having to know the split.
    const set = new Set();
    const liens = Array.isArray(p.liens) ? p.liens : [];
    for (const l of liens) {
      if (l && l.lien_type) set.add(l.lien_type);
    }
    const scTypes = Array.isArray(p.special_condition_types) ? p.special_condition_types : [];
    for (const t of scTypes) if (t) set.add(`sc:${t}`);
    return set;
  }

  function renderLienTypeChips() {
    const container = document.getElementById("lien-type-filters");
    if (!container) return;
    container.innerHTML = "";
    for (const t of state.lienTypes) {
      const el = document.createElement("button");
      el.type = "button";
      el.className = "chip" + (state.activeLienTypes.has(t.key) ? " active" : "");
      el.textContent = `${t.label} (${t.count})`;
      el.addEventListener("click", () => {
        if (state.activeLienTypes.has(t.key)) state.activeLienTypes.delete(t.key);
        else state.activeLienTypes.add(t.key);
        el.classList.toggle("active");
        applyFilters();
      });
      container.appendChild(el);
    }
  }

  function applyFilters() {
    const q = state.query.trim().toLowerCase();
    const filtered = state.all.filter((p) => {
      if (state.activeCategories.size && !state.activeCategories.has(p.category || "Unknown")) return false;
      if (state.activeStatuses.size && !state.activeStatuses.has(effectiveStatus(p))) return false;
      if (state.activeUseCodes.size && !state.activeUseCodes.has(propertyUseCodeKey(p))) return false;
      if (state.activeLienTypes.size) {
        const types = propertyLienTypes(p);
        let hit = false;
        for (const t of state.activeLienTypes) {
          if (types.has(t)) { hit = true; break; }
        }
        if (!hit) return false;
      }
      if (state.bidMin != null && (p.min_bid == null || p.min_bid < state.bidMin)) return false;
      if (state.bidMax != null && (p.min_bid == null || p.min_bid > state.bidMax)) return false;
      if (state.onlyMappable && (p.lat == null || p.lng == null)) return false;
      if (state.onlyLiens && !(Number(p.lien_total) > 0 || (p.liens && p.liens.length))) return false;
      if (state.onlySpecial && !(p.special_conditions && p.special_conditions.length)) return false;
      if (q) {
        const hay = [
          p.ain, p.ain_formatted, p.situs, p.use_desc, p.use_code,
          p.category, p.zoning, p.item_no,
          ...(p.special_conditions || []),
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

  // Use-code chips render "<code> — <desc>  (<count>)" and are filtered by a
  // small search box so long lists (LA County exposes ~400 distinct codes)
  // stay navigable. ``state.useCodes`` is an array of { code, desc, count }.
  function renderUseCodeChips() {
    const container = document.getElementById("use-code-filters");
    if (!container) return;
    container.innerHTML = "";
    const q = state.useCodeQuery.trim().toLowerCase();
    const visible = state.useCodes.filter((u) => {
      if (!q) return true;
      const hay = `${u.code} ${u.desc}`.toLowerCase();
      return hay.includes(q);
    });
    for (const u of visible) {
      const el = document.createElement("button");
      el.type = "button";
      el.className = "chip" + (state.activeUseCodes.has(u.code) ? " active" : "");
      const label = u.desc ? `${u.code} · ${u.desc}` : u.code;
      el.textContent = `${label} (${u.count})`;
      el.title = u.desc ? `${u.code} — ${u.desc}` : u.code;
      el.addEventListener("click", () => {
        if (state.activeUseCodes.has(u.code)) state.activeUseCodes.delete(u.code);
        else state.activeUseCodes.add(u.code);
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
  const useCodeSearch = document.getElementById("use-code-search");
  if (useCodeSearch) {
    useCodeSearch.addEventListener("input", (e) => {
      state.useCodeQuery = e.target.value;
      renderUseCodeChips();
    });
  }
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
  document.getElementById("only-liens").addEventListener("change", (e) => {
    state.onlyLiens = e.target.checked;
    applyFilters();
  });
  document.getElementById("only-special").addEventListener("change", (e) => {
    state.onlySpecial = e.target.checked;
    applyFilters();
  });
  document.getElementById("reset").addEventListener("click", () => {
    state.activeCategories.clear();
    state.activeStatuses.clear();
    state.activeUseCodes.clear();
    state.activeLienTypes.clear();
    state.useCodeQuery = "";
    state.query = "";
    state.bidMin = null;
    state.bidMax = null;
    state.onlyMappable = false;
    state.onlyLiens = false;
    state.onlySpecial = false;
    document.getElementById("q").value = "";
    document.getElementById("bid-min").value = "";
    document.getElementById("bid-max").value = "";
    document.getElementById("only-mappable").checked = false;
    document.getElementById("only-liens").checked = false;
    document.getElementById("only-special").checked = false;
    const ucs = document.getElementById("use-code-search");
    if (ucs) ucs.value = "";
    renderChips("category-filters", state.categories, state.activeCategories);
    renderChips("status-filters", state.statuses, state.activeStatuses);
    renderUseCodeChips();
    renderLienTypeChips();
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
      // Use-code map: code -> { desc, count }. We pick the most common desc
      // per code, since the raw Assessor data occasionally spells the same
      // code with slight variations.
      const useCodeMap = new Map();
      // lien_type key -> { label, count }. Special-condition types are added
      // as synthetic entries prefixed with "sc:" so the "Lien type" filter
      // can span both sources of flags in one list.
      const lienTypeMap = new Map();
      const addLienType = (key, label) => {
        if (!key) return;
        const entry = lienTypeMap.get(key) || { key, label: label || key, count: 0 };
        entry.count += 1;
        if (!entry.label && label) entry.label = label;
        lienTypeMap.set(key, entry);
      };
      for (const p of state.all) {
        cats.add(p.category || "Unknown");
        statuses.add(effectiveStatus(p));
        const code = propertyUseCodeKey(p);
        const entry = useCodeMap.get(code) || { code, desc: p.use_desc || "", count: 0 };
        entry.count += 1;
        if (!entry.desc && p.use_desc) entry.desc = p.use_desc;
        useCodeMap.set(code, entry);

        const liens = Array.isArray(p.liens) ? p.liens : [];
        const seen = new Set();
        for (const l of liens) {
          if (!l || !l.lien_type || seen.has(l.lien_type)) continue;
          seen.add(l.lien_type);
          addLienType(l.lien_type, l.lien_type_label || l.lien_type);
        }
        const scTypes = Array.isArray(p.special_condition_types) ? p.special_condition_types : [];
        const seenSc = new Set();
        for (const t of scTypes) {
          if (!t || seenSc.has(t)) continue;
          seenSc.add(t);
          addLienType(`sc:${t}`, `Special: ${t.replace(/_/g, " ")}`);
        }
      }
      state.categories = [...cats].sort();
      state.statuses = [...statuses].sort();
      state.useCodes = [...useCodeMap.values()].sort((a, b) => {
        // Sort by count desc, then by code asc.
        if (b.count !== a.count) return b.count - a.count;
        return String(a.code).localeCompare(String(b.code), undefined, { numeric: true });
      });
      // Keep the curated lien-type order stable: most-important first, then
      // by frequency. "IRS, weed abatement, brush clearance, special
      // conditions" are the user-requested flags and float to the top.
      const PRIORITY = { irs: 0, weed: 1, brush: 2, state_tax: 3 };
      state.lienTypes = [...lienTypeMap.values()].sort((a, b) => {
        const ap = PRIORITY[a.key] ?? 10;
        const bp = PRIORITY[b.key] ?? 10;
        if (ap !== bp) return ap - bp;
        if (a.key.startsWith("sc:") !== b.key.startsWith("sc:")) {
          return a.key.startsWith("sc:") ? 1 : -1; // liens before SC
        }
        if (b.count !== a.count) return b.count - a.count;
        return a.label.localeCompare(b.label);
      });
      renderChips("category-filters", state.categories, state.activeCategories);
      renderChips("status-filters", state.statuses, state.activeStatuses);
      renderUseCodeChips();
      renderLienTypeChips();

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
