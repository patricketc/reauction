"""Look up a parcel on the LA County Assessor portal.

Two strategies, tried in order:

1. **ArcGIS FeatureServer / MapServer query.** LA County publishes parcels
   through several ArcGIS REST services. We try a short list of known-good
   public endpoints and query each with a few ``where``-clause variants,
   because the AIN field is named differently across services (``AIN`` is
   text on some, numeric on others). Any endpoint that returns a feature
   wins -- we use its geometry for the map marker and its attributes for
   the parcel detail columns.

2. **HTML parcel-detail scrape.** If every ArcGIS endpoint is unreachable
   or returns no feature for the AIN, we fall back to fetching
   ``portal.assessor.lacounty.gov/parceldetail/<AIN>`` and extracting what
   we can from the HTML. This is brittle but a decent backup.

If LA County moves things again and every parcel comes back empty, the
warning log from ``AssessorEnricher.lookup`` is the first place to check --
it will tell you which endpoints were tried and whether any returned
features.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from ._http import JsonCache, RateLimiter, make_session

log = logging.getLogger(__name__)

# Known-good public parcel layers. We try them in order; first hit wins.
# Update this list if LA County reorganizes their GIS portal.
ARCGIS_PARCEL_URLS: tuple[str, ...] = (
    # Assessor's own parcel service (most authoritative for AIN lookups)
    "https://public.gis.lacounty.gov/public/rest/services/LACounty_Dynamic/"
    "Parcel/MapServer/0/query",
    # Cached Assessor parcel layer (used by several county viewers)
    "https://public.gis.lacounty.gov/public/rest/services/LACounty_Cache/"
    "LACounty_Parcel/MapServer/0/query",
    # Assessor FeatureServer (newer)
    "https://maps.assessor.lacounty.gov/GVSWebAPI/rest/services/"
    "Parcel/FeatureServer/0/query",
)

PARCEL_DETAIL_URL = "https://portal.assessor.lacounty.gov/parceldetail/{ain}"


def _where_clauses(ain: str) -> tuple[str, ...]:
    """Every ``where`` variant we know LA County's parcel services use.

    Some layers store AIN as text (``AIN='2006010026'``), others as numeric
    (``AIN=2006010026``), and a few use the sortable/numeric field
    ``AIN_SORT``. Trying all of them costs nothing extra on a cache hit.
    """
    return (
        f"AIN='{ain}'",
        f"AIN={ain}",
        f"AIN_SORT={int(ain)}",
    )


def _polygon_area_sqft(rings: list[list[list[float]]]) -> float | None:
    """Approximate polygon area in square feet from a ring in WGS84 degrees.

    Uses the shoelace formula on an equirectangular projection centered on
    the polygon, converting degrees to feet with a latitude-aware scale. The
    result is accurate to within ~1% at LA's latitude for typical parcel
    sizes -- good enough to backfill a missing ``LandSqFt`` attribute.
    """
    import math

    ring = rings[0]
    if not ring or len(ring) < 3:
        return None
    lat_mean = sum(pt[1] for pt in ring) / len(ring)
    # 1 degree of latitude ~= 364,000 ft; longitude varies with cos(lat).
    ft_per_deg_lat = 364000.0
    ft_per_deg_lng = 364000.0 * math.cos(math.radians(lat_mean))
    acc = 0.0
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i][0] * ft_per_deg_lng, ring[i][1] * ft_per_deg_lat
        x2, y2 = ring[(i + 1) % n][0] * ft_per_deg_lng, ring[(i + 1) % n][1] * ft_per_deg_lat
        acc += x1 * y2 - x2 * y1
    area = abs(acc) / 2.0
    return area if area >= 1 else None


def _parse_arcgis_feature(feat: dict[str, Any]) -> dict[str, Any]:
    attrs = feat.get("attributes") or {}
    geom = feat.get("geometry") or {}

    lat = lng = None
    rings = geom.get("rings")
    if rings and rings[0]:
        xs = [pt[0] for pt in rings[0]]
        ys = [pt[1] for pt in rings[0]]
        lng = sum(xs) / len(xs)
        lat = sum(ys) / len(ys)
    elif "x" in geom and "y" in geom:
        lng, lat = geom["x"], geom["y"]

    def _pick(*keys: str) -> Any:
        """Return the first non-empty attribute value among ``keys``.

        Field names vary across LA County's parcel layers, so we list every
        spelling we've seen. ``0`` is rejected because most of these layers
        use ``0`` as "no data" for numeric fields (bedrooms, year built, etc.).
        """
        for k in keys:
            v = attrs.get(k)
            if v not in (None, "", 0):
                return v
        return None

    result = {
        "source": "arcgis",
        "use_code": _pick(
            "UseCode", "USECODE", "UseType", "SpecificUseType", "GeneralUseType"
        ),
        "use_desc": _pick(
            "UseDescription", "USEDESCRIPTION", "UseTypeDesc",
            "SpecificUseDescription", "GeneralUseDescription",
        ),
        "impr_desc": _pick(
            "ImprovementDescription", "IMP_DESC",
            "BuildingClass", "BldgClass", "BuildingDescription",
        ),
        "situs": _pick(
            "SitusFullAddress", "SITUS_ADDR", "SitusAddress",
            "SitusStreetAddress", "SitusHouseNoStreet",
        ),
        "situs_city": _pick("SitusCity", "SITUS_CITY", "SitusCityState"),
        "situs_zip": _pick("SitusZIP", "SITUS_ZIP", "SitusZip", "SitusZipCode"),
        "year_built": _pick(
            "YearBuilt", "YEARBUILT",
            "EffectiveYearBuilt", "EFF_YEAR_BUILT", "YearBuiltEffective",
        ),
        "bedrooms": _pick(
            "Bedrooms", "BEDROOMS", "Bedrooms1", "BedroomCount", "BedCount"
        ),
        "bathrooms": _pick(
            "Bathrooms", "BATHROOMS", "Bathrooms1",
            "BathroomCount", "BathCount", "Baths",
        ),
        "units": _pick(
            "Units", "UNITS", "UnitsCount", "NumUnits", "NumberOfUnits"
        ),
        # sqft_lot: try every dedicated land-area column we've seen LA County
        # expose across its various parcel layers. We intentionally do NOT
        # fall back to ``Shape__Area`` -- when we request ``outSR=4326`` the
        # server returns that value in decimal degrees squared, which would
        # silently produce garbage numbers. If no dedicated field is present,
        # ``_compute_lot_sqft_from_geometry`` backfills from the polygon.
        "sqft_lot": _pick(
            "LandSqFt", "LandSqft", "LAND_SQFT", "LotSqFt", "LotSqft",
            "SQFT_LOT", "SqFtLot", "LandArea", "LandAreaSqFt", "LAND_AREA",
            "ParcelAreaSqFt", "ParcelSqFt", "Roll_LandSqFt", "Roll_LandSqft",
            "NetSqFt", "GrossSqFt",
        ),
        # sqft_building: SQFTmain is the LA County Assessor canonical field
        # for the main structure's square footage. Additional variants cover
        # the older FeatureServer schema and the cached mirror.
        "sqft_building": _pick(
            "SQFTmain", "SqFtMain", "SQFT_MAIN", "SQFT_Main",
            "Bldg1SqFt", "BldgSqFt", "BLDG_SQFT", "SQFTBldg", "SQFTBuilding",
            "BuildingSqFt", "Building_SqFt", "MainSqFt", "StructureSqFt",
            "SFA", "ImpSqFt", "Improvement_SqFt",
        ),
        "assessed_land": _pick("Roll_LandValue", "LandValue", "LAND_VALUE"),
        "assessed_improvements": _pick(
            "Roll_ImpValue", "ImprovementValue", "IMP_VALUE"
        ),
        "assessed_total": _pick(
            "Roll_TotalValue", "TotalValue", "NetTaxableValue",
            "NetTaxValue", "TotalLandImpAV",
        ),
        "zoning": _pick("Zoning", "ZONING", "ZoneCode"),
        "tax_rate_area": _pick("TaxRateArea", "TRA", "TaxRateCity"),
        "last_sale_date": _pick(
            "SaleDate", "LastSaleDate", "SALE_DATE", "RecordingDate"
        ),
        "last_sale_price": _pick(
            "SaleAmount", "SalePrice", "LastSalePrice", "SALE_PRICE"
        ),
        "lat": lat,
        "lng": lng,
    }

    # Geometry-derived fallback for lot sq ft. Only used when the attribute
    # columns come back empty -- a real ``LandSqFt`` value is preferred.
    if result["sqft_lot"] in (None, 0) and rings and rings[0]:
        geom_sqft = _polygon_area_sqft(rings)
        if geom_sqft:
            result["sqft_lot"] = round(geom_sqft)

    return result


def _arcgis_query(session, url: str, where: str) -> dict[str, Any] | None:
    params = {
        "where": where,
        "outFields": "*",
        "f": "json",
        "returnGeometry": "true",
        "outSR": "4326",
    }
    try:
        resp = session.get(url, params=params, timeout=20)
    except Exception as e:  # noqa: BLE001 -- fall back on any failure
        log.debug("arcgis request failed (%s %r): %s", url, where, e)
        return None
    if not resp.ok:
        log.debug("arcgis non-ok %s for %s %r", resp.status_code, url, where)
        return None
    try:
        data = resp.json()
    except ValueError:
        return None
    if data.get("error"):
        log.debug("arcgis error response (%s %r): %s", url, where, data["error"])
        return None
    features = data.get("features") or []
    if not features:
        return None
    return _parse_arcgis_feature(features[0])


def _arcgis_lookup(session, ain: str) -> dict[str, Any] | None:
    attempts: list[str] = []
    for url in ARCGIS_PARCEL_URLS:
        for where in _where_clauses(ain):
            result = _arcgis_query(session, url, where)
            if result is not None:
                log.debug("arcgis hit for %s via %s (%s)", ain, url, where)
                return result
            attempts.append(f"{url} [{where}]")
    log.warning(
        "no arcgis feature found for AIN %s after %d attempts", ain, len(attempts)
    )
    return None


_USE_RE = re.compile(r"use\s*code[^0-9A-Z]*([0-9A-Z]{3,4})", re.I)
_DESC_RE = re.compile(r"use\s*description[^:]*:\s*([^\n<]+)", re.I)

# Tax-status indicators on the Assessor parcel detail page. Order matters:
# more-specific phrases come first so they win over generic words like
# "delinquent". Values are normalized labels, not direct page strings, so
# the UI has a small fixed vocabulary to switch on.
#
# The negated "no longer subject to the power to sell" rule must come before
# the plain "subject to the power to sell" rule so a redeemed parcel doesn't
# get matched as still-in-default.
_TAX_STATUS_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bno longer subject to (?:the )?power to sell\b", re.I), "Redeemed"),
    (re.compile(r"\b(?:has been )?redeemed\b", re.I), "Redeemed"),
    (re.compile(r"\bpaid in full\b", re.I), "Paid in Full"),
    (re.compile(r"\btax[-\s]?defaulted\b", re.I), "Tax Defaulted"),
    (re.compile(r"\bin\s+(?:tax[-\s]?)?default\b", re.I), "Tax Defaulted"),
    (re.compile(r"\bsubject to (?:the )?power to sell\b", re.I), "Subject to Power to Sell"),
    (re.compile(r"\bprior[-\s]?year\s+(?:taxes?\s+)?(?:are\s+)?delinquent\b", re.I), "Delinquent"),
    (re.compile(r"\bdelinquent\b", re.I), "Delinquent"),
    (re.compile(r"\btaxes?\s+(?:are\s+)?(?:paid\s+)?current\b", re.I), "Current"),
    (re.compile(r"\bcurrent\s+(?:year\s+)?taxes?\s+paid\b", re.I), "Current"),
    (re.compile(r"\bstatus\s*[:\-]\s*current\b", re.I), "Current"),
    (re.compile(r"\bstatus\s*[:\-]\s*(?:tax[-\s]?)?defaulted\b", re.I), "Tax Defaulted"),
)


def _scrape_tax_status(session, ain: str) -> str | None:
    """Return a normalized tax-status label from the Assessor portal, or None.

    Walks the Assessor parcel detail HTML with two strategies:

    1. Targeted: look for any DOM node whose label text mentions "Tax"/"Status"
       and read the value from the same section. The Assessor portal renders
       status in a labelled field, not free-floating prose, so this catches
       cases where the raw page text doesn't read like a sentence.
    2. Sentence-level: scan the full page text against ``_TAX_STATUS_RULES``.

    We require the AIN to appear in the page body first -- a generic error or
    landing page may still contain boilerplate like "tax-defaulted properties"
    that would otherwise match.
    """
    url = PARCEL_DETAIL_URL.format(ain=ain)
    try:
        resp = session.get(url, timeout=20)
    except Exception as e:  # noqa: BLE001
        log.debug("tax-status fetch failed for %s: %s", ain, e)
        return None
    if not resp.ok or not resp.text:
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    text = soup.get_text(" ", strip=True)

    ain_variants = (ain,) if len(ain) != 10 else (
        ain, f"{ain[:4]}-{ain[4:7]}-{ain[7:]}", f"{ain[:4]} {ain[4:7]} {ain[7:]}"
    )
    if not any(v in text for v in ain_variants):
        return None

    # Strategy 1: labelled field. Walk nodes whose text is a "Tax Status"-ish
    # label and read the adjacent value. The portal uses several layouts over
    # time (dt/dd, th/td, label+div), so check the siblings and the parent's
    # subsequent text.
    label_pat = re.compile(r"tax\s*(?:bill\s*)?status|tax\s*default|payment\s*status", re.I)
    for node in soup.find_all(string=label_pat):
        parent = node.parent
        if not parent:
            continue
        candidates: list[str] = []
        for sib in parent.find_all_next(limit=4):
            sib_text = sib.get_text(" ", strip=True)
            if sib_text and sib_text.lower() != node.strip().lower():
                candidates.append(sib_text)
        for cand in candidates:
            for pattern, label in _TAX_STATUS_RULES:
                if pattern.search(cand):
                    return label

    # Strategy 2: whole-page sentence scan.
    for pattern, label in _TAX_STATUS_RULES:
        if pattern.search(text):
            return label
    return None


def _html_lookup(session, ain: str) -> dict[str, Any] | None:
    url = PARCEL_DETAIL_URL.format(ain=ain)
    try:
        resp = session.get(url, timeout=20)
    except Exception as e:  # noqa: BLE001
        log.debug("html request failed for %s: %s", ain, e)
        return None
    if not resp.ok:
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    text = soup.get_text(" ", strip=True)

    use_code = None
    m = _USE_RE.search(text)
    if m:
        use_code = m.group(1)

    use_desc = None
    m = _DESC_RE.search(text)
    if m:
        use_desc = m.group(1).strip()

    # The page usually has a "Situs Address" block.
    situs = None
    for label in ("Situs Address", "Property Address"):
        tag = soup.find(string=re.compile(label, re.I))
        if tag and tag.parent:
            sibling = tag.parent.find_next(string=True)
            if sibling and sibling.strip():
                situs = sibling.strip()
                break

    if not any([use_code, use_desc, situs]):
        return None

    return {
        "source": "html",
        "use_code": use_code,
        "use_desc": use_desc,
        "impr_desc": None,
        "situs": situs,
        "situs_city": None,
        "situs_zip": None,
        "year_built": None,
        "bedrooms": None,
        "bathrooms": None,
        "units": None,
        "sqft_lot": None,
        "sqft_building": None,
        "assessed_land": None,
        "assessed_improvements": None,
        "assessed_total": None,
        "zoning": None,
        "tax_rate_area": None,
        "last_sale_date": None,
        "last_sale_price": None,
        "lat": None,
        "lng": None,
    }


class AssessorEnricher:
    def __init__(self, cache_dir: str, rate: RateLimiter) -> None:
        self.cache = JsonCache(cache_dir)
        self.rate = rate
        self.session = make_session()

    def lookup(self, ain: str) -> dict[str, Any] | None:
        # Structured data path: ArcGIS first, HTML fallback. Cached as a unit.
        cached = self.cache.get(f"assessor_v3_{ain}")
        if cached is not None and cached.get("source") != "none":
            result: dict[str, Any] | None = cached
        else:
            self.rate.wait()
            result = _arcgis_lookup(self.session, ain)
            if result is None:
                self.rate.wait()
                result = _html_lookup(self.session, ain)
            # Cache even the "nothing found" result so we don't hammer on
            # retry within a single run.
            self.cache.set(f"assessor_v3_{ain}", result or {"source": "none"})

        # Tax status: always from the Assessor parcel page, separately cached
        # so it can be invalidated without dropping the structured data, and
        # so a successful ArcGIS lookup doesn't skip it.
        tax_status = self._lookup_tax_status(ain)

        if result is None:
            result = {}
        if tax_status:
            result["tax_status"] = tax_status
        return result or None

    def _lookup_tax_status(self, ain: str) -> str | None:
        key = f"tax_status_v2_{ain}"
        cached = self.cache.get(key)
        if cached is not None:
            # Re-try a miss on the next run in case the portal was flaky,
            # rather than locking in ``None`` forever.
            if cached.get("tax_status") is not None:
                return cached["tax_status"]
            return None
        self.rate.wait()
        status = _scrape_tax_status(self.session, ain)
        self.cache.set(key, {"tax_status": status})
        return status
