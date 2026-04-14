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

    return {
        "source": "arcgis",
        "use_code": _pick("UseCode", "USECODE", "UseType"),
        "use_desc": _pick("UseDescription", "USEDESCRIPTION", "UseTypeDesc"),
        "impr_desc": _pick(
            "ImprovementDescription", "IMP_DESC", "BuildingClass", "BldgClass"
        ),
        "situs": _pick("SitusFullAddress", "SITUS_ADDR", "SitusAddress"),
        "situs_city": _pick("SitusCity", "SITUS_CITY"),
        "situs_zip": _pick("SitusZIP", "SITUS_ZIP", "SitusZip"),
        "year_built": _pick(
            "YearBuilt", "YEARBUILT", "EffectiveYearBuilt", "EFF_YEAR_BUILT"
        ),
        "bedrooms": _pick("Bedrooms", "BEDROOMS", "Bedrooms1", "BedroomCount"),
        "bathrooms": _pick(
            "Bathrooms", "BATHROOMS", "Bathrooms1", "BathroomCount"
        ),
        "units": _pick("Units", "UNITS", "UnitsCount", "NumUnits"),
        "sqft_lot": _pick("SQFTmain", "LandSqFt", "LAND_SQFT", "LotSqFt"),
        "sqft_building": _pick(
            "Bldg1SqFt", "BLDG_SQFT", "SQFTBldg", "BuildingSqFt"
        ),
        "assessed_land": _pick("Roll_LandValue", "LandValue", "LAND_VALUE"),
        "assessed_improvements": _pick(
            "Roll_ImpValue", "ImprovementValue", "IMP_VALUE"
        ),
        "assessed_total": _pick(
            "Roll_TotalValue", "TotalValue", "NetTaxableValue", "NetTaxValue"
        ),
        "zoning": _pick("Zoning", "ZONING"),
        "tax_rate_area": _pick("TaxRateArea", "TRA", "TaxRateCity"),
        "last_sale_date": _pick("SaleDate", "LastSaleDate", "SALE_DATE"),
        "last_sale_price": _pick(
            "SaleAmount", "SalePrice", "LastSalePrice", "SALE_PRICE"
        ),
        "lat": lat,
        "lng": lng,
    }


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
        cached = self.cache.get(f"assessor_{ain}")
        if cached is not None:
            # Older cache entries may have stored {"source": "none"} when every
            # endpoint failed. Re-try those so a URL fix takes effect without
            # manually clearing the cache.
            if cached.get("source") != "none":
                return cached

        self.rate.wait()
        result = _arcgis_lookup(self.session, ain)
        if result is None:
            self.rate.wait()
            result = _html_lookup(self.session, ain)

        # Cache even the "nothing found" result so we don't hammer on retry
        # within a single run.
        self.cache.set(f"assessor_{ain}", result or {"source": "none"})
        return result
