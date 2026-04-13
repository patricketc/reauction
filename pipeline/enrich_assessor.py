"""Look up a parcel on the LA County Assessor portal.

Two strategies, tried in order:

1. **ArcGIS FeatureServer query.** LA County publishes parcels through an
   ArcGIS REST service. Querying by AIN returns structured JSON including
   situs, use code, use description, land sqft, year built, and
   (critically for the map) a geometry we can use to get lat/lng. This is
   the happy path.

2. **HTML parcel-detail scrape.** If the ArcGIS endpoint is unreachable or
   returns no feature for the AIN, we fall back to fetching
   ``portal.assessor.lacounty.gov/parceldetail/<AIN>`` and extracting what
   we can from the HTML. This is brittle but is a decent backup for the
   handful of parcels that fall through.

Both strategies may need adjusting if LA County changes their services. Keep
the fallback -- it will often still work after an ArcGIS URL change.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from ._http import JsonCache, RateLimiter, make_session

log = logging.getLogger(__name__)

# Public parcel feature service. The LA County GIS portal exposes several
# parcel layers; this one is commonly used for AIN lookups. If it 404s, search
# for "LACounty parcel ArcGIS REST" and substitute the current URL.
ARCGIS_PARCEL_URL = (
    "https://public.gis.lacounty.gov/public/rest/services/"
    "LACounty_Dynamic/Parcel/MapServer/0/query"
)

PARCEL_DETAIL_URL = "https://portal.assessor.lacounty.gov/parceldetail/{ain}"


def _arcgis_lookup(session, ain: str) -> dict[str, Any] | None:
    params = {
        "where": f"AIN='{ain}'",
        "outFields": "*",
        "f": "json",
        "returnGeometry": "true",
        "outSR": "4326",
    }
    try:
        resp = session.get(ARCGIS_PARCEL_URL, params=params, timeout=20)
    except Exception as e:  # noqa: BLE001 -- we want to fall back on any failure
        log.debug("arcgis request failed for %s: %s", ain, e)
        return None
    if not resp.ok:
        log.debug("arcgis non-ok (%s) for %s", resp.status_code, ain)
        return None
    try:
        data = resp.json()
    except ValueError:
        return None

    features = data.get("features") or []
    if not features:
        return None
    feat = features[0]
    attrs = feat.get("attributes") or {}
    geom = feat.get("geometry") or {}

    lat = lng = None
    # Polygon centroid fallback: ArcGIS "rings" -> average of first ring
    rings = geom.get("rings")
    if rings and rings[0]:
        xs = [pt[0] for pt in rings[0]]
        ys = [pt[1] for pt in rings[0]]
        lng = sum(xs) / len(xs)
        lat = sum(ys) / len(ys)
    elif "x" in geom and "y" in geom:
        lng, lat = geom["x"], geom["y"]

    # Field names vary across services; grab whichever is populated.
    def _pick(*keys: str) -> Any:
        for k in keys:
            v = attrs.get(k)
            if v not in (None, "", 0):
                return v
        return None

    return {
        "source": "arcgis",
        "use_code": _pick("UseCode", "USECODE", "UseType"),
        "use_desc": _pick("UseDescription", "USEDESCRIPTION", "UseTypeDesc"),
        "situs": _pick("SitusFullAddress", "SITUS_ADDR", "SitusAddress"),
        "situs_city": _pick("SitusCity", "SITUS_CITY"),
        "situs_zip": _pick("SitusZIP", "SITUS_ZIP", "SitusZip"),
        "year_built": _pick("YearBuilt", "YEARBUILT"),
        "sqft_lot": _pick("SQFTmain", "LandSqFt", "LAND_SQFT"),
        "sqft_building": _pick("Bldg1SqFt", "BLDG_SQFT", "SQFTBldg"),
        "assessed_land": _pick("Roll_LandValue", "LandValue"),
        "assessed_improvements": _pick("Roll_ImpValue", "ImprovementValue"),
        "zoning": _pick("Zoning", "ZONING"),
        "lat": lat,
        "lng": lng,
    }


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
        "situs": situs,
        "situs_city": None,
        "situs_zip": None,
        "year_built": None,
        "sqft_lot": None,
        "sqft_building": None,
        "assessed_land": None,
        "assessed_improvements": None,
        "zoning": None,
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
            return cached

        self.rate.wait()
        result = _arcgis_lookup(self.session, ain)
        if result is None:
            self.rate.wait()
            result = _html_lookup(self.session, ain)

        # Cache even the "nothing found" result so we don't hammer on retry.
        self.cache.set(f"assessor_{ain}", result or {"source": "none"})
        return result
