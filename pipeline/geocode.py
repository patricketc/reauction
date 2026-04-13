"""Geocode an address to (lat, lng) via OpenStreetMap Nominatim.

We only call Nominatim as a fallback -- the Assessor ArcGIS endpoint usually
returns a parcel geometry we can use. When we do call it, we respect the
Nominatim usage policy:

  - 1 req/sec maximum
  - identifying User-Agent
  - cache results on disk

See https://operations.osmfoundation.org/policies/nominatim/
"""

from __future__ import annotations

import logging
from typing import Any

from ._http import JsonCache, RateLimiter, make_session

log = logging.getLogger(__name__)

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


class Geocoder:
    def __init__(self, cache_dir: str, rate: RateLimiter) -> None:
        self.cache = JsonCache(cache_dir)
        self.rate = rate
        self.session = make_session()

    def geocode(self, address: str) -> dict[str, Any] | None:
        if not address:
            return None
        key = f"geocode_{address.lower().strip().replace(' ', '_')}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached or None

        self.rate.wait()
        params = {
            "q": f"{address}, Los Angeles County, CA",
            "format": "json",
            "limit": 1,
            "countrycodes": "us",
        }
        try:
            resp = self.session.get(NOMINATIM_URL, params=params, timeout=20)
        except Exception as e:  # noqa: BLE001
            log.debug("nominatim failed for %r: %s", address, e)
            self.cache.set(key, {})
            return None

        if not resp.ok:
            self.cache.set(key, {})
            return None

        try:
            data = resp.json()
        except ValueError:
            self.cache.set(key, {})
            return None

        if not data:
            self.cache.set(key, {})
            return None

        hit = data[0]
        result = {
            "lat": float(hit["lat"]),
            "lng": float(hit["lon"]),
            "display_name": hit.get("display_name"),
        }
        self.cache.set(key, result)
        return result
