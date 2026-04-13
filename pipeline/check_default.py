"""Check whether a parcel is still in tax default with the LA County TTC.

The TTC exposes a property-tax portal ("vcheck") that returns current-year and
prior-year tax status by AIN. There's no documented public API, so we scrape
the summary page and look for signals that the property is (a) still delinquent
on prior-year taxes and (b) still scheduled for sale.

Because redemption can happen at any moment up to the auction, results from
this module are only as fresh as the last cache entry. Re-run close to the
auction for up-to-date status.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from ._http import JsonCache, RateLimiter, make_session

log = logging.getLogger(__name__)

# Best-known endpoints. If one becomes unreachable, check the TTC site for a
# current URL and update here.
VCHECK_URL = "https://vcheck.ttc.lacounty.gov/"
PROPERTY_PORTAL_URL = "https://ttc.lacounty.gov/property-tax-portal-lookup/"

_REDEEMED_RE = re.compile(r"\b(redeemed|paid in full|no longer subject)\b", re.I)
_DEFAULT_RE = re.compile(r"\b(tax[- ]?defaulted|subject to power to sell|auction)\b", re.I)


def _classify(html: str) -> str:
    """Return one of: 'in_default', 'redeemed', 'unknown'."""
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    if _REDEEMED_RE.search(text):
        return "redeemed"
    if _DEFAULT_RE.search(text):
        return "in_default"
    return "unknown"


class DefaultChecker:
    def __init__(self, cache_dir: str, rate: RateLimiter) -> None:
        self.cache = JsonCache(cache_dir)
        self.rate = rate
        self.session = make_session()

    def _fetch(self, ain: str) -> str | None:
        # vcheck accepts AIN as a form POST. We try a simple GET with an ain
        # parameter first, then fall back to the portal lookup page.
        for url in (f"{VCHECK_URL}?ain={ain}", f"{PROPERTY_PORTAL_URL}?ain={ain}"):
            self.rate.wait()
            try:
                resp = self.session.get(url, timeout=20)
            except Exception as e:  # noqa: BLE001
                log.debug("default fetch failed for %s (%s): %s", ain, url, e)
                continue
            if resp.ok and resp.text:
                return resp.text
        return None

    def check(self, ain: str) -> dict[str, Any]:
        key = f"default_{ain}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached

        html = self._fetch(ain)
        status = _classify(html) if html else "unknown"
        result = {"status": status, "source": "vcheck"}
        self.cache.set(key, result)
        return result
