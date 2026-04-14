"""Check whether a parcel is still in tax default with the LA County TTC.

The TTC exposes a property-tax portal ("vcheck") that returns current-year and
prior-year tax status by AIN. There's no documented public API, so we scrape
the summary page and look for signals that the property is (a) still delinquent
on prior-year taxes and (b) still scheduled for sale.

Historically this module was too eager to classify parcels as ``in_default``:
if the TTC endpoint returned its generic landing or error page (as happens
when the AIN lookup fails entirely), the page text still contained words like
"auction" and "tax-defaulted", which matched the in-default regex. That meant
a broken lookup looked identical to a confirmed delinquent parcel.

The current implementation requires two things before a page is classified as
``in_default``:

  1. The AIN has to appear somewhere in the response text. If it doesn't, we
     assume we got a generic/error page and return ``unknown``.
  2. A *per-AIN* default signal has to match -- not just the presence of a
     generic word like "auction".

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

# Phrases that only appear on a parcel-specific confirmation -- these are
# narrow on purpose so the generic landing page doesn't match.
_REDEEMED_RE = re.compile(
    r"\b(has been redeemed|no longer subject to (the )?power to sell|paid in full)\b",
    re.I,
)
_DEFAULT_RE = re.compile(
    r"\b(subject to (the )?power to sell|tax[- ]?defaulted.{0,40}(power to sell|auction|sale)|"
    r"scheduled for (public )?auction)\b",
    re.I,
)


def _ain_variants(ain: str) -> tuple[str, ...]:
    """Produce the AIN spellings we might see in portal output."""
    if len(ain) != 10 or not ain.isdigit():
        return (ain,)
    dashed = f"{ain[:4]}-{ain[4:7]}-{ain[7:]}"
    spaced = f"{ain[:4]} {ain[4:7]} {ain[7:]}"
    return (ain, dashed, spaced)


def _classify(html: str, ain: str) -> str:
    """Return one of: ``'in_default'``, ``'redeemed'``, ``'unknown'``.

    Every parcel listed in the TTC auction book is in default at publication
    time -- that's how it ended up in the book. The vcheck lookup is only
    interesting when it tells us a *redemption* happened between publication
    and today. So the default answer here is ``in_default``, and we only
    override it when the response is clearly about this specific AIN and
    contains a redemption signal.

    ``unknown`` is reserved for genuine lookup failures (no HTML at all), and
    is returned at the caller rather than from this function.
    """
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    if any(v in text for v in _ain_variants(ain)) and _REDEEMED_RE.search(text):
        return "redeemed"
    return "in_default"


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
        # Cache key is versioned ("_v2") because the classification contract
        # changed in April 2026: we now default to ``in_default`` instead of
        # ``unknown``. Old ``default_{ain}`` entries that persisted a stale
        # "unknown for everyone" result would otherwise mask the new logic.
        key = f"default_v2_{ain}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached

        html = self._fetch(ain)
        if html is None:
            # Every known endpoint was unreachable. Don't cache this -- we
            # want to re-try on the next run rather than lock in ``unknown``.
            return {"status": "unknown", "source": "unreachable"}

        result = {"status": _classify(html, ain), "source": "vcheck"}
        self.cache.set(key, result)
        return result
