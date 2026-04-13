"""Pipeline orchestrator.

Runs:

    PDF -> parcel rows -> assessor enrichment -> default status check ->
    geocoding (only if assessor didn't already give us coordinates) ->
    properties.json

Everything downstream of PDF parsing is cached per-AIN under data/cache/, so
you can re-run cheaply.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from .check_default import DefaultChecker
from .enrich_assessor import AssessorEnricher
from .geocode import Geocoder
from .parse_pdf import parse_pdf_to_list
from .use_codes import categorize
from ._http import RateLimiter

log = logging.getLogger("pipeline")


def _format_address(enriched: dict | None, fallback: str | None) -> str | None:
    if not enriched:
        return fallback
    parts = [enriched.get("situs")]
    if enriched.get("situs_city"):
        parts.append(enriched["situs_city"])
    if enriched.get("situs_zip"):
        parts.append(str(enriched["situs_zip"]))
    full = ", ".join(p for p in parts if p)
    return full or fallback


def build(
    pdf_path: str,
    out_path: str,
    cache_dir: str,
    rate: float,
    limit: int | None,
    skip_default: bool,
    skip_geocode: bool,
) -> None:
    limiter = RateLimiter(min_interval=rate)
    assessor = AssessorEnricher(cache_dir, limiter)
    defaulter = DefaultChecker(cache_dir, limiter)
    geocoder = Geocoder(cache_dir, limiter)

    log.info("Parsing PDF %s", pdf_path)
    rows = parse_pdf_to_list(pdf_path)
    log.info("Parsed %d parcel rows", len(rows))
    if limit:
        rows = rows[:limit]
        log.info("Limiting to first %d rows", limit)

    out: list[dict] = []
    started = time.monotonic()
    for i, row in enumerate(rows, start=1):
        ain = row["ain"]
        log.info("[%d/%d] AIN %s", i, len(rows), row["ain_formatted"])

        enriched = assessor.lookup(ain) or {}
        default = {"status": "skipped"} if skip_default else defaulter.check(ain)

        lat = enriched.get("lat")
        lng = enriched.get("lng")
        if (lat is None or lng is None) and not skip_geocode:
            address_for_geo = _format_address(enriched, row.get("situs_hint"))
            if address_for_geo:
                geo = geocoder.geocode(address_for_geo)
                if geo:
                    lat = geo["lat"]
                    lng = geo["lng"]

        category = categorize(enriched.get("use_code"), enriched.get("use_desc"))

        out.append({
            "item_no": row.get("item_no"),
            "ain": ain,
            "ain_formatted": row.get("ain_formatted"),
            "min_bid": row.get("min_bid"),
            "situs": _format_address(enriched, row.get("situs_hint")),
            "use_code": enriched.get("use_code"),
            "use_desc": enriched.get("use_desc"),
            "category": category,
            "year_built": enriched.get("year_built"),
            "sqft_lot": enriched.get("sqft_lot"),
            "sqft_building": enriched.get("sqft_building"),
            "assessed_land": enriched.get("assessed_land"),
            "assessed_improvements": enriched.get("assessed_improvements"),
            "zoning": enriched.get("zoning"),
            "default_status": default.get("status"),
            "lat": lat,
            "lng": lng,
            "assessor_url": f"https://portal.assessor.lacounty.gov/parceldetail/{ain}",
            "ttc_url": f"https://vcheck.ttc.lacounty.gov/?ain={ain}",
        })

        # Periodic flush so a crash doesn't lose everything.
        if i % 20 == 0:
            Path(out_path).write_text(json.dumps({"generated_at": int(time.time()), "properties": out}, indent=2))

    Path(out_path).write_text(json.dumps({"generated_at": int(time.time()), "properties": out}, indent=2))
    log.info("Wrote %d properties to %s in %.1fs", len(out), out_path, time.monotonic() - started)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build auction properties.json from the TTC PDF.")
    p.add_argument("--pdf", default="data/2026A-Auction-Book.pdf")
    p.add_argument("--out", default="web/properties.json")
    p.add_argument("--cache-dir", default="data/cache")
    p.add_argument("--rate", type=float, default=1.0, help="Seconds between requests (default: 1.0)")
    p.add_argument("--limit", type=int, default=None, help="Only process the first N parcels (for testing)")
    p.add_argument("--skip-default", action="store_true", help="Skip the TTC default-status check")
    p.add_argument("--skip-geocode", action="store_true", help="Skip Nominatim geocoding")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    build(
        pdf_path=args.pdf,
        out_path=args.out,
        cache_dir=args.cache_dir,
        rate=args.rate,
        limit=args.limit,
        skip_default=args.skip_default,
        skip_geocode=args.skip_geocode,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
