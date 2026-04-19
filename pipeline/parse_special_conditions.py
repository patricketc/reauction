"""Parse the TTC 'Special Conditions' auction flyer into per-AIN annotations.

The flyer lists parcels that come with unusual sale conditions -- mobile
homes (improvements not conveyed in the sale), cemeteries, water wells,
easements, flood zones, submerged land, etc. Each page of the flyer groups
AINs under a section header that names the condition.

We don't know the flyer's exact layout from year to year, so this parser
is deliberately forgiving: it walks the text line by line, keeps track of
the most recent line that "looks like" a section header, and attaches that
header as a condition to any AIN found beneath it.

If the flyer URL is unreachable or the layout has changed enough to yield
zero AIN matches, the parser returns an empty dict and logs a warning. The
rest of the pipeline treats special conditions as *optional* enrichment --
a zero-match parse just means no parcels are flagged.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from pathlib import Path

import pdfplumber

log = logging.getLogger(__name__)

AIN_RE = re.compile(r"\b(\d{4})[-\s]?(\d{3})[-\s]?(\d{3})\b")

# Map free-text section headers on the Special Conditions flyer to stable
# short keys. This lets the UI filter by condition type even though the
# TTC rewrites the human-readable labels year to year. Patterns are
# declaration-order -- the first match wins -- so overlapping concepts list
# the more specific phrase first.
SPECIAL_CONDITION_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"brush\s+clearance", re.I),       "brush_clearance"),
    (re.compile(r"weed\s+abatement", re.I),        "weed_abatement"),
    (re.compile(r"mobile\s+home", re.I),           "mobile_home"),
    (re.compile(r"cemetery|burial", re.I),         "cemetery"),
    (re.compile(r"easement", re.I),                "easement"),
    (re.compile(r"flood", re.I),                   "flood_zone"),
    (re.compile(r"submerged|underwater", re.I),    "submerged"),
    (re.compile(r"well\b", re.I),                  "water_well"),
    (re.compile(r"access", re.I),                  "access_restricted"),
    (re.compile(r"condominium|condo", re.I),       "condo"),
    (re.compile(r"improvements?\s+not\s+conveyed", re.I), "improvements_not_conveyed"),
)


def classify_special_condition(section: str | None) -> str:
    """Map a free-text flyer section header to a stable condition key."""
    text = section or ""
    for pattern, key in SPECIAL_CONDITION_RULES:
        if pattern.search(text):
            return key
    return "other"

# A "section header" is a short, mostly-alphabetic line without an AIN,
# without dollar amounts, and without other long numeric strings. These
# thresholds are permissive on purpose -- real headers like "MOBILE HOMES"
# or "Parcels Subject to Cemetery Use" should easily clear them.
_HEADER_MAX_LEN = 140
_HEADER_MIN_ALPHA_RATIO = 0.55


def _looks_like_header(line: str) -> bool:
    if not line or len(line) > _HEADER_MAX_LEN:
        return False
    if AIN_RE.search(line) or "$" in line:
        return False
    # Long digit runs (5+) suggest data (case numbers, years, etc.).
    if re.search(r"\d{5,}", line):
        return False
    alpha = sum(c.isalpha() for c in line)
    return alpha / max(len(line), 1) >= _HEADER_MIN_ALPHA_RATIO


def parse_special_conditions(pdf_path: str | Path) -> dict[str, list[str]]:
    """Return ``{ain: [condition labels]}`` from the flyer."""
    result: dict[str, list[str]] = defaultdict(list)
    current_section: str | None = None

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw in text.splitlines():
                line = raw.strip()
                if not line:
                    continue
                ain_match = AIN_RE.search(line)
                if not ain_match:
                    if _looks_like_header(line):
                        current_section = line
                    continue
                ain = "".join(ain_match.group(i) for i in (1, 2, 3))
                label = current_section or "Special condition"
                if label not in result[ain]:
                    result[ain].append(label)

    if not result:
        log.warning("special-conditions parser found no AIN matches in %s", pdf_path)
    else:
        log.info("parsed special conditions for %d AINs", len(result))
    return dict(result)


if __name__ == "__main__":
    import json
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "data/special-conditions.pdf"
    out = parse_special_conditions(path)
    preview = dict(list(out.items())[:20])
    print(json.dumps(preview, indent=2))
    print(f"\n{len(out)} AINs flagged with special conditions")
