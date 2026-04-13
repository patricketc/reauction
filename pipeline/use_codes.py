"""Map LA County Assessor use codes to broad categories for filtering.

The Assessor uses a four-character code. A trailing "V" typically marks the
parcel as vacant. The leading digits roughly indicate:

  0  residential / vacant residential
  1  commercial
  2  industrial
  3  recreational
  4  institutional
  5  irrigated agriculture
  6  rural / dry agriculture
  7  government / public utilities
  8  miscellaneous
  9  mineral / resources

This mapping is intentionally coarse. It's meant to give useful UI filters
("commercial", "residential", "vacant lot", ...) rather than reproduce every
distinction in the Assessor's code list. If you care about a finer split,
expand :func:`categorize` below.
"""

from __future__ import annotations

CATEGORIES = [
    "Vacant Lot",
    "Residential - Single Family",
    "Residential - Multi-Family",
    "Residential - Condo",
    "Commercial",
    "Industrial",
    "Agricultural",
    "Institutional",
    "Recreational",
    "Government / Utility",
    "Mineral / Resource",
    "Other",
    "Unknown",
]


def categorize(use_code: str | None, use_desc: str | None = None) -> str:
    """Return a broad category for this parcel.

    ``use_code`` is the raw Assessor use code (e.g. "0100", "010V", "8800").
    ``use_desc`` is the human text returned alongside it ("Single Family
    Residence", "Vacant Land", ...) and is used as a tiebreaker when the code
    is missing or ambiguous.
    """
    code = (use_code or "").strip().upper()
    desc = (use_desc or "").strip().lower()

    # Description-based shortcuts (helps when code is missing or non-standard).
    if desc:
        if "vacant" in desc:
            return "Vacant Lot"
        if "condo" in desc:
            return "Residential - Condo"

    if not code:
        return "Unknown"

    # Trailing V -> vacant of whatever leading-digit type. Vacant residential
    # and vacant commercial all map to "Vacant Lot" for filter simplicity.
    if code.endswith("V"):
        return "Vacant Lot"

    lead = code[:1]
    two = code[:2]

    if lead == "0":
        # Residential family
        if two in ("01",):
            return "Residential - Single Family"
        if two in ("02", "03"):
            return "Residential - Multi-Family"
        if two in ("04",):
            # 0400 series = 5+ unit multi-family
            return "Residential - Multi-Family"
        if two in ("05",):
            return "Residential - Condo"
        return "Residential - Single Family"
    if lead == "1":
        return "Commercial"
    if lead == "2":
        return "Industrial"
    if lead == "3":
        return "Recreational"
    if lead == "4":
        return "Institutional"
    if lead in ("5", "6"):
        return "Agricultural"
    if lead == "7":
        return "Government / Utility"
    if lead == "8":
        return "Other"
    if lead == "9":
        return "Mineral / Resource"
    return "Other"
