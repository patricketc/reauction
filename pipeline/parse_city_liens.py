"""Parse the City of Los Angeles TTC liens PDF.

The PDF lists city-imposed liens on parcels scheduled for the TTC tax sale
-- nuisance abatement, demolition, weed abatement, utility arrears, etc. --
with a dollar amount for each lien. A single parcel can carry multiple
liens, which we sum into a total.

Layout varies year to year. The resilient-enough heuristic used here:

  * Walk lines top-to-bottom, per page.
  * When a line contains an AIN, remember it as the current parcel.
  * When a line contains at least one strict dollar amount (``$#,###.##``
    or ``$#,###``), attribute every amount on that line to the current
    parcel -- along with a best-effort description derived from whatever
    text remains on the line after removing the AIN and money tokens.
  * Reset the current-parcel pointer at page boundaries so a dangling
    AIN from the previous page doesn't accidentally swallow a header
    or footer total on the next.

Strictness on the money regex ($ is required, and either cents or commas
must be present) is what keeps case numbers, zip codes, and year columns
from being miscounted as liens.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import pdfplumber

log = logging.getLogger(__name__)

AIN_RE = re.compile(r"\b(\d{4})[-\s]?(\d{3})[-\s]?(\d{3})\b")

# Strict money tokens: require a ``$`` and either cents or commas. Matches
# ``$1,234.56``, ``$1,234``, ``$123.45`` -- rejects ``1234`` (could be a year
# or case number) and ``1,234`` (ambiguous).
MONEY_RE = re.compile(r"\$\s?([\d,]+\.\d{2})|\$\s?(\d{1,3}(?:,\d{3})+)")


def _money_values(text: str) -> list[float]:
    out: list[float] = []
    for m in MONEY_RE.finditer(text):
        raw = m.group(1) or m.group(2) or ""
        try:
            val = float(raw.replace(",", ""))
        except ValueError:
            continue
        if val >= 1:
            out.append(val)
    return out


def parse_city_liens(pdf_path: str | Path) -> dict[str, dict[str, Any]]:
    """Return ``{ain: {"liens": [{desc, amount}], "total": float}}``."""
    result: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"liens": [], "total": 0.0}
    )

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            current_ain: str | None = None
            for raw in text.splitlines():
                line = raw.strip()
                if not line:
                    continue
                ain_match = AIN_RE.search(line)
                if ain_match:
                    current_ain = "".join(ain_match.group(i) for i in (1, 2, 3))
                amounts = _money_values(line)
                if not amounts or not current_ain:
                    continue

                desc = AIN_RE.sub("", line)
                desc = MONEY_RE.sub("", desc).strip(" .,-$").strip()
                for amount in amounts:
                    result[current_ain]["liens"].append({
                        "desc": (desc[:140] or "Lien"),
                        "amount": amount,
                    })
                    result[current_ain]["total"] += amount

    if not result:
        log.warning("city-liens parser found no AIN+$ matches in %s", pdf_path)
    else:
        total = sum(r["total"] for r in result.values())
        log.info(
            "parsed city liens for %d AINs (grand total $%.2f)",
            len(result),
            total,
        )
    return dict(result)


if __name__ == "__main__":
    import json
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "data/city-liens.pdf"
    out = parse_city_liens(path)
    preview = dict(list(out.items())[:20])
    print(json.dumps(preview, indent=2))
    print(f"\n{len(out)} AINs carry city liens")
