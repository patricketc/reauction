"""Parse the LA County TTC auction book PDF into structured parcel rows.

The TTC has used a fairly stable layout across recent auction books: each parcel
line carries an item number, a 10-digit AIN (often rendered with dashes as
``####-###-###``), and a minimum bid. Some lines also carry a situs address.

We don't rely on visual column detection -- that turns out to be brittle across
page breaks and multi-line entries. Instead we extract text line by line and use
regular expressions anchored on AIN + dollar amount, which has held up well
across the 2022-2025 books.

If the 2026A book changes layout, this is the first file to look at.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterator

import pdfplumber

log = logging.getLogger(__name__)

# 10 digits, optionally separated ####-###-###. LA County AINs are always 10 digits.
AIN_RE = re.compile(r"\b(\d{4})[-\s]?(\d{3})[-\s]?(\d{3})\b")

# Dollar amount, with optional commas and cents.
MONEY_RE = re.compile(r"\$?\s?([\d,]+(?:\.\d{2})?)")

# Leading item number on a row (1-5 digits).
ITEM_RE = re.compile(r"^\s*(\d{1,5})\b")


@dataclass
class ParcelRow:
    item_no: str | None
    ain: str                # normalized: 10 digits, no separators
    ain_formatted: str      # ####-###-###
    min_bid: float | None
    raw_line: str
    situs_hint: str | None  # best-effort address text from the PDF row


def _normalize_ain(match: re.Match) -> tuple[str, str]:
    a, b, c = match.group(1), match.group(2), match.group(3)
    return f"{a}{b}{c}", f"{a}-{b}-{c}"


def _extract_money(text: str) -> float | None:
    # Prefer the last money-looking token on the line -- the minimum bid is
    # typically in the rightmost column.
    matches = MONEY_RE.findall(text)
    if not matches:
        return None
    # Skip any token that's actually part of the AIN (shouldn't happen after
    # we've pulled the AIN out, but defense in depth).
    for tok in reversed(matches):
        try:
            val = float(tok.replace(",", ""))
        except ValueError:
            continue
        # A minimum bid under $100 is almost certainly a false positive.
        if val >= 100:
            return val
    return None


def iter_parcel_rows(pdf_path: str | Path) -> Iterator[ParcelRow]:
    """Yield one ParcelRow per AIN found in the auction book.

    The implementation is deliberately forgiving: we process every non-empty
    line, find any AIN on it, and attach the rightmost dollar amount as the
    minimum bid. Lines without an AIN are ignored (page headers, section
    breaks, rules, etc.).
    """
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_no, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            for raw in text.splitlines():
                line = raw.strip()
                if not line:
                    continue
                m = AIN_RE.search(line)
                if not m:
                    continue

                ain, ain_fmt = _normalize_ain(m)

                # Slice the AIN + item number off the line before searching for
                # money; otherwise the AIN digits confuse money matching.
                without_ain = (line[: m.start()] + line[m.end():]).strip()
                min_bid = _extract_money(without_ain)

                item_match = ITEM_RE.match(line)
                item_no = item_match.group(1) if item_match else None

                # Everything between the item number and the AIN is usually a
                # situs hint (address + city) in books that include it.
                head = line[: m.start()].strip()
                if item_no and head.startswith(item_no):
                    head = head[len(item_no):].strip()
                situs_hint = head or None

                yield ParcelRow(
                    item_no=item_no,
                    ain=ain,
                    ain_formatted=ain_fmt,
                    min_bid=min_bid,
                    raw_line=line,
                    situs_hint=situs_hint,
                )
            log.debug("page %d processed", page_no)


def parse_pdf_to_list(pdf_path: str | Path) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()
    for row in iter_parcel_rows(pdf_path):
        # Same AIN can appear on multiple lines (continuation rows). Keep the
        # first occurrence that has a minimum bid.
        if row.ain in seen:
            continue
        seen.add(row.ain)
        rows.append(asdict(row))
    return rows


if __name__ == "__main__":
    import json
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "data/2026A-Auction-Book.pdf"
    out = parse_pdf_to_list(path)
    print(json.dumps(out[:5], indent=2))
    print(f"\n{len(out)} parcels parsed")
