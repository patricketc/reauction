"""Parse the LA County TTC auction book PDF into structured parcel rows.

The TTC auction book lays each parcel out in fixed columns:

    ITEM NO. | AIN | SITUS / LEGAL DESCRIPTION | NSB# | MIN BID

Earlier versions of this parser pulled the "rightmost dollar-looking number"
on each line as the minimum bid, which turned out to be wrong: the LEGAL
DESCRIPTION column often contains numbers (lot numbers, tract numbers, year
markers) and the NSB# column is itself an integer. Both sit to the right of
some money tokens, and either could be picked as the minimum bid.

The current parser is column-aware: it uses pdfplumber's word-position data
to find the x-range of the "MIN BID" column header, then extracts the money
value only from words that fall inside that x-range. It falls back to a
stricter "rightmost token with $/cents/commas" heuristic on pages where the
header can't be found (continuation pages, appendix blocks, etc.).

If the 2026A book changes layout, this is the first file to look at.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, Iterator

import pdfplumber

log = logging.getLogger(__name__)

# 10 digits, optionally separated ####-###-###. LA County AINs are always 10 digits.
AIN_RE = re.compile(r"\b(\d{4})[-\s]?(\d{3})[-\s]?(\d{3})\b")

# Dollar amount, with optional leading $ and optional commas/cents. The
# capture group is the numeric body so we can parse it.
MONEY_RE = re.compile(r"\$?\s?([\d,]+(?:\.\d{2})?)")

# Leading item number on a row (1-5 digits).
ITEM_RE = re.compile(r"^\s*(\d{1,5})\b")

# Tolerance (points) used to group pdfplumber words into the same visual line.
_LINE_TOL = 3.0


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


def _money_tokens(text: str) -> list[tuple[float, str]]:
    """Return ``(value, raw_token)`` pairs for every money-looking token."""
    out: list[tuple[float, str]] = []
    for m in MONEY_RE.finditer(text):
        raw = m.group(1)
        try:
            val = float(raw.replace(",", ""))
        except ValueError:
            continue
        out.append((val, raw))
    return out


def _extract_money_strict(text: str) -> float | None:
    """Fallback money extraction used when column detection fails.

    Prefers, in order:
      1. rightmost token with explicit cents (".XX") -- very likely a price
      2. rightmost token with thousands separators
      3. rightmost plain integer >= 100

    Small integers (< 100) are rejected because the NSB# column and a lot of
    legal-description numbers are small, and a $0-99 minimum bid is not
    realistic for LA County tax sales.
    """
    tokens = _money_tokens(text)
    if not tokens:
        return None
    for val, raw in reversed(tokens):
        if "." in raw and val >= 100:
            return val
    for val, raw in reversed(tokens):
        if "," in raw and val >= 100:
            return val
    for val, _ in reversed(tokens):
        if val >= 100:
            return val
    return None


def _group_words_into_lines(
    words: Iterable[dict], tol: float = _LINE_TOL
) -> list[list[dict]]:
    """Group pdfplumber words into rows by y-coordinate (top)."""
    ws = sorted(words, key=lambda w: (w["top"], w["x0"]))
    lines: list[list[dict]] = []
    for w in ws:
        if lines and abs(w["top"] - lines[-1][0]["top"]) <= tol:
            lines[-1].append(w)
        else:
            lines.append([w])
    for row in lines:
        row.sort(key=lambda w: w["x0"])
    return lines


def _find_min_bid_x_range(lines: list[list[dict]]) -> tuple[float, float] | None:
    """Locate the x-range covered by the "MIN BID" column header.

    Accepts a few common spellings the TTC has used across books: ``MIN BID``,
    ``MINIMUM BID``, ``MIN. BID``, and ``MIN. BID PRICE``.
    Returns ``(x0, x1)`` widened a touch so that a dollar sign or long comma
    number in the data row still lands inside the range.
    """
    for row in lines:
        tokens = [w["text"].upper().strip(".:,") for w in row]
        for i, tok in enumerate(tokens):
            if tok in ("MIN", "MINIMUM") and i + 1 < len(tokens) and tokens[i + 1] == "BID":
                x0 = row[i]["x0"]
                # Extend x1 to whichever comes later: end of "BID" or end of a
                # trailing "PRICE" word.
                x1 = row[i + 1]["x1"]
                if i + 2 < len(tokens) and tokens[i + 2] == "PRICE":
                    x1 = row[i + 2]["x1"]
                # Widen generously -- the data row values right-align under the
                # header and can extend past it, especially with a $ sign.
                return (x0 - 10.0, x1 + 60.0)
    return None


def _extract_min_bid_from_line(
    line_words: list[dict], min_bid_range: tuple[float, float] | None
) -> float | None:
    """Extract min bid for a single line, using column bounds if available."""
    if min_bid_range is not None:
        x0, x1 = min_bid_range
        col_words = [w for w in line_words if w["x0"] >= x0 and w["x1"] <= x1]
        if col_words:
            col_text = " ".join(w["text"] for w in col_words)
            # The min bid column only ever holds a money value -- don't filter
            # out small values here, trust the column.
            tokens = _money_tokens(col_text)
            if tokens:
                # Prefer a token with cents/commas, else take the last token.
                for val, raw in reversed(tokens):
                    if "." in raw or "," in raw:
                        return val
                return tokens[-1][0]
    # Either no header was found on this page, or the row had no words in the
    # column band (rare -- e.g. skewed scan). Fall back to the strict heuristic.
    return _extract_money_strict(" ".join(w["text"] for w in line_words))


def iter_parcel_rows(pdf_path: str | Path) -> Iterator[ParcelRow]:
    """Yield one ParcelRow per AIN found in the auction book.

    Processes pages individually so we can locate the MIN BID column header
    within each page. The header typically only appears on the first content
    page, so we also carry forward the most recently seen header range as the
    default for subsequent pages.
    """
    last_min_bid_range: tuple[float, float] | None = None

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_no, page in enumerate(pdf.pages, start=1):
            words = page.extract_words(
                keep_blank_chars=False, use_text_flow=False
            )
            if not words:
                continue

            lines = _group_words_into_lines(words)
            page_range = _find_min_bid_x_range(lines)
            if page_range is not None:
                last_min_bid_range = page_range
            min_bid_range = last_min_bid_range

            for line_words in lines:
                line_text = " ".join(w["text"] for w in line_words).strip()
                if not line_text:
                    continue
                m = AIN_RE.search(line_text)
                if not m:
                    continue

                ain, ain_fmt = _normalize_ain(m)

                # For min bid we use column-aware extraction on the word list;
                # for everything else the flat line text is easier to work with.
                min_bid = _extract_min_bid_from_line(line_words, min_bid_range)

                item_match = ITEM_RE.match(line_text)
                item_no = item_match.group(1) if item_match else None

                # Anything between the item number and the AIN is usually a
                # situs hint (address + city) in books that include it.
                head = line_text[: m.start()].strip()
                if item_no and head.startswith(item_no):
                    head = head[len(item_no):].strip()
                situs_hint = head or None

                yield ParcelRow(
                    item_no=item_no,
                    ain=ain,
                    ain_formatted=ain_fmt,
                    min_bid=min_bid,
                    raw_line=line_text,
                    situs_hint=situs_hint,
                )
            log.debug("page %d processed", page_no)


def parse_pdf_to_list(pdf_path: str | Path) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()
    for row in iter_parcel_rows(pdf_path):
        # Same AIN can appear on multiple lines (continuation rows). Keep the
        # first occurrence.
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
