import re
from datetime import date

import httpx
import pandas as pd
from bs4 import BeautifulSoup, Tag


POLLS_URL = "https://en.wikipedia.org/wiki/Opinion_polling_for_the_next_United_Kingdom_general_election"
USER_AGENT = "seatpredictor/0.0.1 (research; contact: see repository)"

# Header text → internal column. Match is case-insensitive and exact (after strip).
_PARTY_HEADER_MAP = {
    "lab": "lab", "labour": "lab",
    "con": "con", "conservative": "con",
    "ld": "ld", "lib dem": "ld", "liberal democrats": "ld",
    "reform": "reform", "ref": "reform", "ruk": "reform",
    "grn": "green", "green": "green",
    "snp": "snp",
    "pc": "plaid", "plaid": "plaid",
    "others": "other", "other": "other",
}

# Tables we accept must contain at least these party columns (post-normalisation).
_REQUIRED_PARTIES_FOR_VI_TABLE = {"lab", "con", "reform"}

_MONTH = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


def fetch_polls_html(url: str) -> str:
    """Fetch a Wikipedia polling page; return raw HTML text. Honours a custom UA."""
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30.0) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.text


def parse_polls_html(html: str, geography: str) -> pd.DataFrame:
    """Parse polls from a Wikipedia polling page.

    Returns one row per poll with columns:
    pollster, fieldwork_start, fieldwork_end, published_date,
    sample_size, geography, con, lab, ld, reform, green, snp, plaid, other.

    Strategy: walk every <table class="wikitable">; admit only tables whose header
    contains a Pollster column AND at least Lab/Con/Reform party columns. Skip rows
    that fail date parsing. Tolerates footnote refs, asterisks, "—" cells.

    Real Wikipedia page notes (as of 2026):
    - Date column header is "Date(s)conducted" (no spaces), not "Date(s) conducted"
    - Sample column is "Samplesize" (no space)
    - Party "Reform" is listed as "Ref" in column headers
    - Date cells have NO year in text; year is in data-sort-value attribute (end date)
    - Start date is derived from text range + year from data-sort-value end date
    """
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict] = []
    for table in soup.find_all("table", class_="wikitable"):
        header_row = _find_header_row(table)
        if header_row is None:
            continue
        header_cells = [_clean(th) for th in header_row.find_all(["th", "td"])]
        if not header_cells:
            continue

        # Map header index → internal key (party slot or sentinel).
        col_map = _build_column_map(header_cells)
        if not col_map.get("pollster"):
            continue
        party_keys_present = {v for k, v in col_map.items() if v in {"lab", "con", "ld", "reform", "green", "snp", "plaid", "other"}}
        if not _REQUIRED_PARTIES_FOR_VI_TABLE <= party_keys_present:
            continue  # not a national-VI-shaped table

        for tr in table.find_all("tr"):
            if tr is header_row:
                continue
            td_nodes = tr.find_all(["td", "th"])
            if len(td_nodes) < len(header_cells):
                continue
            poll = _parse_row(td_nodes, col_map, geography=geography)
            if poll is not None:
                rows.append(poll)
    return pd.DataFrame(rows)


# --- helpers ---

def _find_header_row(table: Tag) -> Tag | None:
    """Pick the row that contains the column headers (usually the first <tr>)."""
    rows = table.find_all("tr")
    for tr in rows:
        # Heuristic: header row has more <th> than <td>.
        ths = len(tr.find_all("th"))
        tds = len(tr.find_all("td"))
        if ths > tds and ths >= 4:
            return tr  # type: ignore[return-value]
    return rows[0] if rows else None  # type: ignore[return-value]


def _build_column_map(headers: list[str]) -> dict[str, str]:
    """index → internal key. Returns dict of {pollster|sample|date|<party>: idx_str}.
    The keys store stringified indices so we can look them up easily.
    """
    out: dict[str, str] = {}
    for idx, h in enumerate(headers):
        nl = _norm_header(h)
        if nl == "pollster":
            out["pollster"] = str(idx)
        elif nl in {"sample size", "sample", "samplesize"}:
            out["sample"] = str(idx)
        elif nl in {
            "dates conducted", "date(s) conducted", "date conducted",
            "date(s)conducted", "datesconducted",
            "date", "fieldwork", "fieldwork dates",
        }:
            out["date"] = str(idx)
        elif nl in _PARTY_HEADER_MAP:
            out[str(idx)] = _PARTY_HEADER_MAP[nl]
    return out


def _norm_header(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"\[[^\]]*\]", "", s)  # remove footnote refs
    s = re.sub(r"\s+", " ", s)
    return s


def _clean(node: Tag) -> str:
    text = node.get_text()
    text = re.sub(r"\[[^\]]*\]", "", text)   # footnote refs e.g. [1] [a]
    text = text.replace(" ", " ").replace("–", "-").replace("—", "-")
    text = text.replace("&ndash;", "-").replace("&mdash;", "-").replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = text.rstrip("*").strip()
    return text


def _parse_row(
    td_nodes: list[Tag], col_map: dict[str, str], *, geography: str
) -> dict | None:
    pollster_idx = int(col_map["pollster"]) if "pollster" in col_map else -1
    if pollster_idx < 0 or pollster_idx >= len(td_nodes):
        return None
    pollster = _clean(td_nodes[pollster_idx]).strip()
    if not pollster or pollster.lower().startswith("source"):
        return None

    if "date" not in col_map:
        return None
    date_idx = int(col_map["date"])
    if date_idx >= len(td_nodes):
        return None
    date_node = td_nodes[date_idx]
    fws, fwe = _parse_date_from_node(date_node)
    if fws is None or fwe is None:
        return None

    sample_idx = int(col_map["sample"]) if "sample" in col_map else -1
    sample = _parse_int(_clean(td_nodes[sample_idx])) if sample_idx >= 0 and sample_idx < len(td_nodes) else 0

    out: dict = {
        "pollster": pollster,
        "fieldwork_start": fws.isoformat(),
        "fieldwork_end": fwe.isoformat(),
        "published_date": fwe.isoformat(),
        "sample_size": sample,
        "geography": geography,
        "con": 0.0, "lab": 0.0, "ld": 0.0, "reform": 0.0,
        "green": 0.0, "snp": 0.0, "plaid": 0.0, "other": 0.0,
    }
    for k, v in col_map.items():
        if k in {"pollster", "sample", "date"}:
            continue
        idx = int(k)
        if idx >= len(td_nodes):
            continue
        out[v] = _parse_pct(_clean(td_nodes[idx]))
    return out


def _parse_date_from_node(node: Tag) -> tuple[date | None, date | None]:
    """Extract start and end dates from a date cell node.

    Strategy:
    1. If node has data-sort-value (ISO YYYY-MM-DD), use that as end date.
    2. Parse text to find start date (day/month), apply year from end date.
    3. If no data-sort-value, fall back to full text parsing.
    """
    sort_val = node.get("data-sort-value", "")  # type: ignore[arg-type]
    text = _clean(node)

    # Remove footnote markers from text
    text = re.sub(r"\[[^\]]*\]", "", text).strip()

    if sort_val and re.match(r"\d{4}-\d{2}-\d{2}", str(sort_val)):
        end_date = _parse_iso(str(sort_val))
        if end_date:
            start_date = _derive_start_date(text, end_date)
            return start_date, end_date

    # Fallback: full text with explicit year
    return _parse_date_range(text)


def _parse_iso(s: str) -> date | None:
    """Parse YYYY-MM-DD string to date."""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    return None


def _derive_start_date(text: str, end_date: date) -> date:
    """Given date range text (no year) and known end date, compute start date.

    Handles: "22-23 Apr", "22 Apr", "29 Mar - 1 Apr", "17-20 Apr".
    Year is taken from end_date (rolling back one year if month ordering implies it).
    """
    # Normalise dashes / hyphens
    text = text.replace("–", "-").replace("—", "-").strip()

    # Single day: "15 Apr" — start == end
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)$", text)
    if m:
        return end_date  # single day

    # Same-month range: "17-20 Apr"
    m = re.match(r"^(\d{1,2})\s*-\s*\d{1,2}\s+([A-Za-z]+)$", text)
    if m:
        d1, mon = m.groups()
        month = _MONTH.get(mon.lower(), _MONTH.get(mon.lower()[:3]))
        if month:
            yr = end_date.year
            if month > end_date.month:
                yr -= 1  # rolled into previous year
            try:
                return date(yr, month, int(d1))
            except ValueError:
                return end_date

    # Cross-month range: "29 Mar - 1 Apr"
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s*-\s*\d{1,2}\s+([A-Za-z]+)$", text)
    if m:
        d1, mon1, _mon2 = m.groups()
        m1 = _MONTH.get(mon1.lower(), _MONTH.get(mon1.lower()[:3]))
        m2 = _MONTH.get(_mon2.lower(), _MONTH.get(_mon2.lower()[:3]))
        if m1 and m2:
            yr = end_date.year
            if m1 > (m2 or end_date.month):
                yr -= 1  # start is in previous year (Dec-Jan boundary)
            try:
                return date(yr, m1, int(d1))
            except ValueError:
                return end_date

    return end_date  # fallback: use end date as start


def _parse_date_range(text: str) -> tuple[date | None, date | None]:
    """Handles dates with explicit year in text.

    Formats: '18-20 Apr 2026', '29 Mar - 1 Apr 2026', '18 Apr 2026',
    '18 Apr 2026 - 1 May 2026', '29 Mar 2025 - 1 Apr 2026'.
    """
    text = text.strip()
    # Single day: "18 Apr 2026"
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", text)
    if m:
        d, mon, yr = m.groups()
        month = _MONTH.get(mon.lower(), _MONTH.get(mon.lower()[:3]))
        if month:
            dd = date(int(yr), month, int(d))
            return dd, dd

    # Range same month: "18-20 Apr 2026"
    m = re.match(r"^(\d{1,2})\s*-\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", text)
    if m:
        d1, d2, mon, yr = m.groups()
        month = _MONTH.get(mon.lower(), _MONTH.get(mon.lower()[:3]))
        if month:
            return date(int(yr), month, int(d1)), date(int(yr), month, int(d2))

    # Range cross-month same year: "29 Mar - 1 Apr 2026"
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s*-\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", text)
    if m:
        d1, mon1, d2, mon2, yr = m.groups()
        m1 = _MONTH.get(mon1.lower(), _MONTH.get(mon1.lower()[:3]))
        m2 = _MONTH.get(mon2.lower(), _MONTH.get(mon2.lower()[:3]))
        if m1 and m2:
            return date(int(yr), m1, int(d1)), date(int(yr), m2, int(d2))

    # Range cross-year: "29 Dec 2025 - 3 Jan 2026"
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s*-\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", text)
    if m:
        d1, mon1, yr1, d2, mon2, yr2 = m.groups()
        m1 = _MONTH.get(mon1.lower(), _MONTH.get(mon1.lower()[:3]))
        m2 = _MONTH.get(mon2.lower(), _MONTH.get(mon2.lower()[:3]))
        if m1 and m2:
            return date(int(yr1), m1, int(d1)), date(int(yr2), m2, int(d2))

    return None, None


def _parse_int(s: str) -> int:
    s = re.sub(r"[^\d]", "", s)
    try:
        return int(s) if s else 0
    except ValueError:
        return 0


def _parse_pct(s: str) -> float:
    # Take only the first numeric token (handles "7%RB9%YP1%" style extra text)
    s = s.strip()
    m = re.match(r"([\d.]+)%?", s)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    if s in {"", "-", "—", "N/A", "n/a", "–"}:
        return 0.0
    return 0.0
