import io
import logging

import pandas as pd
from schema.common import Nation, PartyCode

logger = logging.getLogger(__name__)


# Columns that must never be rolled into the "other" vote total.
_FIXED_EXCLUDES: set[str] = {
    "electorate", "valid votes", "valid_votes",
    "total votes", "rejected ballots", "majority",
    "invalid votes", "declaration time",
    "county name",
}


def _is_excluded_column(col_name: str) -> bool:
    """Return True if the column should not be rolled into 'other'."""
    n = col_name.lower().strip()
    if n in _FIXED_EXCLUDES:
        return True
    if n.startswith("of which "):
        return True
    if " winner" in n:
        return True
    return False


# Aliases the parser will accept for each party column. Lower-cased, trimmed.
_PARTY_ALIASES: dict[PartyCode, set[str]] = {
    PartyCode.LAB: {"lab", "labour", "lab co-op", "labour co-operative", "lab/co-op"},
    PartyCode.CON: {"con", "conservative", "conservatives", "conservative & unionist"},
    PartyCode.LD: {"ld", "lib dem", "libdem", "liberal democrat", "liberal democrats"},
    PartyCode.REFORM: {"reform", "ref", "ruk", "reform uk"},
    PartyCode.GREEN: {"green", "grn", "green party"},
    PartyCode.SNP: {"snp", "scottish national party"},
    PartyCode.PLAID: {"pc", "plaid", "plaid cymru"},
}

# All other identified parties roll up into "other".
_NATION_ALIASES: dict[str, Nation] = {
    "england": Nation.ENGLAND,
    "wales": Nation.WALES,
    "scotland": Nation.SCOTLAND,
    "northern ireland": Nation.NORTHERN_IRELAND,
}

# Identifying columns the parser expects. First match wins; checked case-insensitively.
_ONS_COL_CANDIDATES = ("ons id", "ons_id", "constituency id", "constituency_id", "pcon code")
_NAME_COL_CANDIDATES = ("constituency name", "constituency", "constituency_name")
_REGION_COL_CANDIDATES = ("region name", "region", "european region")
_NATION_COL_CANDIDATES = ("country name", "country", "nation")
_VALID_VOTES_CANDIDATES = ("valid votes", "valid_votes", "total votes", "valid vote")


def parse_hoc_results(csv_bytes: bytes) -> pd.DataFrame:
    """Parse the HoC Library 2024 GE results CSV into a tidy long DataFrame.

    Robust to column-name variation. Skips party columns it doesn't recognise and rolls
    them into 'other'. Returns one row per (constituency, party) with columns:
    ons_code, constituency_name, region, nation, party, votes, share.
    """
    raw = pd.read_csv(io.BytesIO(csv_bytes))
    raw.columns = [c.strip() for c in raw.columns]
    lower_to_actual = {c.lower(): c for c in raw.columns}

    ons_col = _first_match(_ONS_COL_CANDIDATES, lower_to_actual)
    name_col = _first_match(_NAME_COL_CANDIDATES, lower_to_actual)
    region_col = _first_match(_REGION_COL_CANDIDATES, lower_to_actual)
    nation_col = _first_match(_NATION_COL_CANDIDATES, lower_to_actual)
    valid_col = _first_match(_VALID_VOTES_CANDIDATES, lower_to_actual)
    if not all([ons_col, name_col, region_col, nation_col, valid_col]):
        raise ValueError(
            f"HoC CSV missing required columns. "
            f"ons={ons_col} name={name_col} region={region_col} nation={nation_col} valid={valid_col} "
            f"available={list(raw.columns)[:30]}..."
        )

    # Map each PartyCode to the actual CSV column it corresponds to (if any).
    party_col_for: dict[PartyCode, str | None] = {}
    matched_columns: set[str] = {ons_col, name_col, region_col, nation_col, valid_col}  # type: ignore[arg-type]
    for party, aliases in _PARTY_ALIASES.items():
        for alias in aliases:
            if alias in lower_to_actual:
                party_col_for[party] = lower_to_actual[alias]
                matched_columns.add(lower_to_actual[alias])
                break
        else:
            party_col_for[party] = None

    # Any numeric column NOT matched by a known party rolls up into "other".
    other_cols = [
        c for c in raw.columns
        if c not in matched_columns
        and pd.api.types.is_numeric_dtype(raw[c])
        and not _is_excluded_column(c)
    ]
    if other_cols:
        logger.info(
            "Rolled %d unrecognised columns into 'other': %s",
            len(other_cols),
            ", ".join(other_cols[:5]) + ("..." if len(other_cols) > 5 else ""),
        )

    rows: list[dict] = []
    for _, r in raw.iterrows():
        ons = str(r[ons_col]).strip()
        name = str(r[name_col]).strip()
        region = str(r[region_col]).strip()
        nation_str = str(r[nation_col]).strip().lower()
        if nation_str not in _NATION_ALIASES:
            continue  # skip unknown nation rows
        nation = _NATION_ALIASES[nation_str].value
        valid = float(r[valid_col]) if pd.notna(r[valid_col]) else 0.0

        # Per-party rows
        other_votes = 0
        for party, col in party_col_for.items():
            votes = int(r[col]) if col and pd.notna(r[col]) else 0
            share = (votes / valid * 100.0) if valid > 0 else 0.0
            rows.append({
                "ons_code": ons, "constituency_name": name, "region": region,
                "nation": nation, "party": party.value,
                "votes": votes, "share": round(share, 2),
            })

        # Roll up other columns into "other"
        for c in other_cols:
            v = r[c]
            if pd.notna(v):
                other_votes += int(round(v))
        share = (other_votes / valid * 100.0) if valid > 0 else 0.0
        rows.append({
            "ons_code": ons, "constituency_name": name, "region": region,
            "nation": nation, "party": PartyCode.OTHER.value,
            "votes": other_votes, "share": round(share, 2),
        })
    n_seats = len(raw)
    logger.info("Parsed %d constituencies × parties = %d rows", n_seats, len(rows))
    return pd.DataFrame(rows)


def _first_match(candidates: tuple[str, ...], lower_to_actual: dict[str, str]) -> str | None:
    for c in candidates:
        if c in lower_to_actual:
            return lower_to_actual[c]
    return None
