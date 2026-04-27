import logging
import warnings
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LocalElectionPNSSource:
    """One published source's PNS values for a local-election event."""
    source: str
    source_url: str
    shares: dict[str, float]   # PartyCode.value -> percentage; missing parties default to 0.0


@dataclass(frozen=True)
class LocalElectionEvent:
    """One local-election event with PNS published by 1+ sources, plus a consolidated value."""
    date: date
    name: str
    sources: list[LocalElectionPNSSource]
    consolidated_shares: dict[str, float]    # PartyCode.value -> percentage
    consolidated_method: str                  # "median_across_sources" | "sole_source"
    notes: str | None


def load_local_elections(path: Path) -> list[LocalElectionEvent]:
    """Parse local_elections.yaml. Returns events sorted by date ascending.

    Behaviour for non-happy paths:
      - File missing: emit UserWarning and return []. Bias analysis is expected to
        run with by-elections only in this case.
      - Consolidated shares don't sum to 100 ± 2: emit UserWarning per event but
        continue loading (the user may have intentionally entered partial data).
      - Per-source shares missing optional parties (snp/plaid/etc.): default to 0.0.

    Hard failures (raise) are reserved for actual schema violations (missing
    'date'/'name'/'pns' keys, unparseable YAML).
    """
    if not path.exists():
        warnings.warn(
            f"local_elections.yaml not found at {path}; bias analysis will use by-elections only.",
            UserWarning,
            stacklevel=2,
        )
        return []

    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not raw or "events" not in raw:
        warnings.warn(
            f"local_elections.yaml at {path} has no 'events' key; treating as empty.",
            UserWarning,
            stacklevel=2,
        )
        return []

    events: list[LocalElectionEvent] = []
    for entry in raw["events"]:
        # Hard schema check
        for required in ("date", "name", "pns"):
            if required not in entry:
                raise ValueError(f"local-election entry missing required key '{required}': {entry}")
        if "consolidated" not in entry["pns"]:
            raise ValueError(f"local-election entry missing pns.consolidated: {entry['name']}")
        if "sources" not in entry["pns"] or not entry["pns"]["sources"]:
            raise ValueError(f"local-election entry must have at least one pns.source: {entry['name']}")

        sources = [
            LocalElectionPNSSource(
                source=s["source"],
                source_url=s["source_url"],
                shares={k: float(v) for k, v in s["shares"].items()},
            )
            for s in entry["pns"]["sources"]
        ]
        consolidated_shares = {k: float(v) for k, v in entry["pns"]["consolidated"]["shares"].items()}
        method = entry["pns"]["consolidated"]["method"]
        if method not in ("median_across_sources", "sole_source"):
            raise ValueError(f"unknown consolidated.method '{method}' in event {entry['name']}")

        # Soft check: consolidated shares should sum to 100 ± 2
        total = sum(consolidated_shares.values())
        if not (98.0 <= total <= 102.0):
            warnings.warn(
                f"event '{entry['name']}': consolidated shares sums to {total} "
                f"(outside 98-102 range); check the YAML.",
                UserWarning,
                stacklevel=2,
            )

        events.append(LocalElectionEvent(
            date=entry["date"],
            name=entry["name"],
            sources=sources,
            consolidated_shares=consolidated_shares,
            consolidated_method=method,
            notes=entry.get("notes"),
        ))

    events.sort(key=lambda e: e.date)
    logger.info("Loaded %d local-election events from %s", len(events), path)
    return events
