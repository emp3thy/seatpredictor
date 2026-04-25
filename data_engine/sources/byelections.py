import logging
from datetime import date
from pathlib import Path

import pandas as pd
import yaml
from schema.byelection import ByElectionEvent, ByElectionResult

logger = logging.getLogger(__name__)


def load_byelections(
    yaml_path: Path,
    as_of: date | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load by_elections.yaml. Validates each event against Pydantic models AND
    cross-result invariants (shares sum to ~100% per event).
    Returns (events_df, results_df). If as_of is given, filters events with date <= as_of.
    """
    with yaml_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    event_rows: list[dict] = []
    result_rows: list[dict] = []
    n_before = 0

    for entry in raw["events"]:
        candidates = entry.pop("candidates", [])
        event = ByElectionEvent.model_validate(entry)
        n_before += 1
        if as_of is not None and event.date > as_of:
            continue

        validated = []
        for c in candidates:
            r = ByElectionResult.model_validate({"event_id": event.event_id, **c})
            validated.append(r)

        # Cross-result validation: shares sum to ~100% (±0.5pp tolerance per spec §4.3)
        actual_total = sum(r.actual_share for r in validated)
        prior_total = sum(r.prior_share for r in validated)
        if not (99.5 <= actual_total <= 100.5):
            raise ValueError(
                f"event {event.event_id}: actual_share entries sum to {actual_total:.2f} "
                f"(expected 99.5-100.5). Check vote tallies in by_elections.yaml."
            )
        if not (99.5 <= prior_total <= 100.5):
            raise ValueError(
                f"event {event.event_id}: prior_share entries sum to {prior_total:.2f} "
                f"(expected 99.5-100.5). Check prior_share values; missing parties should "
                f"be listed with prior_share: 0.0."
            )

        event_rows.append(event.model_dump(mode="json"))
        for r in validated:
            result_rows.append(r.model_dump(mode="json"))

    n_after = len(event_rows)
    logger.info(
        "Loaded %d by-election events (%d filtered by as_of %s)",
        n_after,
        n_before - n_after,
        as_of,
    )
    events_df = pd.DataFrame(event_rows)
    results_df = pd.DataFrame(result_rows)
    return events_df, results_df
