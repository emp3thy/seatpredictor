import logging

import pandas as pd
from schema.common import LEFT_BLOC, Nation, PartyCode

logger = logging.getLogger(__name__)


PRIOR_SHARE_THRESHOLD = 2.0  # percentage points


def derive_transfer_matrix(
    events: pd.DataFrame,
    results: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Derive the reform_threat transfer matrix from by-election data.

    Returns:
      cells: DataFrame with columns nation, consolidator, source, weight, n.
      provenance: DataFrame with columns nation, consolidator, event_id.
    """
    cell_records: list[dict] = []
    prov_records: list[dict] = []

    eligible = events[
        (events["threat_party"] == PartyCode.REFORM.value)
        & (events["exclude_from_matrix"] == False)  # noqa: E712
    ]

    for _, ev in eligible.iterrows():
        event_id = ev["event_id"]
        nation = Nation(ev["nation"])
        ev_results = results[results["event_id"] == event_id]
        consolidator = _identify_consolidator(ev_results, nation)
        if consolidator is None:
            continue
        flows = _compute_flows(ev_results, consolidator)
        for source, flow in flows.items():
            cell_records.append({
                "nation": nation.value,
                "consolidator": consolidator.value,
                "source": source.value,
                "weight": flow,
                "n_event": 1,
                "event_id": event_id,
            })
        prov_records.append({
            "nation": nation.value,
            "consolidator": consolidator.value,
            "event_id": event_id,
        })

    n_events = len(eligible)
    if not cell_records:
        empty_cells = pd.DataFrame(columns=["nation", "consolidator", "source", "weight", "n"])
        empty_prov = pd.DataFrame(columns=["nation", "consolidator", "event_id"])
        return empty_cells, empty_prov

    raw = pd.DataFrame(cell_records)
    cells = (
        raw.groupby(["nation", "consolidator", "source"], as_index=False)
        .agg(weight=("weight", "mean"), n=("event_id", "nunique"))
    )
    provenance = pd.DataFrame(prov_records)
    logger.info("Derived %d matrix cells from %d eligible events", len(cells), n_events)
    return cells, provenance


def _identify_consolidator(
    ev_results: pd.DataFrame,
    nation: Nation,
) -> PartyCode | None:
    """Return the left-bloc party with the largest gain over its prior share.
    Deterministic tie-break: when two parties tie on gain, pick the one with the
    higher actual_share (the locally-stronger party); if still tied, pick by
    party-code alphabetical order (final fallback so the function is total).
    """
    left = LEFT_BLOC[nation]
    if not left:
        return None
    candidates = ev_results[ev_results["party"].isin([p.value for p in left])].copy()
    if candidates.empty:
        return None
    candidates["gain"] = candidates["actual_share"] - candidates["prior_share"]
    candidates = candidates.sort_values(
        by=["gain", "actual_share", "party"],
        ascending=[False, False, True],
    )
    best = candidates.iloc[0]
    if best["gain"] <= 0:
        return None
    return PartyCode(best["party"])


def _compute_flows(
    ev_results: pd.DataFrame,
    consolidator: PartyCode,
) -> dict[PartyCode, float]:
    """For each non-Reform, non-consolidator party with prior_share above threshold,
    compute (prior - actual) / prior, clamped [0, 1]."""
    flows: dict[PartyCode, float] = {}
    for _, r in ev_results.iterrows():
        party = PartyCode(r["party"])
        if party == PartyCode.REFORM or party == consolidator:
            continue
        prior = float(r["prior_share"])
        actual = float(r["actual_share"])
        if prior <= PRIOR_SHARE_THRESHOLD:
            continue
        raw_flow = (prior - actual) / prior
        flows[party] = max(0.0, min(1.0, raw_flow))
    return flows
