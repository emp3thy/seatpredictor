import logging

import pandas as pd
from schema.common import LEFT_BLOC, Nation, PartyCode

logger = logging.getLogger(__name__)


PRIOR_SHARE_THRESHOLD = 2.0  # percentage points


def derive_transfer_matrix(
    events: pd.DataFrame,
    results: pd.DataFrame,
    overrides: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Derive the reform_threat transfer matrix from by-election data.

    overrides, when given, is the frame returned by
    data_engine.sources.transfer_overrides.load_transfer_overrides. Each row
    replaces the derived cell with the same (nation, consolidator, source) key,
    or creates that cell if the data produced none. Overridden cells carry n = 0.

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
    if cell_records:
        raw = pd.DataFrame(cell_records)
        cells = (
            raw.groupby(["nation", "consolidator", "source"], as_index=False)
            .agg(weight=("weight", "mean"), n=("event_id", "nunique"))
        )
        provenance = pd.DataFrame(prov_records)
        logger.info("Derived %d matrix cells from %d eligible events", len(cells), n_events)
    else:
        cells = pd.DataFrame(
            columns=["nation", "consolidator", "source", "weight", "n"]
        ).astype({"weight": "float64", "n": "int64"})
        provenance = pd.DataFrame(columns=["nation", "consolidator", "event_id"])

    cells, provenance = _apply_overrides(cells, provenance, overrides)
    return cells, provenance


def _apply_overrides(
    cells: pd.DataFrame,
    provenance: pd.DataFrame,
    overrides: pd.DataFrame | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Replace or create cells from hand-curated overrides.

    Overridden and created cells carry n = 0 to mark them as unsupported by any
    by-election. Each distinct (nation, consolidator) touched gains a single
    'hand_curated' provenance row, so a seat's matrix_provenance shows at a glance
    that curated numbers were involved.
    """
    if overrides is None or overrides.empty:
        return cells, provenance

    cells = cells.copy()
    for _, o in overrides.iterrows():
        key = (
            (cells["nation"] == o["nation"])
            & (cells["consolidator"] == o["consolidator"])
            & (cells["source"] == o["source"])
        )
        if key.any():
            cells.loc[key, "weight"] = float(o["weight"])
            cells.loc[key, "n"] = 0
        else:
            new_row = pd.DataFrame([{
                "nation": o["nation"],
                "consolidator": o["consolidator"],
                "source": o["source"],
                "weight": float(o["weight"]),
                "n": 0,
            }])
            # Concatenating onto a genuinely empty (0-row) frame triggers pandas'
            # "empty or all-NA entries" FutureWarning and can lose the explicit
            # weight/n dtypes, so replace outright instead of concatenating when
            # there is nothing to preserve.
            cells = new_row if cells.empty else pd.concat([cells, new_row], ignore_index=True)

    prov_rows = [
        {"nation": nation, "consolidator": consolidator, "event_id": "hand_curated"}
        for nation, consolidator in (
            overrides[["nation", "consolidator"]].drop_duplicates().itertuples(index=False)
        )
    ]
    provenance = pd.concat(
        [provenance, pd.DataFrame(prov_rows)], ignore_index=True
    )

    cells["weight"] = cells["weight"].astype(float)
    cells["n"] = cells["n"].astype(int)
    logger.info("Applied %d hand-curated overrides", len(overrides))
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
    """For each eligible source party, the fraction of its vote that moved to the
    consolidator.

    The consolidator's own gain is the transfer budget. A source party's raw
    shrinkage, (prior - actual) / prior, measures how much of its vote disappeared —
    not how much reached the consolidator. Scaling every raw shrinkage by
    consolidator_gain / total_loss makes the transferred total equal the
    consolidator's actual gain: an accounting identity, since shares sum to 100
    before and after, so total gains equal total losses. Whatever the consolidator
    did not capture was absorbed by the threat party, without needing to be
    modelled explicitly.

    Restore sits right of Reform: like Reform it is never a flow *source*, but both
    are counted in total_loss when they shrink, because they compete for the same
    freed vote.
    """
    cons_row = ev_results[ev_results["party"] == consolidator.value]
    if cons_row.empty:
        return {}
    consolidator_gain = float(cons_row.iloc[0]["actual_share"]) - float(
        cons_row.iloc[0]["prior_share"]
    )
    if consolidator_gain <= 0:
        return {}

    total_loss = 0.0
    for _, r in ev_results.iterrows():
        if PartyCode(r["party"]) == consolidator:
            continue
        total_loss += max(0.0, float(r["prior_share"]) - float(r["actual_share"]))
    if total_loss <= 0:
        return {}

    scale = max(0.0, min(1.0, consolidator_gain / total_loss))

    flows: dict[PartyCode, float] = {}
    for _, r in ev_results.iterrows():
        party = PartyCode(r["party"])
        if party in (PartyCode.REFORM, PartyCode.RESTORE) or party == consolidator:
            continue
        prior = float(r["prior_share"])
        actual = float(r["actual_share"])
        if prior <= PRIOR_SHARE_THRESHOLD:
            continue
        raw_flow = (prior - actual) / prior
        flows[party] = max(0.0, min(1.0, raw_flow * scale))
    return flows
