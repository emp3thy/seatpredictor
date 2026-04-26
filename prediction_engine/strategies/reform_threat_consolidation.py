"""Reform-threat consolidation strategy."""
import json
import logging

import pandas as pd

from prediction_engine.polls import compute_swing
from prediction_engine.projection import project_raw_shares
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.strategies.base import Strategy, register
from prediction_engine.strategies.uniform_swing import (
    PredictionResult,
    _add_winner_and_metadata,
    _compute_national_totals,
)
from schema.common import LEFT_BLOC, Nation, PartyCode
from schema.prediction import ReformThreatConfig

logger = logging.getLogger(__name__)


def identify_consolidator(
    shares: dict[PartyCode, float],
    nation: str,
    min_share: float = 2.0,
) -> PartyCode | None:
    """Per-seat: return the left-bloc party with the highest current share, or None
    if no left-bloc party clears min_share.

    Tie-break: when two parties tie on share, the alphabetically-earlier PartyCode value
    wins. This matches data_engine.transforms.transfer_matrix._identify_consolidator's
    tie-break (sort by gain desc, then actual_share desc, then party ascending).
    """
    nation_enum = Nation(nation)
    left = LEFT_BLOC[nation_enum]
    if not left:
        return None
    eligible = [p for p in left if shares.get(p, 0.0) >= min_share]
    if not eligible:
        return None
    # min over (-share, party_value) → highest share wins; ties broken by alphabetical party code.
    return min(eligible, key=lambda p: (-shares[p], p.value))


def compute_clarity(
    shares: dict[PartyCode, float],
    consolidator: PartyCode,
    nation: str,
    threshold: float,
) -> float:
    """gap = consolidator share − next-highest left-bloc share; clarity = clip(gap / threshold, 0, 1)."""
    if threshold <= 0:
        raise ValueError(f"threshold must be > 0 (got {threshold})")
    nation_enum = Nation(nation)
    left = LEFT_BLOC[nation_enum] - {consolidator}
    if not left:
        return 1.0  # consolidator is the only left-bloc party
    next_highest = max((shares.get(p, 0.0) for p in left), default=0.0)
    gap = shares.get(consolidator, 0.0) - next_highest
    return max(0.0, min(1.0, gap / threshold))


def apply_flows(
    shares: dict[PartyCode, float],
    leader: PartyCode,
    consolidator: PartyCode,
    weights: dict[PartyCode, float],
    clarity: float,
    multiplier: float,
    flag_sink: list[str],
) -> dict[PartyCode, float]:
    """Redistribute share from each weight-source party to the consolidator.

    For each party p in weights: moved = shares[p] * weight * clarity * multiplier,
    clipped to shares[p]. Mutations isolated to a copy; flag_sink mutated to log
    'multiplier_clipped' if any flow saturated. The leader (and consolidator) are
    excluded from being a source even if they appear in weights.
    """
    out = dict(shares)
    for source, weight in weights.items():
        if source == leader or source == consolidator:
            continue
        wanted = out[source] * weight * clarity * multiplier
        moved = min(wanted, out[source])
        if wanted > out[source] + 1e-9:
            if "multiplier_clipped" not in flag_sink:
                flag_sink.append("multiplier_clipped")
        out[source] -= moved
        out[consolidator] += moved
        logger.debug(
            "apply_flows: %s -> %s moved=%.4f (wanted=%.4f)",
            source.value, consolidator.value, moved, wanted,
        )
    return out


@register("reform_threat_consolidation")
class ReformThreatStrategy(Strategy):
    name = "reform_threat_consolidation"
    config_schema = ReformThreatConfig

    def predict(self, snapshot: Snapshot, scenario: ReformThreatConfig) -> PredictionResult:
        gb_swing = compute_swing(
            snapshot.polls,
            snapshot.results_2024,
            as_of=snapshot.manifest.as_of_date,
            window_days=scenario.polls_window_days,
            geography="GB",
        )
        per_seat = project_raw_shares(snapshot.results_2024, {"GB": gb_swing})

        # Initialise tactical-output columns to uniform-swing defaults; the per-seat loop
        # may override them.
        for p in PartyCode:
            per_seat[f"share_predicted_{p.value}"] = per_seat[f"share_raw_{p.value}"]
        per_seat["consolidator"]      = None
        per_seat["clarity"]           = None
        per_seat["matrix_nation"]     = None
        per_seat["matrix_provenance"] = "[]"
        per_seat["notes"]             = "[]"

        rows: list[dict] = []
        for _, row in per_seat.sort_values(by="ons_code").iterrows():
            updated = _predict_seat(row.to_dict(), snapshot, scenario)
            rows.append(updated)
        per_seat = pd.DataFrame(rows)

        per_seat = _add_winner_and_metadata(per_seat)
        per_seat = per_seat.sort_values(by="ons_code").reset_index(drop=True)
        national = _compute_national_totals(per_seat)

        return PredictionResult(
            per_seat=per_seat,
            national=national,
            run_metadata={
                "strategy": self.name,
                "scenario": scenario.model_dump(mode="json"),
                "snapshot_id": snapshot.snapshot_id,
            },
        )


def _predict_seat(row: dict, snapshot: Snapshot, scenario: ReformThreatConfig) -> dict:
    """Apply the reform-threat algorithm to one seat row. Returns a dict in seats-table
    schema with per-party share_predicted_* and metadata fields.

    Step ordering (matches spec §5.3 strictly):
      1. NI short-circuit
      2. leader != Reform → non_reform_leader fallback
      3. identify consolidator (None → matrix_unavailable; consolidator >= leader → consolidator_already_leads)
      4. compute clarity (always, regardless of matrix availability)
      5. matrix availability check (no consolidator entries → matrix_unavailable, but preserve clarity)
      6. weights lookup per source (cell missing → no_matrix_entry, source share unchanged)
      7. apply flows scaled by clarity × multiplier
      8. re-normalise to 100
    """
    nation = row["nation"]
    flags: list[str] = []

    raw_shares: dict[PartyCode, float] = {p: float(row[f"share_raw_{p.value}"]) for p in PartyCode}

    # 1. NI short-circuit.
    if nation == "northern_ireland":
        flags.append("ni_excluded")
        return _seat_with_flags(row, raw_shares, leader=_argmax(raw_shares),
                                consolidator=None, clarity=None, matrix_nation=None,
                                provenance=[], flags=flags)

    # 2. Non-Reform leader fallback.
    leader = _argmax(raw_shares)
    if leader != PartyCode.REFORM:
        flags.append("non_reform_leader")
        return _seat_with_flags(row, raw_shares, leader=leader,
                                consolidator=None, clarity=None, matrix_nation=None,
                                provenance=[], flags=flags)

    # 3. Identify consolidator.
    consolidator = identify_consolidator(raw_shares, nation=nation)
    if consolidator is None:
        flags.append("matrix_unavailable")
        return _seat_with_flags(row, raw_shares, leader=leader,
                                consolidator=None, clarity=None, matrix_nation=nation,
                                provenance=[], flags=flags)

    if raw_shares[consolidator] >= raw_shares[leader]:
        flags.append("consolidator_already_leads")
        return _seat_with_flags(row, raw_shares, leader=leader,
                                consolidator=consolidator, clarity=None,
                                matrix_nation=nation, provenance=[], flags=flags)

    # 4. Compute clarity. (Spec §5.3 step 4: clarity is always meaningful for an
    # identified consolidator; it does NOT depend on matrix availability.)
    clarity = compute_clarity(raw_shares, consolidator, nation, scenario.clarity_threshold)
    if clarity < 0.5:
        flags.append("low_clarity")

    # 5. Matrix availability. Preserve consolidator + clarity for analyst inspection.
    if not snapshot.consolidator_observed(nation, consolidator.value):
        flags.append("matrix_unavailable")
        return _seat_with_flags(row, raw_shares, leader=leader,
                                consolidator=consolidator, clarity=clarity,
                                matrix_nation=nation, provenance=[], flags=flags)

    # 6. Per-source weight lookup. Missing cells flag once and skip that source.
    weights: dict[PartyCode, float] = {}
    for source in PartyCode:
        if source in (leader, consolidator) or raw_shares[source] <= 0.0:
            continue
        w = snapshot.lookup_weight(nation, consolidator.value, source.value)
        if w is None:
            if "no_matrix_entry" not in flags:
                flags.append("no_matrix_entry")
            continue
        weights[source] = w

    # 7. Apply flows.
    new_shares = apply_flows(
        raw_shares,
        leader=leader,
        consolidator=consolidator,
        weights=weights,
        clarity=clarity,
        multiplier=scenario.multiplier,
        flag_sink=flags,
    )

    # 8. Re-normalise to 100 (apply_flows preserves total in exact arithmetic; float drift
    # makes this safe).
    total = sum(new_shares.values())
    if total > 0:
        new_shares = {p: v * 100.0 / total for p, v in new_shares.items()}

    provenance = snapshot.provenance_for_consolidator(nation, consolidator.value)
    return _seat_with_flags(row, new_shares, leader=leader, consolidator=consolidator,
                            clarity=clarity, matrix_nation=nation,
                            provenance=provenance, flags=flags)


def _seat_with_flags(
    row: dict,
    shares: dict[PartyCode, float],
    leader: PartyCode,
    consolidator: PartyCode | None,
    clarity: float | None,
    matrix_nation: str | None,
    provenance: list[str],
    flags: list[str],
) -> dict:
    out = dict(row)
    for p in PartyCode:
        out[f"share_predicted_{p.value}"] = shares[p]
    out["leader"]            = leader.value
    out["consolidator"]      = consolidator.value if consolidator else None
    out["clarity"]           = clarity
    out["matrix_nation"]     = matrix_nation
    out["matrix_provenance"] = json.dumps(sorted(provenance))
    out["notes"]             = json.dumps(flags)
    return out


def _argmax(shares: dict[PartyCode, float]) -> PartyCode:
    return max(shares, key=lambda p: (shares[p], -ord(p.value[0])))
