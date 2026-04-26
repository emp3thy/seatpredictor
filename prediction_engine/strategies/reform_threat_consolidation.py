"""Reform-threat consolidation strategy. Helpers in this module; full Strategy in Task 9."""
import logging

from schema.common import LEFT_BLOC, Nation, PartyCode

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
