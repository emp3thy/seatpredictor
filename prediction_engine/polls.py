import logging
from datetime import date, timedelta

import pandas as pd
from schema.common import PartyCode

logger = logging.getLogger(__name__)


# Nations covered by the GB national VI poll question (England + Scotland + Wales).
# Northern Ireland is excluded from GB polls — pollsters don't include NI respondents,
# and NI's seats are dominated by NI-specific parties (DUP, Sinn Féin, etc.) which
# bucket as `other` in our PartyCode enum. Including NI's ~18 seats in the GE-2024
# baseline would inflate `other` and dilute every other party's baseline share,
# producing systematically wrong per-party swings.
_GB_NATIONS: tuple[str, ...] = ("england", "scotland", "wales")


def ge2024_national_share(
    results_2024: pd.DataFrame,
    nations: tuple[str, ...] | None = None,
) -> dict[PartyCode, float]:
    """Vote-weighted national share per party from the 2024 GE results.

    nations: tuple of nation values (e.g. ("england", "scotland", "wales")) to restrict
    the aggregation. None means aggregate over every nation in the frame (rare in
    production — callers normally pass an explicit nation tuple).
    """
    df = results_2024
    if nations is not None:
        df = df.loc[df["nation"].isin(nations)]
    if df.empty:
        raise ValueError(f"no results for nations={nations}")

    by_party = df.groupby("party", as_index=False)["votes"].sum()
    total = float(by_party["votes"].sum())
    if total <= 0:
        raise ValueError("results_2024 votes sum to 0")

    # Pivot to party→votes dict in one pass; missing parties default to 0.
    votes_by_party: dict[str, float] = dict(zip(by_party["party"], by_party["votes"].astype(float)))
    return {p: (votes_by_party.get(p.value, 0.0) / total) * 100.0 for p in PartyCode}


# Map the poll-table `geography` label to the GE-2024 baseline nations.
# - "GB" excludes NI (see _GB_NATIONS comment above).
# - "Scotland" / "Wales" use only their own nation.
# - "London" has no London-only baseline column; falls back to GB-nation baseline.
_GEO_TO_NATIONS: dict[str, tuple[str, ...]] = {
    "GB": _GB_NATIONS,
    "Scotland": ("scotland",),
    "Wales": ("wales",),
    "London": _GB_NATIONS,
}


def compute_swing(
    polls: pd.DataFrame,
    results_2024: pd.DataFrame,
    as_of: date,
    window_days: int,
    geography: str,
    reform_polling_correction_pp: float = 0.0,
) -> dict[PartyCode, float]:
    """Average per-party poll share over the window, then subtract GE 2024 share.

    Window: published_date in (as_of - window_days, as_of].
    Failures (no polls match) raise ValueError per spec §8.
    The GE-2024 baseline is restricted to the nations that the poll geography covers
    — GB polls exclude Northern Ireland.

    reform_polling_correction_pp: optional correction added to Reform's swing AFTER
    the poll-derived swing is computed. Positive value: pollsters under-state Reform
    (so we bump the projected swing up by this many pp). Negative: pollsters
    over-state Reform. Default 0.0 is a no-op. The empirical recommended value is
    derived by notebook 05 from electoral events; users typically set it via the
    --reform-polling-correction-pp CLI flag on seatpredict-predict.
    """
    if window_days <= 0:
        raise ValueError(f"window_days must be > 0 (got {window_days})")

    cutoff_lo = (as_of - timedelta(days=window_days)).isoformat()
    cutoff_hi = as_of.isoformat()
    filt = (
        (polls["geography"] == geography)
        & (polls["published_date"] > cutoff_lo)
        & (polls["published_date"] <= cutoff_hi)
    )
    window = polls.loc[filt]
    if window.empty:
        raise ValueError(
            f"no polls in window: geography={geography} as_of={as_of} window_days={window_days}"
        )

    poll_means = {p: float(window[p.value].mean()) for p in PartyCode}
    ge_shares = ge2024_national_share(results_2024, nations=_GEO_TO_NATIONS[geography])
    swing = {p: poll_means[p] - ge_shares[p] for p in PartyCode}

    if reform_polling_correction_pp != 0.0:
        swing[PartyCode.REFORM] += reform_polling_correction_pp

    logger.debug(
        "Swing computed: as_of=%s geography=%s n_polls=%d correction=%+.2f swings=%s",
        as_of, geography, len(window), reform_polling_correction_pp,
        {p.value: round(v, 2) for p, v in swing.items()},
    )
    return swing
