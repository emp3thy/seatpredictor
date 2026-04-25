import logging
from datetime import date, timedelta

import pandas as pd
from schema.common import PartyCode

logger = logging.getLogger(__name__)


_PARTY_VALUES: list[str] = [p.value for p in PartyCode]


def ge2024_national_share(
    results_2024: pd.DataFrame,
    nation_filter: str | None = None,
) -> dict[PartyCode, float]:
    """Vote-weighted national share per party from the 2024 GE results.

    nation_filter: if given (e.g. 'wales'), restrict to that nation; else GB-wide.
    """
    df = results_2024
    if nation_filter is not None:
        df = df[df["nation"] == nation_filter]
    if df.empty:
        raise ValueError(f"no results for nation_filter={nation_filter}")

    by_party = df.groupby("party", as_index=False)["votes"].sum()
    total = float(by_party["votes"].sum())
    if total <= 0:
        raise ValueError("results_2024 votes sum to 0")

    shares: dict[PartyCode, float] = {}
    for p in PartyCode:
        row = by_party[by_party["party"] == p.value]
        v = float(row["votes"].iloc[0]) if not row.empty else 0.0
        shares[p] = (v / total) * 100.0
    return shares


_GEO_TO_NATION_FILTER: dict[str, str | None] = {
    "GB": None,
    "Scotland": "scotland",
    "Wales": "wales",
    "London": None,  # no London-only filter on results_2024 in v1
}


def compute_swing(
    polls: pd.DataFrame,
    results_2024: pd.DataFrame,
    as_of: date,
    window_days: int,
    geography: str,
) -> dict[PartyCode, float]:
    """Average per-party poll share over the window, then subtract GE 2024 share.

    Window: published_date in (as_of - window_days, as_of].
    Failures (no polls match) raise ValueError per spec §8.
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
    window = polls[filt]
    if window.empty:
        raise ValueError(
            f"no polls in window: geography={geography} as_of={as_of} window_days={window_days}"
        )

    poll_means = {p: float(window[p.value].mean()) for p in PartyCode}
    ge_shares = ge2024_national_share(results_2024, nation_filter=_GEO_TO_NATION_FILTER[geography])
    swing = {p: poll_means[p] - ge_shares[p] for p in PartyCode}

    logger.info(
        "Swing computed: as_of=%s geography=%s n_polls=%d swings=%s",
        as_of, geography, len(window),
        {p.value: round(v, 2) for p, v in swing.items()},
    )
    return swing
