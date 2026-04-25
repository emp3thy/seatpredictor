import logging

import pandas as pd
from schema.common import PartyCode

logger = logging.getLogger(__name__)


def _pick_swing_for_nation(
    swings: dict[str, dict[PartyCode, float]], nation: str
) -> dict[PartyCode, float]:
    """Wales seats use Wales swing if present; Scotland likewise; else GB."""
    if nation == "wales" and "Wales" in swings:
        return swings["Wales"]
    if nation == "scotland" and "Scotland" in swings:
        return swings["Scotland"]
    return swings["GB"]


def project_raw_shares(
    results_2024: pd.DataFrame,
    swings: dict[str, dict[PartyCode, float]],
) -> pd.DataFrame:
    """Pivot 2024 results to wide form, apply per-party swing, clamp negatives,
    re-normalise to 100. Returns one row per seat with columns:

        ons_code, constituency_name, region, nation,
        share_2024_<party> (8), share_raw_<party> (8)
    """
    if "GB" not in swings:
        raise ValueError("swings dict must contain 'GB' fallback")

    wide = results_2024.pivot_table(
        index=["ons_code", "constituency_name", "region", "nation"],
        columns="party",
        values="share",
        aggfunc="first",
        fill_value=0.0,
    ).reset_index()
    # Normalise column order — guarantee every party column exists, even at 0%.
    for p in PartyCode:
        if p.value not in wide.columns:
            wide[p.value] = 0.0

    # Apply swing per row, vectorised per nation.
    for nation in wide["nation"].unique():
        mask = wide["nation"] == nation
        swing_for = _pick_swing_for_nation(swings, str(nation))
        for p in PartyCode:
            wide.loc[mask, f"_post_{p.value}"] = (
                wide.loc[mask, p.value] + swing_for[p]
            ).clip(lower=0.0)

    # Re-normalise post-swing shares to sum to 100 per seat.
    post_cols = [f"_post_{p.value}" for p in PartyCode]
    totals = wide[post_cols].sum(axis=1)
    if (totals <= 0).any():
        raise ValueError("post-swing shares non-positive for at least one seat")
    for p in PartyCode:
        wide[f"share_raw_{p.value}"] = wide[f"_post_{p.value}"] * 100.0 / totals

    # Build share_2024_<p> from the pivoted source columns.
    for p in PartyCode:
        wide[f"share_2024_{p.value}"] = wide[p.value]

    keep = (
        ["ons_code", "constituency_name", "region", "nation"]
        + [f"share_2024_{p.value}" for p in PartyCode]
        + [f"share_raw_{p.value}"  for p in PartyCode]
    )

    logger.debug(
        "project_raw_shares: %d seats processed, geographies=%s",
        len(wide),
        list(wide["nation"].unique()),
    )

    return wide[keep].sort_values("ons_code").reset_index(drop=True)
