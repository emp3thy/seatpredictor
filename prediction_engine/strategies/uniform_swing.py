import logging
from dataclasses import dataclass

import pandas as pd

from prediction_engine.polls import compute_swing
from prediction_engine.projection import project_raw_shares
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.strategies.base import Strategy, register
from schema.common import PartyCode
from schema.prediction import UniformSwingConfig

logger = logging.getLogger(__name__)


@dataclass
class PredictionResult:
    """Returned from Strategy.predict. Final SQLite serialisation lives in sqlite_io."""
    per_seat: pd.DataFrame
    national: pd.DataFrame
    run_metadata: dict


@register("uniform_swing")
class UniformSwingStrategy(Strategy):
    name = "uniform_swing"
    config_schema = UniformSwingConfig

    def predict(self, snapshot: Snapshot, scenario: UniformSwingConfig) -> PredictionResult:
        gb_swing = compute_swing(
            snapshot.polls,
            snapshot.results_2024,
            as_of=snapshot.manifest.as_of_date,
            window_days=scenario.polls_window_days,
            geography="GB",
        )
        # v1: Wales/Scotland fall back to GB-only swing per spec §11 open question.
        swings = {"GB": gb_swing}
        per_seat = project_raw_shares(snapshot.results_2024, swings)

        # share_predicted_<p> = share_raw_<p> for uniform-swing baseline.
        for p in PartyCode:
            per_seat[f"share_predicted_{p.value}"] = per_seat[f"share_raw_{p.value}"]

        per_seat = _add_winner_and_metadata(per_seat)
        per_seat = per_seat.sort_values(by="ons_code").reset_index(drop=True)

        national = _compute_national_totals(per_seat)

        logger.debug(
            "UniformSwingStrategy.predict: %d seats, snapshot=%s",
            len(per_seat),
            snapshot.snapshot_id,
        )

        return PredictionResult(
            per_seat=per_seat,
            national=national,
            run_metadata={
                "strategy": self.name,
                "scenario": scenario.model_dump(mode="json"),
                "snapshot_id": snapshot.snapshot_id,
            },
        )


def _add_winner_and_metadata(per_seat: pd.DataFrame) -> pd.DataFrame:
    """Compute predicted_winner, predicted_margin, leader. Set consolidator/clarity/matrix_*
    to null and notes to '[]' ONLY if those columns aren't already present (so the
    reform-threat strategy can populate them per-seat and call this helper without
    losing its work)."""
    party_cols = [f"share_predicted_{p.value}" for p in PartyCode]
    raw_cols   = [f"share_raw_{p.value}"       for p in PartyCode]

    winners = per_seat.loc[:, party_cols].idxmax(axis=1).str.replace("share_predicted_", "", regex=False)
    per_seat["predicted_winner"] = winners.values

    sorted_shares = per_seat.loc[:, party_cols].apply(
        lambda row: sorted(row.values, reverse=True), axis=1, result_type="expand"
    )
    per_seat["predicted_margin"] = sorted_shares.iloc[:, 0] - sorted_shares.iloc[:, 1]

    leaders = per_seat.loc[:, raw_cols].idxmax(axis=1).str.replace("share_raw_", "", regex=False)
    per_seat["leader"] = leaders.values

    # Only fill metadata columns if absent — reform_threat strategy populates them per-seat
    # before calling this helper.
    for col, default in (
        ("consolidator", None),
        ("clarity", None),
        ("matrix_nation", None),
        ("matrix_provenance", "[]"),
        ("notes", "[]"),
    ):
        if col not in per_seat.columns:
            per_seat[col] = default
    return per_seat


def _compute_national_totals(per_seat: pd.DataFrame) -> pd.DataFrame:
    """Long-format DataFrame: scope/scope_value/party/seats."""
    rows: list[dict] = []
    overall = per_seat["predicted_winner"].value_counts()
    for party, seats in overall.items():
        rows.append({"scope": "overall", "scope_value": "", "party": party, "seats": int(seats)})

    for nation in sorted(per_seat["nation"].dropna().unique()):
        sub = per_seat.loc[per_seat["nation"] == nation]
        for party, seats in sub["predicted_winner"].value_counts().items():
            rows.append({"scope": "nation", "scope_value": str(nation), "party": party, "seats": int(seats)})

    for region in sorted(per_seat["region"].dropna().unique()):
        sub = per_seat.loc[per_seat["region"] == region]
        for party, seats in sub["predicted_winner"].value_counts().items():
            rows.append({"scope": "region", "scope_value": str(region), "party": party, "seats": int(seats)})

    return pd.DataFrame(rows, columns=["scope", "scope_value", "party", "seats"])
