import pytest
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.strategies.uniform_swing import UniformSwingStrategy
from schema.prediction import UniformSwingConfig
from schema.common import PartyCode


def test_uniform_swing_returns_per_seat_predictions(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    strat = UniformSwingStrategy()
    result = strat.predict(snap, UniformSwingConfig())
    df = result.per_seat
    assert len(df) == 6
    assert set(df.columns) >= {
        "ons_code", "constituency_name", "nation", "region",
        "share_2024_reform", "share_raw_reform", "share_predicted_reform",
        "predicted_winner", "predicted_margin",
        "leader", "consolidator", "clarity",
        "matrix_nation", "matrix_provenance", "notes",
    }


def test_uniform_swing_share_predicted_equals_share_raw(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    result = UniformSwingStrategy().predict(snap, UniformSwingConfig())
    for p in PartyCode:
        col_raw  = f"share_raw_{p.value}"
        col_pred = f"share_predicted_{p.value}"
        diffs = (result.per_seat[col_pred] - result.per_seat[col_raw]).abs()
        assert (diffs < 1e-9).all(), f"raw and predicted differ for {p.value}"


def test_uniform_swing_consolidator_and_clarity_are_null(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    result = UniformSwingStrategy().predict(snap, UniformSwingConfig())
    assert result.per_seat["consolidator"].isna().all()
    assert result.per_seat["clarity"].isna().all()


def test_uniform_swing_winner_is_max_predicted_share(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    result = UniformSwingStrategy().predict(snap, UniformSwingConfig())
    for _, row in result.per_seat.iterrows():
        share_cols = {p.value: row[f"share_predicted_{p.value}"] for p in PartyCode}
        winner = max(share_cols, key=lambda k: share_cols[k])
        assert row["predicted_winner"] == winner


def test_uniform_swing_national_totals_sum_to_seat_count(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    result = UniformSwingStrategy().predict(snap, UniformSwingConfig())
    overall = result.national[result.national["scope"] == "overall"]
    assert overall["seats"].sum() == len(result.per_seat)  # one winner per seat


def test_uniform_swing_per_nation_seat_counts_consistent(tiny_snapshot_path):
    """The per-nation breakdown's seat counts must sum (across parties, within each nation)
    to the number of seats in that nation. Catches bugs where _compute_national_totals
    double-counts or drops scopes."""
    snap = Snapshot(tiny_snapshot_path)
    result = UniformSwingStrategy().predict(snap, UniformSwingConfig())
    nation_view = result.national[result.national["scope"] == "nation"]
    for nation, sub in nation_view.groupby("scope_value"):
        seats_in_nation = (result.per_seat["nation"] == nation).sum()
        assert sub["seats"].sum() == seats_in_nation, f"{nation}: {sub['seats'].sum()} != {seats_in_nation}"


def test_uniform_swing_winner_tie_break_follows_partycode_order(tiny_snapshot_path):
    """Document and enforce tie-break behavior. The implementation uses pandas idxmax
    over party_cols = [share_predicted_<p> for p in PartyCode], which on a tie returns
    the FIRST column with the max — i.e. PartyCode declaration order. PartyCode order is
    LAB, CON, LD, REFORM, GREEN, SNP, PLAID, OTHER (from schema/common.py); so on a Lab/Reform
    tie, Lab wins.

    This test injects a synthetic Lab/Reform tie and asserts Lab wins.
    """
    import pandas as pd
    snap = Snapshot(tiny_snapshot_path)
    # Run predict, then synthesize a tie post-hoc by manually invoking the helper.
    # Simpler: assert ordering via construction with hand-built shares.
    from prediction_engine.strategies.uniform_swing import _add_winner_and_metadata
    row = {f"share_raw_{p.value}":       0.0 for p in PartyCode}
    row.update({f"share_predicted_{p.value}": 0.0 for p in PartyCode})
    row["share_predicted_lab"]    = 35.0
    row["share_predicted_reform"] = 35.0
    row["share_raw_lab"]    = 35.0
    row["share_raw_reform"] = 35.0
    df = pd.DataFrame([row])
    out = _add_winner_and_metadata(df.copy())
    assert out.iloc[0]["predicted_winner"] == "lab"  # Lab wins because it precedes Reform in PartyCode order


def test_uniform_swing_determinism(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    a = UniformSwingStrategy().predict(snap, UniformSwingConfig()).per_seat
    b = UniformSwingStrategy().predict(snap, UniformSwingConfig()).per_seat
    # Sort identically; row-set equality.
    a_sorted = a.sort_values("ons_code").reset_index(drop=True)
    b_sorted = b.sort_values("ons_code").reset_index(drop=True)
    assert a_sorted.equals(b_sorted)
