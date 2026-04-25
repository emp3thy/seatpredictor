import pandas as pd
import pytest
from prediction_engine.projection import project_raw_shares
from schema.common import PartyCode


def _two_seat_results() -> pd.DataFrame:
    return pd.DataFrame([
        {"ons_code": "E1", "constituency_name": "E1", "region": "R", "nation": "england",
         "party": p, "votes": 0, "share": s}
        for p, s in [("con", 30), ("lab", 30), ("ld", 10), ("reform", 10),
                     ("green", 5), ("snp", 0), ("plaid", 0), ("other", 15)]
    ] + [
        {"ons_code": "W1", "constituency_name": "W1", "region": "R", "nation": "wales",
         "party": p, "votes": 0, "share": s}
        for p, s in [("con", 10), ("lab", 35), ("ld", 5), ("reform", 15),
                     ("green", 5), ("snp", 0), ("plaid", 25), ("other", 5)]
    ])


def test_project_raw_shares_applies_gb_swing_to_england():
    results = _two_seat_results()
    swings = {
        "GB": {p: 0.0 for p in PartyCode},
    }
    swings["GB"][PartyCode.REFORM] = 10.0
    swings["GB"][PartyCode.LAB] = -5.0
    out = project_raw_shares(results, swings)
    e1 = out[out["ons_code"] == "E1"].iloc[0]
    # Reform was 10 + 10 = 20; Lab 30 − 5 = 25; total before renorm = 100 + 5 = 105 → renormalise.
    assert e1["share_raw_reform"] == pytest.approx(20.0 * 100.0 / 105.0, abs=1e-6)
    assert e1["share_raw_lab"]    == pytest.approx(25.0 * 100.0 / 105.0, abs=1e-6)
    # Sum to 100.
    cols = [f"share_raw_{p.value}" for p in PartyCode]
    assert e1[cols].sum() == pytest.approx(100.0, abs=1e-6)


def test_project_raw_shares_uses_wales_swing_when_present():
    results = _two_seat_results()
    swings = {
        "GB":    {p: 0.0 for p in PartyCode},
        "Wales": {p: 0.0 for p in PartyCode},
    }
    swings["GB"][PartyCode.REFORM]    = 10.0
    swings["Wales"][PartyCode.REFORM] = 5.0  # smaller Welsh swing
    out = project_raw_shares(results, swings)
    w1 = out[out["ons_code"] == "W1"].iloc[0]
    # Wales should use Wales swing (+5), not GB (+10).
    expected_pre = 15.0 + 5.0  # 20
    expected_total_before_renorm = 100.0 + 5.0
    assert w1["share_raw_reform"] == pytest.approx(expected_pre * 100.0 / expected_total_before_renorm, abs=1e-6)


def test_project_raw_shares_clamps_negative_to_zero():
    results = _two_seat_results()
    swings = {"GB": {p: 0.0 for p in PartyCode}}
    swings["GB"][PartyCode.REFORM] = -50.0  # would drive reform negative
    out = project_raw_shares(results, swings)
    cols = [f"share_raw_{p.value}" for p in PartyCode]
    # Every seat (both E1 and W1 — Wales falls back to GB swing here) renormalises
    # to exactly 100.0 after the clamp.
    for ons in ("E1", "W1"):
        seat = out[out["ons_code"] == ons].iloc[0]
        assert seat["share_raw_reform"] == pytest.approx(0.0, abs=1e-6)
        assert seat[cols].sum() == pytest.approx(100.0, abs=1e-6)


def test_project_raw_shares_all_seats_sum_to_100():
    """Renormalisation guarantee: every seat's predicted shares sum to exactly 100,
    regardless of swing configuration."""
    results = _two_seat_results()
    swings = {
        "GB":    {p: 0.0 for p in PartyCode},
        "Wales": {p: 0.0 for p in PartyCode},
    }
    swings["GB"][PartyCode.REFORM]    = 7.0
    swings["GB"][PartyCode.LAB]       = -3.0
    swings["Wales"][PartyCode.REFORM] = 4.0
    swings["Wales"][PartyCode.LAB]    = -2.0
    out = project_raw_shares(results, swings)
    cols = [f"share_raw_{p.value}" for p in PartyCode]
    sums = out[cols].sum(axis=1)
    for total in sums:
        assert total == pytest.approx(100.0, abs=1e-6)


def test_project_raw_shares_preserves_identity_columns():
    results = _two_seat_results()
    swings = {"GB": {p: 0.0 for p in PartyCode}}
    out = project_raw_shares(results, swings)
    assert set(out.columns) >= {"ons_code", "constituency_name", "region", "nation"}
    assert len(out) == 2  # one row per seat (pivoted)
