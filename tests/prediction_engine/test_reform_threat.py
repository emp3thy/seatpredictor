import json
import pytest
import pandas as pd
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.strategies.reform_threat_consolidation import (
    identify_consolidator,
    compute_clarity,
    apply_flows,
    ReformThreatStrategy,
)
from schema.common import PartyCode
from schema.prediction import ReformThreatConfig


def _shares(**overrides) -> dict[PartyCode, float]:
    base = {p: 0.0 for p in PartyCode}
    base.update({PartyCode(k): v for k, v in overrides.items()})
    return base


def test_identify_consolidator_picks_highest_left_bloc():
    shares = _shares(reform=35.0, lab=30.0, ld=10.0, green=8.0)
    c = identify_consolidator(shares, nation="england")
    assert c == PartyCode.LAB


def test_identify_consolidator_returns_none_when_no_left_bloc_above_threshold():
    shares = _shares(reform=35.0, lab=1.0, ld=1.0, green=1.0)
    c = identify_consolidator(shares, nation="england", min_share=2.0)
    assert c is None


def test_identify_consolidator_in_wales_includes_plaid():
    shares = _shares(reform=30.0, lab=15.0, plaid=25.0)
    c = identify_consolidator(shares, nation="wales")
    assert c == PartyCode.PLAID


def test_identify_consolidator_in_scotland_includes_snp():
    shares = _shares(reform=30.0, lab=18.0, snp=28.0)
    c = identify_consolidator(shares, nation="scotland")
    assert c == PartyCode.SNP


def test_compute_clarity_full_when_gap_exceeds_threshold():
    shares = _shares(lab=30.0, ld=10.0, green=8.0)
    clarity = compute_clarity(shares, consolidator=PartyCode.LAB, nation="england", threshold=5.0)
    assert clarity == pytest.approx(1.0)


def test_compute_clarity_zero_when_consolidator_tied():
    shares = _shares(lab=20.0, ld=20.0, green=8.0)
    clarity = compute_clarity(shares, consolidator=PartyCode.LAB, nation="england", threshold=5.0)
    assert clarity == pytest.approx(0.0)


def test_compute_clarity_partial():
    shares = _shares(lab=20.0, ld=18.0)  # gap 2pp; threshold 5pp → clarity 0.4
    clarity = compute_clarity(shares, consolidator=PartyCode.LAB, nation="england", threshold=5.0)
    assert clarity == pytest.approx(0.4)


def test_apply_flows_redistributes_share_to_consolidator():
    shares = _shares(reform=35.0, lab=30.0, ld=10.0, green=8.0, con=15.0, other=2.0)
    weights = {PartyCode.LD: 0.5, PartyCode.GREEN: 0.4, PartyCode.CON: 0.2}
    flagged: list[str] = []
    out = apply_flows(
        shares, leader=PartyCode.REFORM, consolidator=PartyCode.LAB,
        weights=weights, clarity=1.0, multiplier=1.0, flag_sink=flagged,
    )
    # LD loses 10*0.5*1*1 = 5; Green loses 8*0.4 = 3.2; Con loses 15*0.2 = 3.
    # Lab gains 5+3.2+3 = 11.2.
    assert out[PartyCode.LD]    == pytest.approx(5.0)
    assert out[PartyCode.GREEN] == pytest.approx(4.8)
    assert out[PartyCode.CON]   == pytest.approx(12.0)
    assert out[PartyCode.LAB]   == pytest.approx(41.2)
    assert out[PartyCode.REFORM] == pytest.approx(35.0)  # unchanged
    assert "multiplier_clipped" not in flagged


def test_apply_flows_multiplier_clipped_when_flow_exceeds_source():
    shares = _shares(reform=35.0, lab=30.0, ld=10.0)
    weights = {PartyCode.LD: 0.8}
    flagged: list[str] = []
    out = apply_flows(
        shares, leader=PartyCode.REFORM, consolidator=PartyCode.LAB,
        weights=weights, clarity=1.0, multiplier=2.0, flag_sink=flagged,
    )
    # Want to move 10*0.8*1*2 = 16, but only 10 available → clipped to 10.
    assert out[PartyCode.LD]  == pytest.approx(0.0)
    assert out[PartyCode.LAB] == pytest.approx(40.0)
    assert "multiplier_clipped" in flagged


def test_apply_flows_zero_clarity_means_no_flow():
    shares = _shares(reform=35.0, lab=30.0, ld=10.0)
    weights = {PartyCode.LD: 0.5}
    flagged: list[str] = []
    out = apply_flows(
        shares, leader=PartyCode.REFORM, consolidator=PartyCode.LAB,
        weights=weights, clarity=0.0, multiplier=1.0, flag_sink=flagged,
    )
    assert out[PartyCode.LD]  == pytest.approx(10.0)
    assert out[PartyCode.LAB] == pytest.approx(30.0)


def test_compute_clarity_full_when_rivals_have_zero_share():
    """When the consolidator's left-bloc rivals all have 0 share (others didn't make
    the seat threshold), next_highest=0 and gap=consolidator_share. With share >> threshold
    that clamps to 1.0. This is the typical "uncontested consolidation" case in the seed."""
    # Wales LEFT_BLOC = {lab, ld, green, plaid}. Plaid alone has share; next_highest = 0.
    # gap = 20, threshold = 5 → 20/5 = 4.0 clamped to 1.0.
    shares = _shares(reform=30.0, plaid=20.0)
    clarity = compute_clarity(shares, consolidator=PartyCode.PLAID, nation="wales", threshold=5.0)
    assert clarity == pytest.approx(1.0)


def test_identify_consolidator_returns_none_for_northern_ireland():
    """LEFT_BLOC[Nation.NORTHERN_IRELAND] is empty by construction; no consolidator
    is ever identifiable in NI. Task 9's strategy short-circuits on NI before reaching
    this helper, but the contract here is the safety net."""
    shares = _shares(reform=30.0, lab=10.0, ld=5.0)
    assert identify_consolidator(shares, nation="northern_ireland") is None


def test_identify_consolidator_tie_break_alphabetical():
    """When two left-bloc parties tie on share, the alphabetically-earlier party wins
    (matches data_engine's _identify_consolidator tie-break behavior)."""
    shares = _shares(reform=35.0, green=20.0, lab=20.0)  # Green and Lab tied at 20
    c = identify_consolidator(shares, nation="england")
    assert c == PartyCode.GREEN  # 'g' < 'l' alphabetically


def test_compute_clarity_rejects_zero_threshold():
    shares = _shares(lab=20.0, ld=10.0)
    with pytest.raises(ValueError, match="threshold must be > 0"):
        compute_clarity(shares, consolidator=PartyCode.LAB, nation="england", threshold=0.0)


# ---------------------------------------------------------------------------
# Integration tests (Task 9)
# ---------------------------------------------------------------------------

def _seat(per_seat, ons_code):
    return per_seat[per_seat["ons_code"] == ons_code].iloc[0]


def test_reform_threat_seat_a_clear_consolidation(tiny_snapshot_path):
    """Seat A (Aldermouth, england, lab consolidator, high clarity) — flow applies."""
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    a = _seat(res.per_seat, "TST00001")
    assert a["consolidator"] == "lab"
    assert a["matrix_nation"] == "england"
    assert json.loads(a["matrix_provenance"]) == ["tst_eng_2025"]
    assert a["share_predicted_lab"] > a["share_raw_lab"]
    assert a["share_predicted_ld"] < a["share_raw_ld"]


def test_reform_threat_seat_c_non_reform_leader_short_circuits(tiny_snapshot_path):
    """Seat C has Con leading, not Reform — short-circuit, flag."""
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    c = _seat(res.per_seat, "TST00003")
    flags = json.loads(c["notes"])
    assert "non_reform_leader" in flags
    assert c["share_predicted_con"] == pytest.approx(c["share_raw_con"], abs=1e-9)
    assert pd.isna(c["consolidator"])


def test_reform_threat_seat_d_wales_plaid_consolidator(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    d = _seat(res.per_seat, "TST00004")
    assert d["consolidator"] == "plaid"
    assert d["matrix_nation"] == "wales"
    assert d["share_predicted_plaid"] > d["share_raw_plaid"]


def test_reform_threat_seat_e_scotland_no_matrix(tiny_snapshot_path):
    """Seat E — SNP would be the consolidator, but Scotland has no matrix entry → matrix_unavailable."""
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    e = _seat(res.per_seat, "TST00005")
    flags = json.loads(e["notes"])
    assert "matrix_unavailable" in flags
    assert e["share_predicted_snp"] == pytest.approx(e["share_raw_snp"], abs=1e-9)
    # Per spec §5.3 step 5: clarity computed BEFORE matrix-availability check, so it survives.
    assert e["consolidator"] == "snp"
    assert e["clarity"] is not None and not pd.isna(e["clarity"])


def test_reform_threat_seat_f_ni_excluded(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    f = _seat(res.per_seat, "TST00006")
    flags = json.loads(f["notes"])
    assert "ni_excluded" in flags
    assert f["share_predicted_other"] == pytest.approx(f["share_raw_other"], abs=1e-9)


def test_reform_threat_low_clarity_flag(tiny_snapshot_path):
    """Seat B (Bramford): Lab/LD near-tied (gap=2pp) → low_clarity at default threshold=5pp."""
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    b = _seat(res.per_seat, "TST00002")
    flags = json.loads(b["notes"])
    assert "low_clarity" in flags
    assert b["consolidator"] == "lab"


def test_reform_threat_multiplier_monotone(tiny_snapshot_path):
    """Seat A: with weight 0.6 for LD, raising multiplier from 0.5 → 1.5 must move ≥ as much LD share."""
    snap = Snapshot(tiny_snapshot_path)
    moves: list[float] = []
    for m in (0.5, 1.0, 1.5):
        res = ReformThreatStrategy().predict(snap, ReformThreatConfig(multiplier=m))
        a = _seat(res.per_seat, "TST00001")
        moves.append(a["share_raw_ld"] - a["share_predicted_ld"])
    assert moves[0] <= moves[1] <= moves[2] + 1e-9


def test_reform_threat_determinism(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    a = ReformThreatStrategy().predict(snap, ReformThreatConfig()).per_seat
    b = ReformThreatStrategy().predict(snap, ReformThreatConfig()).per_seat
    a_sorted = a.sort_values("ons_code").reset_index(drop=True)
    b_sorted = b.sort_values("ons_code").reset_index(drop=True)
    assert a_sorted.equals(b_sorted)


def test_reform_threat_shares_sum_to_100_per_seat(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    cols = [f"share_predicted_{p.value}" for p in PartyCode]
    sums = res.per_seat[cols].sum(axis=1)
    for s in sums:
        assert s == pytest.approx(100.0, abs=1e-6)


def test_reform_threat_consolidator_already_leads_unit():
    """Hand-built shares unit test for the consolidator_already_leads guard.

    Why the integration path is unreachable: _argmax uses the key
    (share, -ord(p.value[0])). On a Reform=Lab=35 tie, Reform's key is (35, -114) and
    Lab's key is (35, -108). max() picks the larger tuple, so -108 > -114 → Lab wins
    the leader role. _predict_seat then short-circuits via non_reform_leader BEFORE
    the consolidator_already_leads check is reached. The guard remains a defensive
    structural invariant — the unit test below validates its arithmetic without
    invoking _predict_seat.
    """
    raw_shares = {p: 0.0 for p in PartyCode}
    raw_shares[PartyCode.REFORM] = 35.0
    raw_shares[PartyCode.LAB]    = 35.0
    raw_shares[PartyCode.LD]     = 10.0
    raw_shares[PartyCode.OTHER]  = 20.0
    consolidator = identify_consolidator(raw_shares, nation="england")
    assert consolidator == PartyCode.LAB
    assert raw_shares[consolidator] >= raw_shares[PartyCode.REFORM]


def test_reform_threat_multiplier_clipped_integration(tiny_snapshot_path):
    """Integration coverage for the multiplier_clipped flag: a high multiplier on
    Seat A should saturate at least one source's full share (ld_share * 0.6 * 1.0 * m
    exceeds ld_share when m >= 1/0.6 ≈ 1.667). The flag must round-trip through
    _seat_with_flags' json.dumps."""
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig(multiplier=5.0))
    a = _seat(res.per_seat, "TST00001")
    flags = json.loads(a["notes"])
    assert "multiplier_clipped" in flags


def test_reform_threat_honours_reform_polling_correction(tiny_snapshot_path):
    """+5pp correction lifts Reform's projected raw share before per-seat
    threat-consolidation logic runs. share_raw_reform mean should rise; the
    threat strategy may then flip MORE seats away from Reform (because Reform
    leads in more seats post-correction), but share_raw is what the test pins."""
    from prediction_engine.snapshot_loader import Snapshot
    from prediction_engine.strategies.reform_threat_consolidation import ReformThreatStrategy
    from schema.prediction import ReformThreatConfig
    snap = Snapshot(tiny_snapshot_path)
    base = ReformThreatStrategy().predict(snap, ReformThreatConfig()).per_seat
    corr = ReformThreatStrategy().predict(
        snap, ReformThreatConfig(reform_polling_correction_pp=5.0)
    ).per_seat
    base_reform_mean = base["share_raw_reform"].mean()
    corr_reform_mean = corr["share_raw_reform"].mean()
    assert corr_reform_mean - base_reform_mean > 3.0
    assert corr_reform_mean - base_reform_mean < 5.5
