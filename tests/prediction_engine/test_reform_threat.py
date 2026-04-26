import pytest
from prediction_engine.strategies.reform_threat_consolidation import (
    identify_consolidator,
    compute_clarity,
    apply_flows,
)
from schema.common import PartyCode


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


def test_compute_clarity_full_when_consolidator_is_only_left_bloc_party():
    """Edge case: in NI the LEFT_BLOC is empty, but other configurations could leave
    the consolidator as the only left-bloc party present. With no rivals to compare
    against, clarity is treated as 1.0 — the consolidation is trivially unambiguous."""
    # Wales LEFT_BLOC = {lab, ld, green, plaid}. With only Plaid having any share,
    # next_highest is 0, gap = full plaid share → clarity clamped to 1.0.
    shares = _shares(reform=30.0, plaid=20.0)
    clarity = compute_clarity(shares, consolidator=PartyCode.PLAID, nation="wales", threshold=5.0)
    assert clarity == pytest.approx(1.0)


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
