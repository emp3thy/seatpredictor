from datetime import date
import pandas as pd
import pytest
from prediction_engine.polls import compute_swing, ge2024_national_share
from schema.common import PartyCode


def _polls_df_simple() -> pd.DataFrame:
    # Two GB polls with DIFFERENT numbers so the window-filter test below
    # distinguishes a 1-poll window from a 2-poll window.
    # Old poll (04-18): reform=20, lab=30. New poll (04-23): reform=30, lab=26.
    # Mean of both: reform=25, lab=28. New-poll-only mean: reform=30, lab=26.
    return pd.DataFrame([
        {"pollster": "X", "fieldwork_start": "2026-04-15", "fieldwork_end": "2026-04-17",
         "published_date": "2026-04-18", "sample_size": 1000, "geography": "GB",
         "con": 22.0, "lab": 30.0, "ld": 12.0, "reform": 20.0, "green": 8.0, "snp": 3.0, "plaid": 1.0, "other": 4.0},
        {"pollster": "Y", "fieldwork_start": "2026-04-20", "fieldwork_end": "2026-04-22",
         "published_date": "2026-04-23", "sample_size": 1000, "geography": "GB",
         "con": 18.0, "lab": 26.0, "ld": 12.0, "reform": 30.0, "green": 8.0, "snp": 3.0, "plaid": 1.0, "other": 2.0},
    ])


def _results_2024_df() -> pd.DataFrame:
    # Two seats x 8 parties; total votes 100 with party splits chosen so vote-weighted national share is round.
    rows = []
    for ons in ("S1", "S2"):
        rows.extend([
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "con",    "votes": 20, "share": 20.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "lab",    "votes": 30, "share": 30.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "ld",     "votes": 10, "share": 10.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "reform", "votes": 15, "share": 15.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "green",  "votes":  5, "share":  5.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "snp",    "votes":  3, "share":  3.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "plaid",  "votes":  1, "share":  1.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "other",  "votes": 16, "share": 16.0},
        ])
    return pd.DataFrame(rows)


def test_ge2024_national_share_vote_weighted():
    shares = ge2024_national_share(_results_2024_df(), nations=None)
    assert shares[PartyCode.CON]    == pytest.approx(20.0)
    assert shares[PartyCode.LAB]    == pytest.approx(30.0)
    assert shares[PartyCode.REFORM] == pytest.approx(15.0)


def test_compute_swing_gb_baseline_excludes_northern_ireland():
    """GB polls cover England + Scotland + Wales only; the GE-2024 baseline must
    exclude Northern Ireland's seats. NI's votes (mostly NI-specific parties bucketed
    as `other`) would dilute every other party's GB baseline if included.

    Construct a frame with England-only data (lab=30%) plus a large NI seat where
    100% of votes are coded as `other`. With NI-included baseline, lab share gets
    diluted; with NI-excluded (correct), lab share is the GB-only 30% and the GB
    swing computes against that.
    """
    england = _results_2024_df()  # 2 England seats, 200 total votes, lab=60 of those
    ni = pd.DataFrame([
        {"ons_code": "NI1", "constituency_name": "NI1", "region": "NI",
         "nation": "northern_ireland", "party": "other", "votes": 1000, "share": 100.0},
    ])
    results = pd.concat([england, ni], ignore_index=True)

    # GB nations: England-only baseline → lab share = 60/200 = 30%.
    gb_shares = ge2024_national_share(results, nations=("england", "scotland", "wales"))
    assert gb_shares[PartyCode.LAB] == pytest.approx(30.0)

    # All-nations: lab share gets diluted by NI's 1000 votes (none lab) → 60/1200 = 5%.
    all_shares = ge2024_national_share(results, nations=None)
    assert all_shares[PartyCode.LAB] == pytest.approx(60.0 / 1200.0 * 100.0)
    assert all_shares[PartyCode.LAB] != pytest.approx(30.0)


def test_compute_swing_subtracts_ge2024_share():
    polls = _polls_df_simple()
    results = _results_2024_df()
    swing = compute_swing(polls, results, as_of=date(2026, 4, 25), window_days=14, geography="GB")
    # Two-poll mean: reform=(20+30)/2=25; ge2024 reform=15 -> swing=+10.
    assert swing[PartyCode.REFORM] == pytest.approx(10.0)
    # Two-poll mean lab=(30+26)/2=28; ge2024 lab=30 -> swing=-2.
    assert swing[PartyCode.LAB]    == pytest.approx(-2.0)
    # Two-poll mean con=(22+18)/2=20; ge2024 con=20 -> swing=0.
    assert swing[PartyCode.CON]    == pytest.approx(0.0)


def test_compute_swing_window_excludes_old_poll():
    polls = _polls_df_simple()
    results = _results_2024_df()
    # window_days=3 from 2026-04-25 -> cutoff_lo=2026-04-22 (exclusive).
    # 04-18 poll FAILS (<= cutoff_lo); 04-23 poll PASSES.
    # New-poll-only: reform=30, ge2024=15 -> swing=+15. Distinct from the 2-poll mean.
    swing = compute_swing(polls, results, as_of=date(2026, 4, 25), window_days=3, geography="GB")
    assert swing[PartyCode.REFORM] == pytest.approx(15.0)
    # New-poll-only lab=26 vs ge2024=30 -> swing=-4 (vs -2 in the 2-poll case).
    assert swing[PartyCode.LAB] == pytest.approx(-4.0)


def test_compute_swing_wide_window_includes_both_polls():
    polls = _polls_df_simple()
    results = _results_2024_df()
    # window_days=14 includes both polls; mean of (20, 30) = 25; swing = 25 - 15 = 10.
    swing = compute_swing(polls, results, as_of=date(2026, 4, 25), window_days=14, geography="GB")
    assert swing[PartyCode.REFORM] == pytest.approx(10.0)


def test_compute_swing_raises_when_no_polls_in_window():
    polls = _polls_df_simple()
    results = _results_2024_df()
    with pytest.raises(ValueError, match="no polls in window"):
        compute_swing(polls, results, as_of=date(2024, 1, 1), window_days=14, geography="GB")


def test_compute_swing_filters_geography():
    polls = _polls_df_simple()
    results = _results_2024_df()
    with pytest.raises(ValueError, match="no polls in window"):
        compute_swing(polls, results, as_of=date(2026, 4, 25), window_days=14, geography="Wales")


def test_compute_swing_default_correction_is_no_op():
    """Default reform_polling_correction_pp=0.0 produces the same swing as before."""
    polls = _polls_df_simple()
    results = _results_2024_df()
    swing_no_arg = compute_swing(polls, results, as_of=date(2026, 4, 25),
                                  window_days=14, geography="GB")
    swing_zero = compute_swing(polls, results, as_of=date(2026, 4, 25),
                                window_days=14, geography="GB",
                                reform_polling_correction_pp=0.0)
    for p in PartyCode:
        assert swing_no_arg[p] == pytest.approx(swing_zero[p])


def test_compute_swing_positive_correction_bumps_reform():
    """+2.5 correction adds 2.5 to Reform's swing; other parties unchanged."""
    polls = _polls_df_simple()
    results = _results_2024_df()
    swing_base = compute_swing(polls, results, as_of=date(2026, 4, 25),
                                window_days=14, geography="GB")
    swing_corr = compute_swing(polls, results, as_of=date(2026, 4, 25),
                                window_days=14, geography="GB",
                                reform_polling_correction_pp=2.5)
    assert swing_corr[PartyCode.REFORM] == pytest.approx(swing_base[PartyCode.REFORM] + 2.5)
    # All other parties' swings unchanged in compute_swing — the per-seat
    # renormalisation in project_raw_shares handles the redistribution.
    for p in PartyCode:
        if p is PartyCode.REFORM:
            continue
        assert swing_corr[p] == pytest.approx(swing_base[p])


def test_compute_swing_negative_correction_pulls_reform_down():
    """Negative correction subtracts from Reform's swing."""
    polls = _polls_df_simple()
    results = _results_2024_df()
    swing_base = compute_swing(polls, results, as_of=date(2026, 4, 25),
                                window_days=14, geography="GB")
    swing_corr = compute_swing(polls, results, as_of=date(2026, 4, 25),
                                window_days=14, geography="GB",
                                reform_polling_correction_pp=-1.5)
    assert swing_corr[PartyCode.REFORM] == pytest.approx(swing_base[PartyCode.REFORM] - 1.5)
