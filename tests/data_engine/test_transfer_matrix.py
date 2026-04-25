import pandas as pd
import pytest
from data_engine.transforms.transfer_matrix import (
    derive_transfer_matrix,
    PRIOR_SHARE_THRESHOLD,
)
from schema.common import PartyCode, Nation


def _fake_byelections() -> tuple[pd.DataFrame, pd.DataFrame]:
    # One Welsh event: Caerphilly-style. Reform threat. Plaid consolidator (+19pp).
    # Lab fell from 46 → 11 (flow rate 35/46 ≈ 0.761).
    # LD fell from 2.4 → 1.2 (1.2/2.4 = 0.5).
    # Con fell from 17.3 → 2.5 (14.8/17.3 ≈ 0.855).
    # Green prior 1.3% — below threshold, excluded.
    events = pd.DataFrame([{
        "event_id": "caer_test",
        "name": "Caer test",
        "date": "2025-10-23",
        "event_type": "senedd",
        "nation": "wales",
        "region": "X",
        "threat_party": "reform",
        "exclude_from_matrix": False,
        "narrative_url": None,
    }])
    results = pd.DataFrame([
        {"event_id": "caer_test", "party": "plaid",  "votes": 0, "actual_share": 47.4, "prior_share": 28.4},
        {"event_id": "caer_test", "party": "reform", "votes": 0, "actual_share": 36.0, "prior_share":  1.7},
        {"event_id": "caer_test", "party": "lab",    "votes": 0, "actual_share": 11.0, "prior_share": 46.0},
        {"event_id": "caer_test", "party": "con",    "votes": 0, "actual_share":  2.5, "prior_share": 17.3},
        {"event_id": "caer_test", "party": "ld",     "votes": 0, "actual_share":  1.2, "prior_share":  2.4},
        {"event_id": "caer_test", "party": "green",  "votes": 0, "actual_share":  1.0, "prior_share":  1.3},
    ])
    return events, results


def test_derives_consolidator_from_biggest_left_bloc_gainer():
    events, results = _fake_byelections()
    cells, prov = derive_transfer_matrix(events, results)
    assert (cells["consolidator"] == "plaid").all()


def test_lab_to_plaid_flow_rate():
    events, results = _fake_byelections()
    cells, _ = derive_transfer_matrix(events, results)
    lab_row = cells[(cells["consolidator"] == "plaid") & (cells["source"] == "lab")].iloc[0]
    expected = (46.0 - 11.0) / 46.0
    assert abs(lab_row["weight"] - expected) < 1e-6
    assert lab_row["nation"] == "wales"
    assert lab_row["n"] == 1


def test_below_threshold_source_excluded():
    events, results = _fake_byelections()
    cells, _ = derive_transfer_matrix(events, results)
    # Green's prior 1.3% < threshold (2%) → no row for green-as-source
    green_rows = cells[cells["source"] == "green"]
    assert len(green_rows) == 0


def test_provenance_links_cell_to_event():
    events, results = _fake_byelections()
    _, prov = derive_transfer_matrix(events, results)
    plaid_prov = prov[(prov["nation"] == "wales") & (prov["consolidator"] == "plaid")]
    assert "caer_test" in set(plaid_prov["event_id"])


def test_event_excluded_when_excluded_flag_true():
    events, results = _fake_byelections()
    events.loc[0, "exclude_from_matrix"] = True
    cells, _ = derive_transfer_matrix(events, results)
    assert len(cells) == 0


def test_event_excluded_when_threat_not_reform():
    events, results = _fake_byelections()
    events.loc[0, "threat_party"] = "con"
    cells, _ = derive_transfer_matrix(events, results)
    assert len(cells) == 0


def test_two_events_average():
    events, results = _fake_byelections()
    # Add a second English event with Lab as consolidator and Green→Lab observed flow.
    events2 = pd.DataFrame([{
        "event_id": "ev2",
        "name": "ev2",
        "date": "2026-02-26",
        "event_type": "westminster_byelection",
        "nation": "england",
        "region": "X",
        "threat_party": "reform",
        "exclude_from_matrix": False,
        "narrative_url": None,
    }])
    results2 = pd.DataFrame([
        {"event_id": "ev2", "party": "lab",    "votes": 0, "actual_share": 50.0, "prior_share": 30.0},
        {"event_id": "ev2", "party": "reform", "votes": 0, "actual_share": 30.0, "prior_share": 10.0},
        {"event_id": "ev2", "party": "green",  "votes": 0, "actual_share":  5.0, "prior_share": 20.0},
        {"event_id": "ev2", "party": "ld",     "votes": 0, "actual_share":  5.0, "prior_share":  20.0},
        {"event_id": "ev2", "party": "con",    "votes": 0, "actual_share": 10.0, "prior_share": 20.0},
    ])
    events_all = pd.concat([events, events2], ignore_index=True)
    results_all = pd.concat([results, results2], ignore_index=True)
    cells, _ = derive_transfer_matrix(events_all, results_all)
    england_lab = cells[(cells["nation"] == "england") & (cells["consolidator"] == "lab")]
    assert set(england_lab["source"]) == {"green", "ld", "con"}
