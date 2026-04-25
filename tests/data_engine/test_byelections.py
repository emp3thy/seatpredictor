from datetime import date
from pathlib import Path
import pytest
from data_engine.sources.byelections import load_byelections
from schema.byelection import EventType
from schema.common import Nation, PartyCode


def test_loads_committed_yaml():
    events_df, results_df = load_byelections(Path("data/hand_curated/by_elections.yaml"))
    # 4 events as of plan date
    assert len(events_df) >= 4
    expected_ids = {
        "runcorn_helsby_2025",
        "hamilton_larkhall_stonehouse_2025",
        "caerphilly_senedd_2025",
        "gorton_denton_2026",
    }
    assert expected_ids <= set(events_df["event_id"])


def test_caerphilly_threat_is_reform():
    events_df, _ = load_byelections(Path("data/hand_curated/by_elections.yaml"))
    row = events_df[events_df["event_id"] == "caerphilly_senedd_2025"].iloc[0]
    assert row["threat_party"] == PartyCode.REFORM.value
    assert row["nation"] == Nation.WALES.value
    assert row["event_type"] == EventType.SENEDD.value


def test_results_per_event_present():
    _, results_df = load_byelections(Path("data/hand_curated/by_elections.yaml"))
    caerphilly = results_df[results_df["event_id"] == "caerphilly_senedd_2025"]
    assert len(caerphilly) >= 6  # plaid, reform, lab, con, ld, green at minimum
    plaid = caerphilly[caerphilly["party"] == "plaid"].iloc[0]
    assert abs(plaid["actual_share"] - 47.4) < 0.1


def test_loader_filters_by_as_of_date():
    events_df, _ = load_byelections(
        Path("data/hand_curated/by_elections.yaml"),
        as_of=date(2025, 12, 31),
    )
    # Gorton (Feb 2026) excluded
    assert "gorton_denton_2026" not in set(events_df["event_id"])
    assert "caerphilly_senedd_2025" in set(events_df["event_id"])


def test_loader_rejects_event_with_actual_shares_not_summing_to_100(tmp_path: Path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("""
events:
  - event_id: bad_event
    name: Bad event
    date: 2026-01-01
    event_type: westminster_byelection
    nation: england
    region: X
    threat_party: reform
    exclude_from_matrix: false
    narrative_url: https://example.com
    candidates:
      - { party: reform, votes: 100, actual_share: 50.0, prior_share: 30.0 }
      - { party: lab,    votes: 100, actual_share: 30.0, prior_share: 50.0 }
""", encoding="utf-8")
    with pytest.raises(ValueError, match="actual_share entries sum"):
        load_byelections(bad)
