from datetime import date
import pytest
from pydantic import ValidationError
from schema.byelection import ByElectionEvent, ByElectionResult, EventType
from schema.common import PartyCode, Nation


def _valid_event() -> dict:
    return {
        "event_id": "caerphilly_senedd_2025",
        "name": "Caerphilly Senedd by-election",
        "date": date(2025, 10, 23),
        "event_type": "senedd",
        "nation": "wales",
        "region": "South Wales East",
        "threat_party": "reform",
        "exclude_from_matrix": False,
        "narrative_url": "https://en.wikipedia.org/wiki/2025_Caerphilly_by-election",
    }


def _valid_result() -> dict:
    return {
        "event_id": "caerphilly_senedd_2025",
        "party": "plaid",
        "votes": 15961,
        "actual_share": 47.4,
        "prior_share": 28.4,
    }


def test_event_valid():
    ev = ByElectionEvent.model_validate(_valid_event())
    assert ev.event_id == "caerphilly_senedd_2025"
    assert ev.event_type == EventType.SENEDD
    assert ev.threat_party == PartyCode.REFORM


def test_event_null_threat_party_implies_exclude():
    payload = _valid_event()
    payload["threat_party"] = None
    payload["exclude_from_matrix"] = False
    ev = ByElectionEvent.model_validate(payload)
    assert ev.exclude_from_matrix is True  # auto-coerced


def test_result_valid():
    r = ByElectionResult.model_validate(_valid_result())
    assert r.party == PartyCode.PLAID
    assert r.actual_share == 47.4


def test_event_types():
    assert {e.value for e in EventType} == {
        "westminster_byelection", "senedd", "holyrood"
    }


def test_round_trip_event():
    ev = ByElectionEvent.model_validate(_valid_event())
    restored = ByElectionEvent.model_validate(ev.model_dump(mode="json"))
    assert restored == ev
