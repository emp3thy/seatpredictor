from datetime import date
import pytest
from pydantic import ValidationError
from schema.poll import Poll, Geography


def _valid_poll_payload() -> dict:
    return {
        "pollster": "YouGov",
        "fieldwork_start": date(2026, 4, 18),
        "fieldwork_end": date(2026, 4, 20),
        "published_date": date(2026, 4, 21),
        "sample_size": 1842,
        "geography": "GB",
        "con": 22.0,
        "lab": 28.0,
        "ld": 11.0,
        "reform": 24.0,
        "green": 8.0,
        "snp": 3.0,
        "plaid": 1.0,
        "other": 3.0,
    }


def test_poll_accepts_valid_payload():
    poll = Poll.model_validate(_valid_poll_payload())
    assert poll.pollster == "YouGov"
    assert poll.geography == Geography.GB
    assert poll.lab == 28.0


def test_poll_rejects_shares_summing_far_from_100():
    payload = _valid_poll_payload()
    payload["lab"] = 50.0  # now sums to 122
    with pytest.raises(ValidationError, match="shares must sum to ~100"):
        Poll.model_validate(payload)


def test_poll_geography_values():
    assert {g.value for g in Geography} == {"GB", "Scotland", "Wales", "London"}


def test_poll_round_trip_via_dict():
    poll = Poll.model_validate(_valid_poll_payload())
    raw = poll.model_dump(mode="json")
    restored = Poll.model_validate(raw)
    assert restored == poll


def test_poll_fieldwork_dates_must_be_ordered():
    payload = _valid_poll_payload()
    payload["fieldwork_start"] = date(2026, 4, 25)
    payload["fieldwork_end"] = date(2026, 4, 20)
    with pytest.raises(ValidationError, match="fieldwork_start"):
        Poll.model_validate(payload)
