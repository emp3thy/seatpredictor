import pytest
from pydantic import ValidationError
from schema.constituency import ConstituencyResult
from schema.common import PartyCode, Nation


def _valid_row() -> dict:
    return {
        "ons_code": "E14001234",
        "constituency_name": "Gorton and Denton",
        "region": "North West",
        "nation": "england",
        "party": "lab",
        "votes": 18234,
        "share": 49.7,
    }


def test_constituency_result_valid():
    row = ConstituencyResult.model_validate(_valid_row())
    assert row.ons_code == "E14001234"
    assert row.party == PartyCode.LAB
    assert row.nation == Nation.ENGLAND


def test_share_must_be_between_zero_and_one_hundred():
    payload = _valid_row()
    payload["share"] = 110.0
    with pytest.raises(ValidationError):
        ConstituencyResult.model_validate(payload)


def test_votes_must_be_non_negative():
    payload = _valid_row()
    payload["votes"] = -1
    with pytest.raises(ValidationError):
        ConstituencyResult.model_validate(payload)


def test_round_trip():
    row = ConstituencyResult.model_validate(_valid_row())
    restored = ConstituencyResult.model_validate(row.model_dump(mode="json"))
    assert restored == row
