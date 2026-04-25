from datetime import datetime, date, timezone
import pytest
from pydantic import ValidationError
from schema.snapshot import SnapshotManifest


def _valid() -> dict:
    return {
        "as_of_date": date(2026, 4, 25),
        "schema_version": 1,
        "content_hash": "a3f2b00c0011",
        "generated_at": datetime(2026, 4, 25, 14, 30, tzinfo=timezone.utc),
        "source_versions": {
            "wikipedia_polls": "fetched_2026-04-25",
            "hoc_results": "ge_2024",
            "byelections": "yaml_sha:1234abcd",
        },
    }


def test_manifest_valid():
    m = SnapshotManifest.model_validate(_valid())
    assert m.schema_version == 1
    assert m.source_versions["hoc_results"] == "ge_2024"


def test_schema_version_must_be_positive():
    payload = _valid()
    payload["schema_version"] = 0
    with pytest.raises(ValidationError):
        SnapshotManifest.model_validate(payload)


def test_content_hash_required_nonempty():
    payload = _valid()
    payload["content_hash"] = ""
    with pytest.raises(ValidationError):
        SnapshotManifest.model_validate(payload)


def test_round_trip():
    m = SnapshotManifest.model_validate(_valid())
    restored = SnapshotManifest.model_validate(m.model_dump(mode="json"))
    assert restored == m
