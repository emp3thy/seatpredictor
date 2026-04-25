from datetime import date, datetime, timezone
from pathlib import Path
import sqlite3
import pandas as pd
import pytest
from data_engine.sqlite_io import (
    open_snapshot_db,
    write_dataframe,
    read_dataframe,
    write_manifest,
    read_manifest,
    compute_input_hash,
)
from schema.snapshot import SnapshotManifest


def test_open_creates_file(tmp_path: Path):
    db_path = tmp_path / "test.sqlite"
    with open_snapshot_db(db_path) as conn:
        assert isinstance(conn, sqlite3.Connection)
    assert db_path.exists()


def test_dataframe_round_trip(tmp_path: Path):
    df = pd.DataFrame({
        "name": ["a", "b", "c"],
        "value": [1.5, 2.5, 3.5],
        "count": [10, 20, 30],
    })
    db_path = tmp_path / "test.sqlite"
    with open_snapshot_db(db_path) as conn:
        write_dataframe(conn, "things", df)
    with open_snapshot_db(db_path) as conn:
        restored = read_dataframe(conn, "things")
    pd.testing.assert_frame_equal(
        restored.sort_values("name").reset_index(drop=True),
        df.sort_values("name").reset_index(drop=True),
        check_dtype=False,
    )


def test_manifest_round_trip(tmp_path: Path):
    db_path = tmp_path / "test.sqlite"
    m = SnapshotManifest(
        as_of_date=date(2026, 4, 25),
        schema_version=1,
        content_hash="abc123",
        generated_at=datetime(2026, 4, 25, tzinfo=timezone.utc),
        source_versions={"x": "y"},
    )
    with open_snapshot_db(db_path) as conn:
        write_manifest(conn, m)
    with open_snapshot_db(db_path) as conn:
        restored = read_manifest(conn)
    assert restored == m


def test_input_hash_deterministic():
    h1 = compute_input_hash(
        as_of_date=date(2026, 4, 25),
        schema_version=1,
        source_versions={"a": "v1", "b": "v2"},
    )
    h2 = compute_input_hash(
        as_of_date=date(2026, 4, 25),
        schema_version=1,
        source_versions={"b": "v2", "a": "v1"},  # key order differs
    )
    assert h1 == h2
    assert len(h1) == 12  # short hash


def test_input_hash_changes_with_inputs():
    h1 = compute_input_hash(
        as_of_date=date(2026, 4, 25), schema_version=1, source_versions={"a": "v1"}
    )
    h2 = compute_input_hash(
        as_of_date=date(2026, 4, 26), schema_version=1, source_versions={"a": "v1"}
    )
    h3 = compute_input_hash(
        as_of_date=date(2026, 4, 25), schema_version=2, source_versions={"a": "v1"}
    )
    h4 = compute_input_hash(
        as_of_date=date(2026, 4, 25), schema_version=1, source_versions={"a": "v2"}
    )
    assert len({h1, h2, h3, h4}) == 4
