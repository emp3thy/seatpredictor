from datetime import date, datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest
from prediction_engine.sqlite_io import (
    PREDICTION_SCHEMA_VERSION,
    write_prediction_db,
    read_prediction_seats,
    read_prediction_config,
    read_prediction_national,
    read_prediction_notes_index,
    compute_config_hash,
    build_run_id,
    prediction_filename,
)
from schema.common import PartyCode
from schema.prediction import RunConfig, UniformSwingConfig


def _seats_df() -> pd.DataFrame:
    rows = []
    for ons in ("TST1", "TST2"):
        row = {
            "ons_code": ons, "constituency_name": ons, "nation": "england", "region": "X",
            "predicted_winner": "lab", "predicted_margin": 5.0,
            "leader": "lab", "consolidator": None, "clarity": None,
            "matrix_nation": None, "matrix_provenance": "[]",
            "notes": json.dumps(["non_reform_leader"]) if ons == "TST1" else "[]",
        }
        for prefix in ("share_2024", "share_raw", "share_predicted"):
            for p in PartyCode:
                row[f"{prefix}_{p.value}"] = 12.5
        rows.append(row)
    return pd.DataFrame(rows)


def _national_df() -> pd.DataFrame:
    return pd.DataFrame([
        {"scope": "overall", "scope_value": "", "party": "lab", "seats": 2},
    ])


def _run_config() -> RunConfig:
    return RunConfig(
        snapshot_id="2026-04-25__v1__abc123def456",
        snapshot_content_hash="abc123def456",
        snapshot_as_of_date=date(2026, 4, 25),
        strategy="uniform_swing",
        scenario_config_json='{"polls_window_days": 14}',
        config_hash="0011223344aa",
        schema_version=PREDICTION_SCHEMA_VERSION,
        run_id="abc123def456__uniform_swing__0011223344aa__baseline",
        label="baseline",
        generated_at=datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc),
    )


def test_compute_config_hash_stable():
    cfg1 = UniformSwingConfig(polls_window_days=14)
    cfg2 = UniformSwingConfig(polls_window_days=14)
    assert compute_config_hash(cfg1) == compute_config_hash(cfg2)
    assert len(compute_config_hash(cfg1)) == 12


def test_compute_config_hash_distinct_for_distinct_config():
    a = compute_config_hash(UniformSwingConfig(polls_window_days=14))
    b = compute_config_hash(UniformSwingConfig(polls_window_days=21))
    assert a != b


def test_build_run_id_format():
    rid = build_run_id("abc123def456", "uniform_swing", "0011223344aa", "baseline")
    assert rid == "abc123def456__uniform_swing__0011223344aa__baseline"


def test_prediction_filename(tmp_path: Path):
    out = prediction_filename(
        out_dir=tmp_path,
        snapshot_content_hash="abc123",
        strategy="uniform_swing",
        config_hash="cfg789",
        label="baseline",
    )
    assert out == tmp_path / "abc123__uniform_swing__cfg789__baseline.sqlite"


def test_write_prediction_db_round_trip(tmp_path: Path):
    out = tmp_path / "pred.sqlite"
    seats = _seats_df()
    nat   = _national_df()
    cfg   = _run_config()

    write_prediction_db(out, seats=seats, national=nat, run_config=cfg)
    assert out.exists()

    seats_back = read_prediction_seats(out)
    assert len(seats_back) == 2
    assert set(seats_back["ons_code"]) == {"TST1", "TST2"}

    nat_back = read_prediction_national(out)
    assert nat_back.iloc[0]["seats"] == 2

    cfg_back = read_prediction_config(out)
    assert cfg_back.run_id == cfg.run_id
    assert cfg_back.scenario_config_json == cfg.scenario_config_json
    # datetime + date round-trip: SQLite stores ISO strings, pandas reads strings,
    # Pydantic v2 coerces back. Locking this contract keeps Task 11's runner stable
    # if pandas / SQLite type-affinity behaviour ever shifts.
    assert cfg_back.generated_at == cfg.generated_at
    assert cfg_back.snapshot_as_of_date == cfg.snapshot_as_of_date

    notes_back = read_prediction_notes_index(out)
    # TST1 has 1 flag, TST2 has none → 1 row total
    assert len(notes_back) == 1
    assert notes_back.iloc[0]["ons_code"] == "TST1"
    assert notes_back.iloc[0]["flag"] == "non_reform_leader"


def test_label_slug_validation(tmp_path: Path):
    with pytest.raises(ValueError, match="invalid label"):
        prediction_filename(
            out_dir=tmp_path,
            snapshot_content_hash="abc123",
            strategy="uniform_swing",
            config_hash="cfg789",
            label="bad label/with slashes",
        )


def test_label_slug_accepts_underscore_and_hyphen(tmp_path: Path):
    """Sweep labels in real use look like `sweep_m1p25` or `v2-final`. Regression
    guard against an over-tightening of the label regex."""
    out = prediction_filename(
        out_dir=tmp_path,
        snapshot_content_hash="abc123",
        strategy="uniform_swing",
        config_hash="cfg789",
        label="sweep_m1p25-v2",
    )
    assert out.name == "abc123__uniform_swing__cfg789__sweep_m1p25-v2.sqlite"


def test_label_slug_rejects_empty_string(tmp_path: Path):
    with pytest.raises(ValueError, match="invalid label"):
        prediction_filename(
            out_dir=tmp_path,
            snapshot_content_hash="abc123",
            strategy="uniform_swing",
            config_hash="cfg789",
            label="",
        )


def test_round_trip_preserves_int_types(tmp_path: Path):
    """schema_version comes back from SQLite as numpy.int64 (via pandas), but Pydantic's
    int validator must accept it. This guards against subtle type-coercion regressions."""
    out = tmp_path / "pred_int.sqlite"
    write_prediction_db(out, seats=_seats_df(), national=_national_df(), run_config=_run_config())
    cfg_back = read_prediction_config(out)
    assert isinstance(cfg_back.schema_version, int)
    assert cfg_back.schema_version == PREDICTION_SCHEMA_VERSION


def test_explode_notes_handles_empty_flag_lists(tmp_path: Path):
    """A prediction where no seat carries any flag must still produce a writable
    notes_index (empty DataFrame) and not crash on read-back."""
    seats = _seats_df()
    seats["notes"] = "[]"  # remove all flags from every seat
    out = tmp_path / "pred_empty_notes.sqlite"
    write_prediction_db(out, seats=seats, national=_national_df(), run_config=_run_config())
    notes_back = read_prediction_notes_index(out)
    assert len(notes_back) == 0
    assert set(notes_back.columns) == {"ons_code", "flag"}
