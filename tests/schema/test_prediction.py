from datetime import date, datetime, timezone
import pytest
from pydantic import ValidationError
from schema.prediction import (
    UniformSwingConfig,
    ReformThreatConfig,
    SeatPrediction,
    NationalTotal,
    RunConfig,
)
from schema.common import PartyCode, Nation


def test_uniform_swing_config_defaults():
    cfg = UniformSwingConfig()
    assert cfg.polls_window_days == 14


def test_uniform_swing_config_validates_positive_window():
    with pytest.raises(ValidationError):
        UniformSwingConfig(polls_window_days=0)


def test_reform_threat_config_defaults():
    cfg = ReformThreatConfig()
    assert cfg.multiplier == 1.0
    assert cfg.clarity_threshold == 5.0
    assert cfg.polls_window_days == 14


def test_reform_threat_config_validation_bounds():
    with pytest.raises(ValidationError):
        ReformThreatConfig(multiplier=-0.1)
    with pytest.raises(ValidationError):
        ReformThreatConfig(clarity_threshold=0.0)


def _seat_kwargs() -> dict:
    base = dict(
        ons_code="E14000001",
        constituency_name="Aldershot",
        nation=Nation.ENGLAND,
        region="South East",
        predicted_winner=PartyCode.LAB,
        predicted_margin=2.5,
        leader=PartyCode.LAB,
        consolidator=None,
        clarity=None,
        matrix_nation="england",
        matrix_provenance=[],
        notes=[],
    )
    for prefix in ("share_2024", "share_raw", "share_predicted"):
        for p in ["con", "lab", "ld", "reform", "green", "snp", "plaid", "other"]:
            base[f"{prefix}_{p}"] = 12.5
    return base


def test_seat_prediction_round_trip():
    seat = SeatPrediction.model_validate(_seat_kwargs())
    raw = seat.model_dump(mode="json")
    restored = SeatPrediction.model_validate(raw)
    assert restored == seat


def test_seat_prediction_rejects_unknown_note_flag():
    kwargs = _seat_kwargs()
    kwargs["notes"] = ["definitely_not_a_real_flag"]
    with pytest.raises(ValidationError, match="unknown notes flag"):
        SeatPrediction.model_validate(kwargs)


def test_seat_prediction_accepts_known_note_flags():
    kwargs = _seat_kwargs()
    kwargs["notes"] = ["non_reform_leader", "ni_excluded"]
    seat = SeatPrediction.model_validate(kwargs)
    assert seat.notes == ["non_reform_leader", "ni_excluded"]


def test_national_total_validates():
    nt = NationalTotal(scope="overall", scope_value="", party=PartyCode.LAB, seats=210)
    assert nt.seats == 210


def test_run_config_round_trip():
    rc = RunConfig(
        snapshot_id="2026-04-25__v1__abc123def456",
        snapshot_content_hash="abc123def456",
        snapshot_as_of_date=date(2026, 4, 25),
        strategy="uniform_swing",
        scenario_config_json='{"polls_window_days": 14}',
        config_hash="0011223344aa",
        schema_version=1,
        run_id="abc123def456__uniform_swing__0011223344aa__baseline",
        label="baseline",
        generated_at=datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert rc.label == "baseline"
    raw = rc.model_dump(mode="json")
    restored = RunConfig.model_validate(raw)
    assert restored == rc


def test_scenario_config_default_reform_polling_correction_is_zero():
    """The new field defaults to 0.0 — no-op when callers don't set it."""
    from schema.prediction import UniformSwingConfig, ReformThreatConfig
    assert UniformSwingConfig().reform_polling_correction_pp == 0.0
    assert ReformThreatConfig().reform_polling_correction_pp == 0.0


def test_scenario_config_reform_polling_correction_accepts_positive_and_negative():
    """Positive = pollsters under-state Reform; negative = pollsters over-state.
    No clamp — caller's choice."""
    from schema.prediction import UniformSwingConfig, ReformThreatConfig
    assert UniformSwingConfig(reform_polling_correction_pp=2.5).reform_polling_correction_pp == 2.5
    assert ReformThreatConfig(reform_polling_correction_pp=-1.0).reform_polling_correction_pp == -1.0


def test_scenario_config_reform_polling_correction_round_trips_through_model_dump():
    """The field appears in model_dump() so RunConfig.scenario_config_json captures it."""
    from schema.prediction import ReformThreatConfig
    cfg = ReformThreatConfig(reform_polling_correction_pp=1.5, multiplier=1.0)
    dumped = cfg.model_dump(mode="json")
    assert dumped["reform_polling_correction_pp"] == 1.5
    # Round-trip
    restored = ReformThreatConfig.model_validate(dumped)
    assert restored.reform_polling_correction_pp == 1.5
