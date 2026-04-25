from datetime import date, datetime
from typing import Literal
from pydantic import BaseModel, Field, field_validator
from schema.common import PartyCode, Nation


ALLOWED_NOTE_FLAGS = frozenset({
    "non_reform_leader",
    "consolidator_already_leads",
    "low_clarity",
    "no_matrix_entry",
    "matrix_unavailable",
    "multiplier_clipped",
    "ni_excluded",
})


class ScenarioConfig(BaseModel):
    """Base for strategy-specific knobs. Subclasses add their own fields."""
    pass


class UniformSwingConfig(ScenarioConfig):
    polls_window_days: int = Field(default=14, gt=0)


class ReformThreatConfig(ScenarioConfig):
    multiplier: float = Field(default=1.0, ge=0.0)
    clarity_threshold: float = Field(default=5.0, gt=0.0)
    polls_window_days: int = Field(default=14, gt=0)


_PARTIES = ["con", "lab", "ld", "reform", "green", "snp", "plaid", "other"]


def _share_field() -> "Field":
    return Field(ge=0.0, le=100.0)


class SeatPrediction(BaseModel):
    ons_code: str = Field(min_length=1)
    constituency_name: str = Field(min_length=1)
    nation: Nation
    region: str

    # 24 share columns (8 parties × 3 prefixes). Listed explicitly for clarity.
    share_2024_con: float = _share_field()
    share_2024_lab: float = _share_field()
    share_2024_ld: float = _share_field()
    share_2024_reform: float = _share_field()
    share_2024_green: float = _share_field()
    share_2024_snp: float = _share_field()
    share_2024_plaid: float = _share_field()
    share_2024_other: float = _share_field()

    share_raw_con: float = _share_field()
    share_raw_lab: float = _share_field()
    share_raw_ld: float = _share_field()
    share_raw_reform: float = _share_field()
    share_raw_green: float = _share_field()
    share_raw_snp: float = _share_field()
    share_raw_plaid: float = _share_field()
    share_raw_other: float = _share_field()

    share_predicted_con: float = _share_field()
    share_predicted_lab: float = _share_field()
    share_predicted_ld: float = _share_field()
    share_predicted_reform: float = _share_field()
    share_predicted_green: float = _share_field()
    share_predicted_snp: float = _share_field()
    share_predicted_plaid: float = _share_field()
    share_predicted_other: float = _share_field()

    predicted_winner: PartyCode
    predicted_margin: float

    leader: PartyCode
    consolidator: PartyCode | None = None
    clarity: float | None = Field(default=None, ge=0.0, le=1.0)

    matrix_nation: str | None = None  # 'england'/'wales'/'scotland'/None
    matrix_provenance: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    @field_validator("notes")
    @classmethod
    def _validate_notes(cls, v: list[str]) -> list[str]:
        for flag in v:
            if flag not in ALLOWED_NOTE_FLAGS:
                raise ValueError(f"unknown notes flag: {flag}")
        return v


class NationalTotal(BaseModel):
    scope: Literal["overall", "nation", "region"]
    scope_value: str  # '' for overall; 'england' etc. for nation; region name for region
    party: PartyCode
    seats: int = Field(ge=0)


class RunConfig(BaseModel):
    snapshot_id: str = Field(min_length=1)
    snapshot_content_hash: str = Field(min_length=1)
    snapshot_as_of_date: date
    strategy: str = Field(min_length=1)
    scenario_config_json: str = Field(min_length=1)
    config_hash: str = Field(min_length=1)
    schema_version: int = Field(gt=0)
    run_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    generated_at: datetime
