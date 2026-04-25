from datetime import date
from enum import Enum
from pydantic import BaseModel, Field, HttpUrl, model_validator
from schema.common import PartyCode, Nation


class EventType(str, Enum):
    WESTMINSTER_BYELECTION = "westminster_byelection"
    SENEDD = "senedd"
    HOLYROOD = "holyrood"


class ByElectionEvent(BaseModel):
    event_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    date: date
    event_type: EventType
    nation: Nation
    region: str
    threat_party: PartyCode | None = None
    exclude_from_matrix: bool = False
    narrative_url: HttpUrl | None = None

    @model_validator(mode="after")
    def _coerce_exclusion(self) -> "ByElectionEvent":
        if self.threat_party is None:
            object.__setattr__(self, "exclude_from_matrix", True)
        return self


class ByElectionResult(BaseModel):
    event_id: str = Field(min_length=1)
    party: PartyCode
    votes: int = Field(ge=0)
    actual_share: float = Field(ge=0, le=100)
    prior_share: float = Field(ge=0, le=100)
