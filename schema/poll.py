from datetime import date
from enum import Enum
from pydantic import BaseModel, Field, model_validator


class Geography(str, Enum):
    GB = "GB"
    SCOTLAND = "Scotland"
    WALES = "Wales"
    LONDON = "London"


class Poll(BaseModel):
    pollster: str = Field(min_length=1)
    fieldwork_start: date
    fieldwork_end: date
    published_date: date
    sample_size: int = Field(gt=0)
    geography: Geography
    con: float = Field(ge=0, le=100)
    lab: float = Field(ge=0, le=100)
    ld: float = Field(ge=0, le=100)
    reform: float = Field(ge=0, le=100)
    green: float = Field(ge=0, le=100)
    snp: float = Field(ge=0, le=100)
    plaid: float = Field(ge=0, le=100)
    other: float = Field(ge=0, le=100)

    @model_validator(mode="after")
    def _check_shares_and_dates(self) -> "Poll":
        total = self.con + self.lab + self.ld + self.reform + self.green + self.snp + self.plaid + self.other
        if not (99.0 <= total <= 101.0):
            raise ValueError(f"shares must sum to ~100 (got {total:.2f})")
        if self.fieldwork_start > self.fieldwork_end:
            raise ValueError("fieldwork_start must be on or before fieldwork_end")
        return self
