from pydantic import BaseModel, Field
from schema.common import PartyCode, Nation


class ConstituencyResult(BaseModel):
    ons_code: str = Field(min_length=1)
    constituency_name: str = Field(min_length=1)
    region: str = Field(min_length=1)
    nation: Nation
    party: PartyCode
    votes: int = Field(ge=0)
    share: float = Field(ge=0, le=100)
