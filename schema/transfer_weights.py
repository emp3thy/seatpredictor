from pydantic import BaseModel, Field
from schema.common import PartyCode, Nation


class TransferWeightCell(BaseModel):
    nation: Nation
    consolidator: PartyCode
    source: PartyCode
    weight: float = Field(ge=0.0, le=1.0)
    n: int = Field(gt=0)


class TransferWeightProvenance(BaseModel):
    nation: Nation
    consolidator: PartyCode
    event_id: str = Field(min_length=1)
