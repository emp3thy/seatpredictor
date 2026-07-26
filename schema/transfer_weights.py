from pydantic import BaseModel, Field
from schema.common import PartyCode, Nation


class TransferWeightCell(BaseModel):
    nation: Nation
    consolidator: PartyCode
    source: PartyCode
    weight: float = Field(ge=0.0, le=1.0)
    n: int = Field(ge=0)  # 0 == hand-curated, no supporting by-election events


class TransferWeightOverride(BaseModel):
    """A hand-curated matrix cell from data/hand_curated/transfer_overrides.yaml.

    Replaces a derived cell with the same key, or creates the cell if no
    by-election produced one. `rationale` is required: an override is a judgement
    call and the reasoning must travel with the number.
    """
    nation: Nation
    consolidator: PartyCode
    source: PartyCode
    weight: float = Field(ge=0.0, le=1.0)
    rationale: str = Field(min_length=1)


class TransferWeightProvenance(BaseModel):
    nation: Nation
    consolidator: PartyCode
    event_id: str = Field(min_length=1)
