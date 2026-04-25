from datetime import date, datetime
from pydantic import BaseModel, Field


class SnapshotManifest(BaseModel):
    as_of_date: date
    schema_version: int = Field(gt=0)
    content_hash: str = Field(min_length=1)
    generated_at: datetime
    source_versions: dict[str, str] = Field(default_factory=dict)
