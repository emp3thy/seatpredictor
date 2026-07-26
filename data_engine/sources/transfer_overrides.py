import logging
from pathlib import Path

import pandas as pd
import yaml
from schema.transfer_weights import TransferWeightOverride

logger = logging.getLogger(__name__)


COLUMNS = ["nation", "consolidator", "source", "weight", "rationale"]


def load_transfer_overrides(yaml_path: Path) -> pd.DataFrame:
    """Load transfer_overrides.yaml into a DataFrame with columns
    nation, consolidator, source, weight, rationale.

    Overrides are optional: a missing file returns an empty DataFrame with those
    columns rather than raising, so a checkout without the file still builds.

    A duplicate (nation, consolidator, source) key raises ValueError. Silent
    last-wins would hide a curation mistake, and these numbers are judgement
    calls that someone has to defend.
    """
    if not yaml_path.exists():
        logger.info("No transfer overrides at %s; using derived weights only", yaml_path)
        return pd.DataFrame(columns=COLUMNS)

    with yaml_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    rows: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in raw.get("overrides") or []:
        o = TransferWeightOverride.model_validate(entry)
        key = (o.nation.value, o.consolidator.value, o.source.value)
        if key in seen:
            raise ValueError(
                f"duplicate transfer override for {key} in {yaml_path}"
            )
        seen.add(key)
        rows.append({
            "nation": o.nation.value,
            "consolidator": o.consolidator.value,
            "source": o.source.value,
            "weight": o.weight,
            "rationale": o.rationale,
        })

    if not rows:
        return pd.DataFrame(columns=COLUMNS)

    logger.info("Loaded %d hand-curated transfer overrides", len(rows))
    return pd.DataFrame(rows, columns=COLUMNS)
