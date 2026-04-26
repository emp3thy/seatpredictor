import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.sqlite_io import (
    PREDICTION_SCHEMA_VERSION,
    build_run_id,
    compute_config_hash,
    prediction_filename,
    write_prediction_db,
)
from prediction_engine import strategies as _strategies  # noqa: F401  populates registry
from prediction_engine.strategies.base import STRATEGY_REGISTRY
from schema.prediction import RunConfig, ScenarioConfig

logger = logging.getLogger(__name__)


def run_prediction(
    *,
    snapshot_path: Path,
    strategy_name: str,
    scenario: ScenarioConfig,
    out_dir: Path,
    label: str = "baseline",
) -> Path:
    """Load snapshot -> run strategy -> write prediction SQLite. Idempotent on
    (snapshot_content_hash, strategy, config_hash, label).
    """
    if strategy_name not in STRATEGY_REGISTRY:
        raise KeyError(f"unknown strategy: {strategy_name}")
    strategy_cls = STRATEGY_REGISTRY[strategy_name]
    # Validate scenario via the strategy's own schema (catches mistyped configs).
    scenario_validated = strategy_cls.config_schema.model_validate(scenario.model_dump())

    snapshot = Snapshot(snapshot_path)
    config_hash = compute_config_hash(scenario_validated)
    out_path = prediction_filename(
        out_dir=out_dir,
        snapshot_content_hash=snapshot.manifest.content_hash,
        strategy=strategy_name,
        config_hash=config_hash,
        label=label,
    )
    if out_path.exists():
        logger.info("Prediction %s already exists; reusing", out_path.name)
        return out_path

    strat = strategy_cls()
    result = strat.predict(snapshot, scenario_validated)

    run_id = build_run_id(snapshot.manifest.content_hash, strategy_name, config_hash, label)
    cfg = RunConfig(
        snapshot_id=snapshot.snapshot_id,
        snapshot_content_hash=snapshot.manifest.content_hash,
        snapshot_as_of_date=snapshot.manifest.as_of_date,
        strategy=strategy_name,
        scenario_config_json=json.dumps(scenario_validated.model_dump(mode="json"), sort_keys=True),
        config_hash=config_hash,
        schema_version=PREDICTION_SCHEMA_VERSION,
        run_id=run_id,
        label=label,
        generated_at=datetime.now(tz=timezone.utc),
    )

    write_prediction_db(out_path, seats=result.per_seat, national=result.national, run_config=cfg)
    logger.info("Wrote prediction %s", out_path.name)
    return out_path
