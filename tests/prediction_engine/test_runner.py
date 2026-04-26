from pathlib import Path
import pytest
from prediction_engine.runner import run_prediction
from prediction_engine.sqlite_io import read_prediction_seats, read_prediction_config
from schema.prediction import UniformSwingConfig


def test_run_prediction_writes_sqlite(tiny_snapshot_path, tmp_path: Path):
    out = run_prediction(
        snapshot_path=tiny_snapshot_path,
        strategy_name="uniform_swing",
        scenario=UniformSwingConfig(),
        out_dir=tmp_path,
        label="baseline",
    )
    assert out.exists()
    seats = read_prediction_seats(out)
    assert len(seats) == 6


def test_run_prediction_idempotent(tiny_snapshot_path, tmp_path: Path):
    """Idempotency contract: same (snapshot, strategy, config, label) => same path AND
    the file is NOT rewritten on the second call.

    We don't compare st_mtime_ns directly — filesystem timestamp resolution varies
    (Windows NTFS = 100ns, ext4 = 1ns, FAT32 = 2s) and pytest's tmp_path may live on
    a low-resolution mount. Instead, compare the SHA-256 of the file's bytes before
    and after; same bytes => no rewrite.
    """
    import hashlib

    def _file_hash(p: Path) -> str:
        return hashlib.sha256(p.read_bytes()).hexdigest()

    a = run_prediction(
        snapshot_path=tiny_snapshot_path,
        strategy_name="uniform_swing",
        scenario=UniformSwingConfig(),
        out_dir=tmp_path,
        label="baseline",
    )
    hash_before = _file_hash(a)
    b = run_prediction(
        snapshot_path=tiny_snapshot_path,
        strategy_name="uniform_swing",
        scenario=UniformSwingConfig(),
        out_dir=tmp_path,
        label="baseline",
    )
    assert a == b
    assert _file_hash(b) == hash_before, "second call rewrote the prediction file"


def test_run_prediction_writes_config_table(tiny_snapshot_path, tmp_path: Path):
    out = run_prediction(
        snapshot_path=tiny_snapshot_path,
        strategy_name="uniform_swing",
        scenario=UniformSwingConfig(),
        out_dir=tmp_path,
        label="baseline",
    )
    cfg = read_prediction_config(out)
    assert cfg.strategy == "uniform_swing"
    assert cfg.label == "baseline"
    # snapshot_content_hash from tiny snapshot fixture
    assert cfg.snapshot_content_hash == "tinyhash0001"


def test_run_prediction_unknown_strategy_raises(tiny_snapshot_path, tmp_path: Path):
    with pytest.raises(KeyError, match="unknown strategy"):
        run_prediction(
            snapshot_path=tiny_snapshot_path,
            strategy_name="nope",
            scenario=UniformSwingConfig(),
            out_dir=tmp_path,
            label="baseline",
        )


def test_run_prediction_rejects_cross_strategy_config(tiny_snapshot_path, tmp_path: Path):
    """Pass a ReformThreatConfig to uniform_swing — the runner's
    config_schema.model_validate(scenario.model_dump()) round-trip should reject the
    extra `multiplier`/`clarity_threshold` fields per ScenarioConfig's extra='forbid'."""
    from pydantic import ValidationError
    from schema.prediction import ReformThreatConfig
    with pytest.raises(ValidationError):
        run_prediction(
            snapshot_path=tiny_snapshot_path,
            strategy_name="uniform_swing",
            scenario=ReformThreatConfig(multiplier=1.5),
            out_dir=tmp_path,
            label="baseline",
        )
