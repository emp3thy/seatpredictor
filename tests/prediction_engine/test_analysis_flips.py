from pathlib import Path
from prediction_engine.runner import run_prediction
from prediction_engine.analysis.flips import compute_flips
from schema.prediction import UniformSwingConfig, ReformThreatConfig


def test_compute_flips_returns_dataframe(tiny_snapshot_path, tmp_path: Path):
    a = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="uniform_swing",
                       scenario=UniformSwingConfig(), out_dir=tmp_path / "out", label="a")
    b = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="reform_threat_consolidation",
                       scenario=ReformThreatConfig(), out_dir=tmp_path / "out", label="b")
    flips = compute_flips(a, b)
    assert set(flips.columns) >= {"ons_code", "constituency_name", "winner_a", "winner_b"}


def test_compute_flips_empty_when_runs_identical(tiny_snapshot_path, tmp_path: Path):
    a = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="uniform_swing",
                       scenario=UniformSwingConfig(), out_dir=tmp_path / "out", label="a")
    flips = compute_flips(a, a)
    assert flips.empty
