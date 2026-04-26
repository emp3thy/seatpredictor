from pathlib import Path
from click.testing import CliRunner

from prediction_engine.cli_analyze import main as analyze_main
from prediction_engine.runner import run_prediction
from schema.prediction import UniformSwingConfig, ReformThreatConfig


def _two_runs(tiny_snapshot_path, tmp_path: Path):
    a = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="uniform_swing",
                       scenario=UniformSwingConfig(), out_dir=tmp_path / "out", label="a")
    b = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="reform_threat_consolidation",
                       scenario=ReformThreatConfig(), out_dir=tmp_path / "out", label="b")
    return a, b


def test_drilldown_prints_seat_report(tiny_snapshot_path, tmp_path: Path):
    a, _ = _two_runs(tiny_snapshot_path, tmp_path)
    res = CliRunner().invoke(analyze_main, [
        "drilldown", "--run", str(a), "--seat", "TST00001", "--explain",
    ])
    assert res.exit_code == 0, res.output
    assert "TST00001" in res.output


def test_flips_prints_diff(tiny_snapshot_path, tmp_path: Path):
    a, b = _two_runs(tiny_snapshot_path, tmp_path)
    res = CliRunner().invoke(analyze_main, ["flips", "--runs", str(a), str(b)])
    assert res.exit_code == 0, res.output
    assert "flips" in res.output.lower() or "no flips" in res.output.lower()


def test_flips_no_flips_when_runs_identical(tiny_snapshot_path, tmp_path: Path):
    """Same path passed for both --runs ⇒ guaranteed zero flips. Locks the
    short-circuit message that callers (notebooks, scripts) may grep for."""
    a, _ = _two_runs(tiny_snapshot_path, tmp_path)
    res = CliRunner().invoke(analyze_main, ["flips", "--runs", str(a), str(a)])
    assert res.exit_code == 0, res.output
    assert "no flips between the two runs" in res.output


def test_drilldown_json_branch_no_explain(tiny_snapshot_path, tmp_path: Path):
    """Without --explain, drilldown emits indented JSON (consumed by scripts/notebooks).
    Locks the JSON contract: ons_code, share_raw dict, share_predicted dict."""
    import json
    a, _ = _two_runs(tiny_snapshot_path, tmp_path)
    res = CliRunner().invoke(analyze_main, [
        "drilldown", "--run", str(a), "--seat", "TST00001",
    ])
    assert res.exit_code == 0, res.output
    data = json.loads(res.output)
    assert data["ons_code"] == "TST00001"
    assert "share_raw" in data
    assert "share_predicted" in data
