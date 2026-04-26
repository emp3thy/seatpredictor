from pathlib import Path

from click.testing import CliRunner

from prediction_engine.cli import main


def test_list_strategies_prints_both():
    res = CliRunner().invoke(main, ["list-strategies"])
    assert res.exit_code == 0
    assert "uniform_swing" in res.output
    assert "reform_threat_consolidation" in res.output


def test_run_uniform_swing_writes_file(tiny_snapshot_path, tmp_path: Path):
    out_dir = tmp_path / "out"
    res = CliRunner().invoke(main, [
        "run",
        "--snapshot", str(tiny_snapshot_path),
        "--strategy", "uniform_swing",
        "--out-dir", str(out_dir),
        "--label", "test",
        "--polls-window-days", "14",
    ])
    assert res.exit_code == 0, res.output
    files = list(out_dir.glob("*.sqlite"))
    assert len(files) == 1


def test_run_reform_threat_writes_file(tiny_snapshot_path, tmp_path: Path):
    out_dir = tmp_path / "out"
    res = CliRunner().invoke(main, [
        "run",
        "--snapshot", str(tiny_snapshot_path),
        "--strategy", "reform_threat_consolidation",
        "--out-dir", str(out_dir),
        "--label", "test",
        "--multiplier", "1.0",
        "--clarity-threshold", "5.0",
        "--polls-window-days", "14",
    ])
    assert res.exit_code == 0, res.output
    assert len(list(out_dir.glob("*.sqlite"))) == 1


def test_sweep_produces_one_file_per_multiplier(tiny_snapshot_path, tmp_path: Path):
    out_dir = tmp_path / "out"
    res = CliRunner().invoke(main, [
        "sweep",
        "--snapshot", str(tiny_snapshot_path),
        "--strategy", "reform_threat_consolidation",
        "--out-dir", str(out_dir),
        "--label-prefix", "swp",
        "--multiplier", "0.5,1.0,1.5",
        "--clarity-threshold", "5.0",
        "--polls-window-days", "14",
    ])
    assert res.exit_code == 0, res.output
    assert len(list(out_dir.glob("*.sqlite"))) == 3


def test_diff_lists_flips(tiny_snapshot_path, tmp_path: Path):
    out_dir = tmp_path / "out"
    runner = CliRunner()
    runner.invoke(main, [
        "run", "--snapshot", str(tiny_snapshot_path),
        "--strategy", "uniform_swing",
        "--out-dir", str(out_dir), "--label", "us",
    ])
    runner.invoke(main, [
        "run", "--snapshot", str(tiny_snapshot_path),
        "--strategy", "reform_threat_consolidation",
        "--out-dir", str(out_dir), "--label", "rtc",
    ])
    files = sorted(out_dir.glob("*.sqlite"))
    res = runner.invoke(main, ["diff", str(files[0]), str(files[1])])
    assert res.exit_code == 0, res.output
    # Output is human-readable; just check it ran and printed something.
    assert "flips" in res.output.lower() or "no flips" in res.output.lower()


def test_run_unknown_strategy_exits_nonzero(tiny_snapshot_path, tmp_path: Path):
    out_dir = tmp_path / "out"
    res = CliRunner().invoke(main, [
        "run",
        "--snapshot", str(tiny_snapshot_path),
        "--strategy", "nope",
        "--out-dir", str(out_dir),
        "--label", "test",
    ])
    assert res.exit_code != 0
