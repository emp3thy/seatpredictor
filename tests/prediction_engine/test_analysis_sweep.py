from pathlib import Path
from prediction_engine.runner import run_prediction
from prediction_engine.analysis.sweep import collect_sweep
from schema.prediction import ReformThreatConfig


def test_collect_sweep_summarises_runs(tiny_snapshot_path, tmp_path: Path):
    paths = []
    for m in (0.5, 1.0):
        paths.append(run_prediction(
            snapshot_path=tiny_snapshot_path,
            strategy_name="reform_threat_consolidation",
            scenario=ReformThreatConfig(multiplier=m),
            out_dir=tmp_path / "out",
            label=f"swp_m{m:.2f}".replace(".", "p"),
        ))
    summary = collect_sweep(paths)
    # one row per (run, party) with seats column
    assert len(summary) > 0
    assert {"run_id", "multiplier", "clarity_threshold", "party", "seats"} <= set(summary.columns)
    assert set(summary["multiplier"]) == {0.5, 1.0}
