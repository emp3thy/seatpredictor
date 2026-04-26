from pathlib import Path
import pytest
from prediction_engine.runner import run_prediction
from prediction_engine.analysis.drilldown import explain_seat
from schema.prediction import ReformThreatConfig


def test_explain_seat_returns_structured_report(tiny_snapshot_path, tmp_path: Path):
    out = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="reform_threat_consolidation",
                        scenario=ReformThreatConfig(), out_dir=tmp_path / "out", label="t")
    report = explain_seat(out, ons_code="TST00001")
    assert report["ons_code"] == "TST00001"
    assert "share_raw" in report
    assert "share_predicted" in report
    assert "consolidator" in report
    assert "matrix_provenance" in report
    assert "notes" in report


def test_explain_seat_unknown_seat_raises(tiny_snapshot_path, tmp_path: Path):
    out = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="reform_threat_consolidation",
                        scenario=ReformThreatConfig(), out_dir=tmp_path / "out", label="t")
    with pytest.raises(KeyError, match="ZZZ00000"):
        explain_seat(out, ons_code="ZZZ00000")
