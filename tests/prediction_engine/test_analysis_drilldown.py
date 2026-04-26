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


def test_explain_seat_emits_strict_rfc8259_json_for_short_circuit_seat(
    tiny_snapshot_path, tmp_path: Path,
):
    """Seats that short-circuit (NI / non_reform_leader / matrix_unavailable with no
    consolidator) have NULL clarity, which after the SQLite round-trip pandas reads as
    float('nan'). json.dumps(NaN) emits the JS literal `NaN`, invalid per RFC 8259
    and rejected by strict parsers like jq. The fix coerces NaN/None to JSON null.

    Seat C (Carchester) is the non_reform_leader path — clarity NULL.
    Seat F (Foyle) is the NI path — clarity NULL.
    """
    import json
    out = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="reform_threat_consolidation",
                        scenario=ReformThreatConfig(), out_dir=tmp_path / "out", label="t")
    for ons in ("TST00003", "TST00006"):
        report = explain_seat(out, ons_code=ons)
        # clarity comes back as None, not NaN
        assert report["clarity"] is None
        # Strict JSON: re-parse must succeed and clarity is null.
        s = json.dumps(report)
        reparsed = json.loads(s)
        assert reparsed["clarity"] is None
