import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from data_engine.sources.local_elections import LocalElectionEvent, LocalElectionPNSSource
from prediction_engine.analysis.poll_bias import (
    EVENT_WEIGHTS,
    BiasResult,
    compute_reform_bias,
    write_bias_json,
)


def _polls_df(rows):
    cols = ["pollster", "fieldwork_start", "fieldwork_end", "published_date",
            "sample_size", "geography", "con", "lab", "ld", "reform",
            "green", "snp", "plaid", "other"]
    return pd.DataFrame(rows, columns=cols)


def _byelections_results_df(rows):
    return pd.DataFrame(rows, columns=["event_id", "party", "votes", "actual_share", "prior_share"])


def _byelections_events_df(rows):
    return pd.DataFrame(rows, columns=["event_id", "name", "date", "event_type",
                                        "nation", "region", "threat_party",
                                        "exclude_from_matrix", "narrative_url"])


class _StubSnapshot:
    def __init__(self, polls, byelections_events, byelections_results):
        self.polls = polls
        self.byelections_events = byelections_events
        self.byelections_results = byelections_results

        class _M:
            content_hash = "deadbeef0123"
            as_of_date = date(2026, 4, 26)
        self.manifest = _M()

        self.snapshot_id = "test-snapshot"


def test_compute_reform_bias_single_byelection_no_local():
    """One by-election with one final-week poll: bias = actual - poll_mean."""
    snapshot = _StubSnapshot(
        polls=_polls_df([
            ("YouGov", "2025-04-25", "2025-04-27", "2025-04-28", 1500, "GB",
             20.0, 25.0, 12.0, 12.0, 8.0, 3.0, 1.0, 19.0),  # reform=12
        ]),
        byelections_events=_byelections_events_df([
            ("runcorn_helsby_2025", "Runcorn", "2025-05-01", "westminster_byelection",
             "england", "North West", "reform", False, ""),
        ]),
        byelections_results=_byelections_results_df([
            ("runcorn_helsby_2025", "reform", 12645, 38.72, 18.10),
        ]),
    )
    result = compute_reform_bias(snapshot, local_elections=None)
    assert result.n_events_used == 1
    assert result.n_events_with_polls == 1
    assert len(result.per_event) == 1
    e = result.per_event[0]
    assert e["event_id"] == "runcorn_helsby_2025"
    assert e["actual_share_pp"] == pytest.approx(38.72)
    assert e["poll_mean_share_pp"] == pytest.approx(12.0)
    assert e["bias_pp"] == pytest.approx(38.72 - 12.0)
    assert e["weight"] == 1.0
    assert result.aggregate_bias_pp == pytest.approx(38.72 - 12.0)
    assert result.recommended_reform_polling_correction_pp == pytest.approx(38.72 - 12.0)


def test_compute_reform_bias_excludes_events_without_polls_from_aggregate():
    """An event with zero polls in window stays in per_event (descriptive) but
    its bias_pp is None and it does NOT contribute to the aggregate."""
    snapshot = _StubSnapshot(
        polls=_polls_df([
            # Only event-1 has polls in window; event-2 has nothing.
            ("YouGov", "2025-04-25", "2025-04-27", "2025-04-28", 1500, "GB",
             20.0, 25.0, 12.0, 10.0, 8.0, 3.0, 1.0, 21.0),
        ]),
        byelections_events=_byelections_events_df([
            ("e1", "Event 1", "2025-05-01", "westminster_byelection",
             "england", "North West", "reform", False, ""),
            ("e2", "Event 2 (no polls)", "2024-09-15", "westminster_byelection",
             "england", "South East", "reform", False, ""),
        ]),
        byelections_results=_byelections_results_df([
            ("e1", "reform", 12645, 38.72, 18.10),
            ("e2", "reform", 5000, 22.00, 5.00),
        ]),
    )
    result = compute_reform_bias(snapshot, local_elections=None)
    assert result.n_events_used == 2
    assert result.n_events_with_polls == 1
    e2 = next(e for e in result.per_event if e["event_id"] == "e2")
    assert e2["bias_pp"] is None
    assert e2["n_polls_in_window"] == 0
    # Aggregate only uses e1
    assert result.aggregate_bias_pp == pytest.approx(38.72 - 10.0)


def test_compute_reform_bias_local_election_uses_consolidated_shares():
    """Local-election event uses pns.consolidated.shares['reform'] as actual."""
    snapshot = _StubSnapshot(
        polls=_polls_df([
            ("BBC", "2025-04-25", "2025-04-27", "2025-04-28", 1500, "GB",
             20.0, 25.0, 12.0, 14.0, 8.0, 3.0, 1.0, 17.0),
        ]),
        byelections_events=_byelections_events_df([]),
        byelections_results=_byelections_results_df([]),
    )
    local = [LocalElectionEvent(
        date=date(2025, 5, 1),
        name="May 2025",
        sources=[LocalElectionPNSSource(source="BBC", source_url="https://x", shares={"reform": 30.0})],
        consolidated_shares={"reform": 30.0},
        consolidated_method="sole_source",
        notes=None,
    )]
    result = compute_reform_bias(snapshot, local_elections=local)
    assert result.n_events_used == 1
    e = result.per_event[0]
    assert e["type"] == "local_election"
    assert e["actual_share_pp"] == 30.0
    assert e["bias_pp"] == pytest.approx(30.0 - 14.0)


def test_compute_reform_bias_per_pollster_decomposition():
    """Per-pollster bias: each pollster gets its own mean_bias_pp + n_events_with_polls."""
    snapshot = _StubSnapshot(
        polls=_polls_df([
            # Window for event on 2025-05-01: [2025-04-24, 2025-04-30]
            ("YouGov",        "2025-04-25", "2025-04-26", "2025-04-27", 1500, "GB",
             20.0, 25.0, 12.0, 11.0, 8.0, 3.0, 1.0, 20.0),
            ("More in Common","2025-04-25", "2025-04-26", "2025-04-28", 1500, "GB",
             20.0, 25.0, 12.0, 13.0, 8.0, 3.0, 1.0, 18.0),
        ]),
        byelections_events=_byelections_events_df([
            ("e1", "E1", "2025-05-01", "westminster_byelection",
             "england", "North West", "reform", False, ""),
        ]),
        byelections_results=_byelections_results_df([
            ("e1", "reform", 12000, 30.0, 18.0),
        ]),
    )
    result = compute_reform_bias(snapshot, local_elections=None)
    assert "yougov" in result.per_pollster
    assert "more_in_common" in result.per_pollster
    # YouGov polled reform=11, actual=30 -> bias = +19
    assert result.per_pollster["yougov"]["mean_bias_pp"] == pytest.approx(19.0)
    # More in Common polled reform=13, actual=30 -> bias = +17
    assert result.per_pollster["more_in_common"]["mean_bias_pp"] == pytest.approx(17.0)
    # Both saw 1 event -> low reliability
    assert result.per_pollster["yougov"]["reliability"] == "low"
    assert result.per_pollster["yougov"]["n_events_with_polls"] == 1


def test_compute_reform_bias_returns_empty_aggregate_when_no_events_have_polls():
    """If no events have any polls in window, aggregate_bias_pp is 0.0 and
    recommended is 0.0 - explicit no-op rather than NaN."""
    snapshot = _StubSnapshot(
        polls=_polls_df([]),
        byelections_events=_byelections_events_df([
            ("e1", "E1", "2025-05-01", "westminster_byelection",
             "england", "North West", "reform", False, ""),
        ]),
        byelections_results=_byelections_results_df([
            ("e1", "reform", 12000, 30.0, 18.0),
        ]),
    )
    result = compute_reform_bias(snapshot, local_elections=None)
    assert result.n_events_used == 1
    assert result.n_events_with_polls == 0
    assert result.aggregate_bias_pp == 0.0
    assert result.recommended_reform_polling_correction_pp == 0.0


def test_write_bias_json_roundtrips(tmp_path):
    """write_bias_json produces a file that re-loads and matches the schema in N9."""
    snapshot = _StubSnapshot(
        polls=_polls_df([
            ("YouGov", "2025-04-25", "2025-04-26", "2025-04-28", 1500, "GB",
             20.0, 25.0, 12.0, 12.0, 8.0, 3.0, 1.0, 19.0),
        ]),
        byelections_events=_byelections_events_df([
            ("e1", "E1", "2025-05-01", "westminster_byelection",
             "england", "North West", "reform", False, ""),
        ]),
        byelections_results=_byelections_results_df([
            ("e1", "reform", 12000, 30.0, 18.0),
        ]),
    )
    result = compute_reform_bias(snapshot, local_elections=None)
    out = tmp_path / "bias.json"
    write_bias_json(result, snapshot, local_elections_yaml_path=None, out_path=out)
    assert out.exists()
    j = json.loads(out.read_text(encoding="utf-8"))
    assert j["schema_version"] == 1
    assert j["derived_from_snapshot_hash"] == "deadbeef0123"
    assert j["derived_from_snapshot_as_of_date"] == "2026-04-26"
    assert j["derived_from_local_elections_yaml_sha256"] is None
    assert j["method"]["weights"] == EVENT_WEIGHTS
    assert j["aggregate"]["bias_pp"] == pytest.approx(18.0)
    assert j["aggregate"]["recommended_reform_polling_correction_pp"] == pytest.approx(18.0)
    assert len(j["per_event"]) == 1
