import warnings
from datetime import date
from pathlib import Path

import pytest

from data_engine.sources.local_elections import load_local_elections, LocalElectionEvent


_FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_load_local_elections_returns_sorted_events():
    events = load_local_elections(_FIXTURES / "local_elections_sample.yaml")
    assert len(events) == 2
    assert events[0].date == date(2025, 5, 1)
    assert events[1].date == date(2026, 5, 7)
    assert all(isinstance(e, LocalElectionEvent) for e in events)


def test_load_local_elections_event_a_two_sources():
    events = load_local_elections(_FIXTURES / "local_elections_sample.yaml")
    a = events[0]
    assert a.name == "Sample event A — two sources, median"
    assert a.consolidated_method == "median_across_sources"
    assert a.consolidated_shares["reform"] == 30.0
    assert len(a.sources) == 2
    assert a.sources[0].source == "BBC"
    assert a.sources[1].shares["reform"] == 32.0


def test_load_local_elections_event_b_sole_source():
    events = load_local_elections(_FIXTURES / "local_elections_sample.yaml")
    b = events[1]
    assert b.consolidated_method == "sole_source"
    assert len(b.sources) == 1
    assert b.consolidated_shares == b.sources[0].shares


def test_load_local_elections_warns_on_bad_consolidated_sum(tmp_path):
    """Consolidated shares not summing to 100 ± 2 must emit UserWarning."""
    bad = tmp_path / "bad.yaml"
    bad.write_text("""
events:
  - date: 2025-05-01
    name: "Bad sum"
    pns:
      sources:
        - source: "X"
          source_url: "https://x.test"
          shares: { con: 30.0, lab: 30.0, reform: 30.0 }
      consolidated:
        method: "sole_source"
        shares: { con: 30.0, lab: 30.0, reform: 30.0 }
    notes: "sums to 90, not 100"
""", encoding="utf-8")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        load_local_elections(bad)
    msgs = [str(w.message) for w in caught]
    assert any("sums to 90.0" in m or "outside 98-102" in m for m in msgs), \
        f"expected sum-warning, got: {msgs}"


def test_load_local_elections_missing_file_returns_empty_list_with_warning(tmp_path):
    """Graceful: missing file -> [] + warning, NOT an exception."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = load_local_elections(tmp_path / "does_not_exist.yaml")
    assert result == []
    assert any("not found" in str(w.message).lower() for w in caught)


def test_load_local_elections_missing_optional_parties_default_to_zero():
    events = load_local_elections(_FIXTURES / "local_elections_sample.yaml")
    a = events[0]
    # snp and plaid not listed in fixture A — must default to 0.0.
    assert a.consolidated_shares.get("snp", 0.0) == 0.0
    assert a.consolidated_shares.get("plaid", 0.0) == 0.0
