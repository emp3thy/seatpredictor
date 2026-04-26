from datetime import date
import pandas as pd
import pytest
from prediction_engine.snapshot_loader import Snapshot


def test_snapshot_loads_manifest(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    assert snap.manifest.as_of_date == date(2026, 4, 25)
    assert snap.manifest.content_hash == "tinyhash0001"


def test_snapshot_id_is_filename_stem(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    assert snap.snapshot_id == tiny_snapshot_path.stem


def test_snapshot_polls_lazy_load(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    polls = snap.polls
    assert isinstance(polls, pd.DataFrame)
    assert len(polls) == 2
    # Lazy: same object on repeat access (cached).
    assert snap.polls is polls


def test_snapshot_results_2024(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    r = snap.results_2024
    assert set(r["ons_code"].unique()) == {"TST00001", "TST00002", "TST00003", "TST00004", "TST00005", "TST00006"}
    assert set(r.columns) >= {"ons_code", "constituency_name", "region", "nation", "party", "votes", "share"}


def test_snapshot_transfer_weights_long_format(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    tw = snap.transfer_weights
    assert set(tw.columns) >= {"nation", "consolidator", "source", "weight", "n"}
    assert len(tw) > 0


def test_snapshot_lookup_weight(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    # england/lab/ld is in the seed at 0.6
    assert snap.lookup_weight("england", "lab", "ld") == pytest.approx(0.6)
    # england/lab/snp not seeded → None
    assert snap.lookup_weight("england", "lab", "snp") is None
    # scotland has no consolidator entries → None
    assert snap.lookup_weight("scotland", "lab", "ld") is None


def test_snapshot_consolidator_observed(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    assert snap.consolidator_observed("england", "lab") is True
    assert snap.consolidator_observed("scotland", "lab") is False
    assert snap.consolidator_observed("wales", "plaid") is True


def test_snapshot_provenance_for_consolidator(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    events = snap.provenance_for_consolidator("england", "lab")
    assert events == ["tst_eng_2025"]


def test_snapshot_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        Snapshot(tmp_path / "nonexistent.sqlite")


def test_snapshot_manifest_cached(tiny_snapshot_path):
    """Confirm `manifest` is cached_property: repeat access returns the same instance."""
    snap = Snapshot(tiny_snapshot_path)
    m1 = snap.manifest
    assert snap.manifest is m1
