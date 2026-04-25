from datetime import date, datetime, timezone
from pathlib import Path
import pandas as pd
import pytest
import respx
import httpx
from data_engine.snapshot import build_snapshot, BuildSnapshotConfig, SCHEMA_VERSION
from data_engine.sqlite_io import open_snapshot_db, read_dataframe, read_manifest
from data_engine.raw_cache import RawCache
from data_engine.sources.wikipedia_polls import POLLS_URL


HOC_URL = "https://researchbriefings.files.parliament.uk/documents/CBP-10009/HoC-GE2024-results-by-constituency.csv"


@pytest.fixture
def primed_cache(tmp_path: Path, fixtures_dir: Path) -> RawCache:
    cache = RawCache(root=tmp_path / "raw_cache")
    today = date(2026, 4, 25)
    cache.put(
        cache.key("wikipedia_polls", today),
        (fixtures_dir / "wikipedia_polls_sample.html").read_bytes(),
        meta={"url": POLLS_URL},
    )
    cache.put(
        cache.key("hoc_results", today),
        (fixtures_dir / "hoc_results_sample.csv").read_bytes(),
        meta={"url": HOC_URL},
    )
    return cache


def test_builds_snapshot_with_all_tables(tmp_path: Path, primed_cache: RawCache):
    out = tmp_path / "snapshots"
    cfg = BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=Path("data/hand_curated/by_elections.yaml"),
    )
    path = build_snapshot(cfg)
    assert path.exists()
    with open_snapshot_db(path) as conn:
        polls = read_dataframe(conn, "polls")
        results_2024 = read_dataframe(conn, "results_2024")
        events = read_dataframe(conn, "byelections_events")
        ev_results = read_dataframe(conn, "byelections_results")
        weights = read_dataframe(conn, "transfer_weights")
        provenance = read_dataframe(conn, "transfer_weights_provenance")
        manifest = read_manifest(conn)
    assert len(polls) > 0
    assert len(results_2024) > 0
    assert len(events) >= 4
    assert len(ev_results) > 0
    assert len(weights) > 0
    assert len(provenance) > 0
    assert manifest.schema_version == SCHEMA_VERSION
    assert manifest.as_of_date == date(2026, 4, 25)


def test_snapshot_filename_includes_input_hash(tmp_path: Path, primed_cache: RawCache):
    out = tmp_path / "snapshots"
    cfg = BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=Path("data/hand_curated/by_elections.yaml"),
    )
    path = build_snapshot(cfg)
    assert path.name.startswith("2026-04-25__v1__")
    assert path.suffix == ".sqlite"


def test_idempotent_rerun_returns_same_path(tmp_path: Path, primed_cache: RawCache):
    out = tmp_path / "snapshots"
    cfg = BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=Path("data/hand_curated/by_elections.yaml"),
    )
    p1 = build_snapshot(cfg)
    p2 = build_snapshot(cfg)
    assert p1 == p2  # same content hash → same filename → reuse


def test_as_of_filter_changes_input_hash(tmp_path: Path, primed_cache: RawCache):
    # Prime cache for both dates
    primed_cache.put(
        primed_cache.key("wikipedia_polls", date(2025, 12, 31)),
        primed_cache.get_bytes(primed_cache.key("wikipedia_polls", date(2026, 4, 25))),
        meta={},
    )
    primed_cache.put(
        primed_cache.key("hoc_results", date(2025, 12, 31)),
        primed_cache.get_bytes(primed_cache.key("hoc_results", date(2026, 4, 25))),
        meta={},
    )
    out = tmp_path / "snapshots"
    p_apr = build_snapshot(BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=Path("data/hand_curated/by_elections.yaml"),
    ))
    p_dec = build_snapshot(BuildSnapshotConfig(
        as_of_date=date(2025, 12, 31),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=Path("data/hand_curated/by_elections.yaml"),
    ))
    assert p_apr != p_dec


def test_partial_failure_does_not_leave_corrupt_snapshot(
    tmp_path: Path, primed_cache: RawCache, monkeypatch
):
    """If write_dataframe raises mid-way, no .sqlite file should be left at the
    final path — only the .tmp is cleaned up. The next run can produce a clean
    snapshot."""
    out = tmp_path / "snapshots"
    cfg = BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=Path("data/hand_curated/by_elections.yaml"),
    )
    # Inject a failure in the third write_dataframe call
    from data_engine import snapshot as snapshot_mod
    original = snapshot_mod.write_dataframe
    call_count = {"n": 0}
    def boom(conn, table, df):
        call_count["n"] += 1
        if call_count["n"] == 3:
            raise RuntimeError("simulated mid-write failure")
        return original(conn, table, df)
    monkeypatch.setattr(snapshot_mod, "write_dataframe", boom)

    with pytest.raises(RuntimeError, match="simulated mid-write"):
        build_snapshot(cfg)

    # No .sqlite at final path; no .tmp leftover
    snapshots = list(out.glob("*.sqlite"))
    tmps = list(out.glob("*.tmp"))
    assert snapshots == [], f"Final-path snapshot should not exist: {snapshots}"
    assert tmps == [], f"Tmp file should be cleaned up: {tmps}"


def test_polls_geographies_v1_guard(tmp_path: Path, primed_cache: RawCache):
    """v1 only supports ('GB',); other tuples must raise NotImplementedError."""
    cfg = BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=tmp_path / "snapshots",
        byelections_yaml=Path("data/hand_curated/by_elections.yaml"),
        polls_geographies=("GB", "Wales"),
    )
    with pytest.raises(NotImplementedError, match="v1 supports"):
        build_snapshot(cfg)
