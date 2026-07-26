from datetime import date
from pathlib import Path

import pytest
from data_engine.snapshot import build_snapshot, BuildSnapshotConfig, SCHEMA_VERSION
from data_engine.sqlite_io import open_snapshot_db, read_dataframe, read_manifest
from data_engine.raw_cache import RawCache
from data_engine.sources.wikipedia_polls import POLLS_URL


_REPO_ROOT = Path(__file__).resolve().parents[2]

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
        byelections_yaml=_REPO_ROOT / "data" / "hand_curated" / "by_elections.yaml",
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
        byelections_yaml=_REPO_ROOT / "data" / "hand_curated" / "by_elections.yaml",
    )
    path = build_snapshot(cfg)
    assert path.name.startswith(f"2026-04-25__v{SCHEMA_VERSION}__")
    assert path.suffix == ".sqlite"


def test_idempotent_rerun_returns_same_path(tmp_path: Path, primed_cache: RawCache):
    out = tmp_path / "snapshots"
    cfg = BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=_REPO_ROOT / "data" / "hand_curated" / "by_elections.yaml",
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
        byelections_yaml=_REPO_ROOT / "data" / "hand_curated" / "by_elections.yaml",
    ))
    p_dec = build_snapshot(BuildSnapshotConfig(
        as_of_date=date(2025, 12, 31),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=_REPO_ROOT / "data" / "hand_curated" / "by_elections.yaml",
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
        byelections_yaml=_REPO_ROOT / "data" / "hand_curated" / "by_elections.yaml",
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
        byelections_yaml=_REPO_ROOT / "data" / "hand_curated" / "by_elections.yaml",
        polls_geographies=("GB", "Wales"),
    )
    with pytest.raises(NotImplementedError, match="v1 supports"):
        build_snapshot(cfg)


def test_manifest_round_trip_matches_filename_hash(tmp_path: Path, primed_cache: RawCache):
    """A freshly-built snapshot's manifest.content_hash must equal the hash in its filename,
    and source_versions must round-trip as a dict[str,str] with the expected keys."""
    out = tmp_path / "snapshots"
    cfg = BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=_REPO_ROOT / "data" / "hand_curated" / "by_elections.yaml",
    )
    path = build_snapshot(cfg)
    # Filename is "<as_of>__v<schema>__<hash>.sqlite"; extract hash segment
    filename_hash = path.stem.split("__")[2]

    with open_snapshot_db(path) as conn:
        manifest = read_manifest(conn)

    assert manifest.content_hash == filename_hash
    assert isinstance(manifest.source_versions, dict)
    assert set(manifest.source_versions.keys()) == {
        "wikipedia_polls", "hoc_results", "byelections_yaml",
        "polls_geographies", "transfer_overrides_yaml",
    }
    for k, v in manifest.source_versions.items():
        assert isinstance(k, str)
        assert isinstance(v, str)


def test_overrides_absent_hashes_to_none(tmp_path: Path, primed_cache: RawCache):
    from data_engine.snapshot import _source_versions
    cfg = BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=tmp_path,
        byelections_yaml=_REPO_ROOT / "data" / "hand_curated" / "by_elections.yaml",
        transfer_overrides_yaml=None,
    )
    assert _source_versions(cfg)["transfer_overrides_yaml"] == "none"


def test_editing_overrides_changes_the_input_hash(tmp_path: Path, primed_cache: RawCache):
    """A curated weight change must invalidate the cached snapshot."""
    from data_engine.snapshot import _source_versions
    ovr = tmp_path / "transfer_overrides.yaml"

    def versions(weight: float) -> dict:
        ovr.write_text(
            "overrides:\n"
            "  - nation: england\n"
            "    consolidator: lab\n"
            "    source: con\n"
            f"    weight: {weight}\n"
            "    rationale: test\n",
            encoding="utf-8",
        )
        return _source_versions(BuildSnapshotConfig(
            as_of_date=date(2026, 4, 25),
            raw_cache=primed_cache,
            out_dir=tmp_path,
            byelections_yaml=_REPO_ROOT / "data" / "hand_curated" / "by_elections.yaml",
            transfer_overrides_yaml=ovr,
        ))

    assert versions(0.2)["transfer_overrides_yaml"] != versions(0.3)["transfer_overrides_yaml"]
