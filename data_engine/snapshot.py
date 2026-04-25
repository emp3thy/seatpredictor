from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
from data_engine.raw_cache import RawCache
from data_engine.sources.byelections import load_byelections
from data_engine.sources.hoc_results import parse_hoc_results
from data_engine.sources.wikipedia_polls import parse_polls_html
from data_engine.sqlite_io import (
    compute_input_hash,
    open_snapshot_db,
    write_dataframe,
    write_manifest,
)
from data_engine.transforms.transfer_matrix import derive_transfer_matrix
from schema.snapshot import SnapshotManifest


SCHEMA_VERSION = 1


@dataclass
class BuildSnapshotConfig:
    """Inputs for build_snapshot.

    polls_geographies: which geographies to include in the polls table. v1 default is
    ("GB",) only — the GB-wide page is the single fetch. Regional sub-pages
    (Scotland/Wales/London) require additional fetches; they're plumbed through this
    field but not wired up in v1's CLI to keep scope tight. Plan B may add them when
    a strategy actually consumes regional swing data.
    """
    as_of_date: date
    raw_cache: RawCache
    out_dir: Path
    byelections_yaml: Path
    polls_geographies: tuple[str, ...] = ("GB",)


def build_snapshot(cfg: BuildSnapshotConfig) -> Path:
    """Read raw cache, transform, write a single SQLite snapshot. Idempotent.

    If a snapshot with the same input hash already exists, returns that path
    without re-running the transform.
    """
    # Source versions feed into the input hash
    source_versions = _source_versions(cfg)
    input_hash = compute_input_hash(
        as_of_date=cfg.as_of_date,
        schema_version=SCHEMA_VERSION,
        source_versions=source_versions,
    )
    out_path = (
        cfg.out_dir
        / f"{cfg.as_of_date.isoformat()}__v{SCHEMA_VERSION}__{input_hash}.sqlite"
    )
    if out_path.exists():
        return out_path

    # Parse each source
    polls_df = _build_polls_df(cfg)
    results_df = _build_results_df(cfg)
    events_df, ev_results_df = load_byelections(cfg.byelections_yaml, as_of=cfg.as_of_date)
    cells_df, provenance_df = derive_transfer_matrix(events_df, ev_results_df)

    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    with open_snapshot_db(out_path) as conn:
        write_dataframe(conn, "polls", polls_df)
        write_dataframe(conn, "results_2024", results_df)
        write_dataframe(conn, "byelections_events", events_df)
        write_dataframe(conn, "byelections_results", ev_results_df)
        write_dataframe(conn, "transfer_weights", cells_df)
        write_dataframe(conn, "transfer_weights_provenance", provenance_df)
        manifest = SnapshotManifest(
            as_of_date=cfg.as_of_date,
            schema_version=SCHEMA_VERSION,
            content_hash=input_hash,
            generated_at=datetime.now(tz=timezone.utc),
            source_versions=source_versions,
        )
        write_manifest(conn, manifest)
    return out_path


def _build_polls_df(cfg: BuildSnapshotConfig) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for geo in cfg.polls_geographies:
        key = cfg.raw_cache.key("wikipedia_polls", cfg.as_of_date)
        if not cfg.raw_cache.exists(key):
            raise FileNotFoundError(
                f"raw cache miss for wikipedia_polls@{cfg.as_of_date}; run `seatpredict-data fetch` first"
            )
        html = cfg.raw_cache.get_bytes(key).decode("utf-8")
        df = parse_polls_html(html, geography=geo)
        parts.append(df)
    polls = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if not polls.empty:
        polls = polls[polls["published_date"] <= cfg.as_of_date.isoformat()]
    return polls


def _build_results_df(cfg: BuildSnapshotConfig) -> pd.DataFrame:
    key = cfg.raw_cache.key("hoc_results", cfg.as_of_date)
    if not cfg.raw_cache.exists(key):
        raise FileNotFoundError(
            f"raw cache miss for hoc_results@{cfg.as_of_date}; run `seatpredict-data fetch` first"
        )
    csv_bytes = cfg.raw_cache.get_bytes(key)
    return parse_hoc_results(csv_bytes)


def _source_versions(cfg: BuildSnapshotConfig) -> dict[str, str]:
    yaml_bytes = cfg.byelections_yaml.read_bytes()
    import hashlib
    yaml_hash = hashlib.sha256(yaml_bytes).hexdigest()[:12]
    return {
        "wikipedia_polls": cfg.as_of_date.isoformat(),
        "hoc_results": "ge_2024",
        "byelections_yaml": yaml_hash,
        "polls_geographies": ",".join(cfg.polls_geographies),
    }
