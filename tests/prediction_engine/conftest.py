from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
import yaml

from data_engine.sqlite_io import open_snapshot_db, write_dataframe, write_manifest
from data_engine.transforms.transfer_matrix import derive_transfer_matrix
from schema.snapshot import SnapshotManifest


_SEED_PATH = Path(__file__).parent.parent / "fixtures" / "tiny_snapshot_seed.yaml"
_AS_OF = date(2026, 4, 25)
_CONTENT_HASH = "tinyhash0001"


@pytest.fixture
def tiny_snapshot_path(tmp_path: Path) -> Path:
    """Build a real Plan-A-format SQLite snapshot from tiny_snapshot_seed.yaml.

    The transfer-weights matrix is derived by Plan A's derive_transfer_matrix
    so any future change there is reflected here automatically (no drift).
    """
    with _SEED_PATH.open(encoding="utf-8") as f:
        seed = yaml.safe_load(f)

    polls = pd.DataFrame(seed["polls"])
    for col in ("fieldwork_start", "fieldwork_end", "published_date"):
        polls[col] = polls[col].astype(str)

    results = pd.DataFrame(seed["results_2024"])

    events = pd.DataFrame(seed["byelections_events"])
    events["date"] = events["date"].astype(str)
    # derive_transfer_matrix expects the boolean as a real bool, not a string.
    events["exclude_from_matrix"] = events["exclude_from_matrix"].astype(bool)

    results_by = pd.DataFrame(seed["byelections_results"])

    cells, provenance = derive_transfer_matrix(events, results_by)

    out = tmp_path / f"{_AS_OF.isoformat()}__v1__{_CONTENT_HASH}.sqlite"
    with open_snapshot_db(out) as conn:
        write_dataframe(conn, "polls", polls)
        write_dataframe(conn, "results_2024", results)
        write_dataframe(conn, "byelections_events", events)
        write_dataframe(conn, "byelections_results", results_by)
        write_dataframe(conn, "transfer_weights", cells)
        write_dataframe(conn, "transfer_weights_provenance", provenance)
        manifest = SnapshotManifest(
            as_of_date=_AS_OF,
            schema_version=1,
            content_hash=_CONTENT_HASH,
            generated_at=datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc),
            source_versions={"wikipedia_polls": _AS_OF.isoformat(), "hoc_results": "ge_2024"},
        )
        write_manifest(conn, manifest)
    return out
