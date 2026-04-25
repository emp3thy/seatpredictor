import contextlib
import hashlib
import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Iterator

import pandas as pd
from schema.snapshot import SnapshotManifest


@contextlib.contextmanager
def open_snapshot_db(path: Path) -> Iterator[sqlite3.Connection]:
    """Open a SQLite connection; create file if missing. Commits on clean exit."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def write_dataframe(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> None:
    """Write DataFrame to table, replacing if it exists."""
    df.to_sql(table, conn, if_exists="replace", index=False)


def read_dataframe(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    """Read full table as DataFrame."""
    return pd.read_sql_query(f"SELECT * FROM {table}", conn)


def write_manifest(conn: sqlite3.Connection, manifest: SnapshotManifest) -> None:
    """Persist manifest to a single-row 'manifest' table."""
    payload = manifest.model_dump(mode="json")
    payload["source_versions"] = json.dumps(payload["source_versions"], sort_keys=True)
    df = pd.DataFrame([payload])
    write_dataframe(conn, "manifest", df)


def read_manifest(conn: sqlite3.Connection) -> SnapshotManifest:
    """Read the single-row manifest back into a SnapshotManifest."""
    df = read_dataframe(conn, "manifest")
    if len(df) != 1:
        raise ValueError(f"manifest table must have exactly 1 row, found {len(df)}")
    row = df.iloc[0].to_dict()
    row["source_versions"] = json.loads(row["source_versions"])
    return SnapshotManifest.model_validate(row)


def compute_input_hash(
    *,
    as_of_date: date,
    schema_version: int,
    source_versions: dict[str, str],
) -> str:
    """12-char hash over inputs that determine snapshot content. Stable across key order."""
    canonical = json.dumps(
        {
            "as_of_date": as_of_date.isoformat(),
            "schema_version": schema_version,
            "source_versions": dict(sorted(source_versions.items())),
        },
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
