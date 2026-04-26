import logging
from functools import cached_property
from pathlib import Path

import pandas as pd
from data_engine.sqlite_io import open_snapshot_db, read_dataframe, read_manifest
from schema.snapshot import SnapshotManifest

logger = logging.getLogger(__name__)


class Snapshot:
    """Read-only typed wrapper around a Plan-A snapshot SQLite file.

    Tables are loaded lazily on first attribute access and cached for the
    lifetime of the Snapshot instance. The underlying file is opened once
    per attribute access; cached DataFrames are independent of the file
    handle so the file is never held open across calls.
    """

    def __init__(self, path: Path):
        self._path = Path(path)
        if not self._path.exists():
            raise FileNotFoundError(f"snapshot not found: {self._path}")

    @property
    def path(self) -> Path:
        return self._path

    @property
    def snapshot_id(self) -> str:
        return self._path.stem

    @cached_property
    def manifest(self) -> SnapshotManifest:
        with open_snapshot_db(self._path) as conn:
            return read_manifest(conn)

    def _read(self, table: str) -> pd.DataFrame:
        with open_snapshot_db(self._path) as conn:
            return read_dataframe(conn, table)

    @cached_property
    def polls(self) -> pd.DataFrame:
        return self._read("polls")

    @cached_property
    def results_2024(self) -> pd.DataFrame:
        return self._read("results_2024")

    @cached_property
    def byelections_events(self) -> pd.DataFrame:
        return self._read("byelections_events")

    @cached_property
    def byelections_results(self) -> pd.DataFrame:
        return self._read("byelections_results")

    @cached_property
    def transfer_weights(self) -> pd.DataFrame:
        return self._read("transfer_weights")

    @cached_property
    def transfer_weights_provenance(self) -> pd.DataFrame:
        return self._read("transfer_weights_provenance")

    def lookup_weight(self, nation: str, consolidator: str, source: str) -> float | None:
        """Return weight for (nation, consolidator, source) or None if absent."""
        tw = self.transfer_weights
        m = (tw["nation"] == nation) & (tw["consolidator"] == consolidator) & (tw["source"] == source)
        if not m.any():
            return None
        return float(tw.loc[m, "weight"].iloc[0])

    def consolidator_observed(self, nation: str, consolidator: str) -> bool:
        """True if any matrix cell exists for this (nation, consolidator)."""
        tw = self.transfer_weights
        return bool(((tw["nation"] == nation) & (tw["consolidator"] == consolidator)).any())

    def provenance_for_consolidator(self, nation: str, consolidator: str) -> list[str]:
        """Return contributing event_ids (sorted) for this (nation, consolidator)."""
        prov = self.transfer_weights_provenance
        m = (prov["nation"] == nation) & (prov["consolidator"] == consolidator)
        return sorted(prov.loc[m, "event_id"].astype(str).tolist())
