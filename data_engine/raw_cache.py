import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class CacheKey:
    source: str
    fetch_date: date

    def relpath(self) -> str:
        return f"{self.source}/{self.fetch_date.isoformat()}"


class RawCache:
    """File-based cache for fetched artifacts. Idempotent per (source, fetch_date)."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def key(self, source: str, fetch_date: date) -> CacheKey:
        return CacheKey(source=source, fetch_date=fetch_date)

    def _dir(self, key: CacheKey) -> Path:
        return self.root / key.relpath()

    def exists(self, key: CacheKey) -> bool:
        return (self._dir(key) / "content.bin").exists()

    def put(self, key: CacheKey, data: bytes, meta: dict) -> None:
        """Store content + metadata atomically. fetched_at is stamped by the cache
        and overrides any caller-supplied value."""
        d = self._dir(key)
        d.mkdir(parents=True, exist_ok=True)
        meta_with_ts = {**meta, "fetched_at": datetime.now(tz=timezone.utc).isoformat()}
        # Write meta first so exists() (which checks content.bin) only flips True
        # once both files are present — avoids partial-write windows.
        (d / "meta.json").write_text(
            json.dumps(meta_with_ts, sort_keys=True, indent=2), encoding="utf-8"
        )
        (d / "content.bin").write_bytes(data)

    def get_bytes(self, key: CacheKey) -> bytes:
        return (self._dir(key) / "content.bin").read_bytes()

    def get_meta(self, key: CacheKey) -> dict:
        return json.loads((self._dir(key) / "meta.json").read_text(encoding="utf-8"))
