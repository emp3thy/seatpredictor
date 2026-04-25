from datetime import date
from pathlib import Path
import pytest
from data_engine.raw_cache import RawCache


def test_cache_miss_then_hit(tmp_path: Path):
    cache = RawCache(root=tmp_path)
    key = cache.key("wikipedia_polls", date(2026, 4, 25))
    assert not cache.exists(key)
    cache.put(key, b"<html>polls</html>", meta={"url": "https://example.com"})
    assert cache.exists(key)
    data = cache.get_bytes(key)
    assert data == b"<html>polls</html>"
    meta = cache.get_meta(key)
    assert meta["url"] == "https://example.com"
    assert "fetched_at" in meta


def test_cache_keys_distinguish_source_and_date(tmp_path: Path):
    cache = RawCache(root=tmp_path)
    k1 = cache.key("wikipedia_polls", date(2026, 4, 25))
    k2 = cache.key("wikipedia_polls", date(2026, 4, 26))
    k3 = cache.key("hoc_results", date(2026, 4, 25))
    assert k1 != k2
    assert k1 != k3


def test_force_refresh(tmp_path: Path):
    cache = RawCache(root=tmp_path)
    key = cache.key("hoc_results", date(2026, 4, 25))
    cache.put(key, b"v1", meta={})
    assert cache.get_bytes(key) == b"v1"
    cache.put(key, b"v2", meta={})  # overwrites
    assert cache.get_bytes(key) == b"v2"
