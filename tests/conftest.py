import pytest
from pathlib import Path


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def tmp_snapshot_path(tmp_path: Path) -> Path:
    return tmp_path / "test_snapshot.sqlite"
