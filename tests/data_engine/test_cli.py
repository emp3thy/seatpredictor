from datetime import date
from pathlib import Path
import shutil
import pytest
from click.testing import CliRunner
from data_engine.cli import main


@pytest.fixture
def primed_repo(tmp_path: Path, fixtures_dir: Path) -> Path:
    """Create a temp project root with raw cache primed and the YAML present."""
    root = tmp_path / "project"
    root.mkdir()
    raw = root / "data" / "raw_cache"
    today = date(2026, 4, 25)
    src_dir = raw / "wikipedia_polls" / today.isoformat()
    src_dir.mkdir(parents=True)
    (src_dir / "content.bin").write_bytes(
        (fixtures_dir / "wikipedia_polls_sample.html").read_bytes()
    )
    (src_dir / "meta.json").write_text("{}")
    src_dir = raw / "hoc_results" / today.isoformat()
    src_dir.mkdir(parents=True)
    (src_dir / "content.bin").write_bytes(
        (fixtures_dir / "hoc_results_sample.csv").read_bytes()
    )
    (src_dir / "meta.json").write_text("{}")
    hand = root / "data" / "hand_curated"
    hand.mkdir(parents=True)
    shutil.copy(Path("data/hand_curated/by_elections.yaml"), hand / "by_elections.yaml")
    return root


def test_cli_snapshot_creates_file(primed_repo: Path, monkeypatch):
    monkeypatch.chdir(primed_repo)
    runner = CliRunner()
    result = runner.invoke(main, ["snapshot", "--as-of", "2026-04-25"])
    assert result.exit_code == 0, result.output
    snaps = list((primed_repo / "data" / "snapshots").glob("*.sqlite"))
    assert len(snaps) == 1
    assert "2026-04-25__v1__" in snaps[0].name


def test_cli_snapshot_is_idempotent(primed_repo: Path, monkeypatch):
    monkeypatch.chdir(primed_repo)
    runner = CliRunner()
    runner.invoke(main, ["snapshot", "--as-of", "2026-04-25"])
    snaps_before = sorted((primed_repo / "data" / "snapshots").glob("*.sqlite"))
    runner.invoke(main, ["snapshot", "--as-of", "2026-04-25"])
    snaps_after = sorted((primed_repo / "data" / "snapshots").glob("*.sqlite"))
    assert snaps_before == snaps_after  # no new file


def test_cli_help_lists_subcommands():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert "fetch" in result.output
    assert "snapshot" in result.output
