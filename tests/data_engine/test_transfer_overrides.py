from pathlib import Path

import pytest
from pydantic import ValidationError

from data_engine.sources.transfer_overrides import load_transfer_overrides


_VALID = """
overrides:
  - nation: england
    consolidator: lab
    source: con
    weight: 0.2
    rationale: Most of the collapsing Conservative vote goes to Reform.
  - nation: england
    consolidator: lab
    source: green
    weight: 0.7
    rationale: Green voters consolidate readily but not universally.
"""


def _write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "transfer_overrides.yaml"
    p.write_text(text, encoding="utf-8")
    return p


def test_loads_valid_file(tmp_path):
    df = load_transfer_overrides(_write(tmp_path, _VALID))
    assert list(df.columns) == ["nation", "consolidator", "source", "weight", "rationale"]
    assert len(df) == 2
    con = df[df["source"] == "con"].iloc[0]
    assert con["nation"] == "england"
    assert con["consolidator"] == "lab"
    assert con["weight"] == pytest.approx(0.2)
    assert "Reform" in con["rationale"]


def test_missing_file_returns_empty_frame(tmp_path):
    df = load_transfer_overrides(tmp_path / "does_not_exist.yaml")
    assert df.empty
    assert list(df.columns) == ["nation", "consolidator", "source", "weight", "rationale"]


def test_empty_overrides_list_returns_empty_frame(tmp_path):
    df = load_transfer_overrides(_write(tmp_path, "overrides: []\n"))
    assert df.empty
    assert list(df.columns) == ["nation", "consolidator", "source", "weight", "rationale"]


def test_duplicate_key_raises(tmp_path):
    text = _VALID + """
  - nation: england
    consolidator: lab
    source: con
    weight: 0.9
    rationale: Contradicts the first entry.
"""
    with pytest.raises(ValueError, match="duplicate"):
        load_transfer_overrides(_write(tmp_path, text))


def test_missing_rationale_rejected(tmp_path):
    text = """
overrides:
  - nation: england
    consolidator: lab
    source: con
    weight: 0.2
"""
    with pytest.raises(ValidationError):
        load_transfer_overrides(_write(tmp_path, text))


def test_weight_out_of_range_rejected(tmp_path):
    text = """
overrides:
  - nation: england
    consolidator: lab
    source: con
    weight: 1.4
    rationale: Too big.
"""
    with pytest.raises(ValidationError):
        load_transfer_overrides(_write(tmp_path, text))


def test_unknown_nation_rejected(tmp_path):
    text = """
overrides:
  - nation: mercia
    consolidator: lab
    source: con
    weight: 0.2
    rationale: Not a nation.
"""
    with pytest.raises(ValidationError):
        load_transfer_overrides(_write(tmp_path, text))
