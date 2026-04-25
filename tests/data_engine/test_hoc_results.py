from pathlib import Path
import pandas as pd
from data_engine.sources.hoc_results import parse_hoc_results
from schema.common import Nation, PartyCode


def test_parse_produces_one_row_per_constituency_party(fixtures_dir: Path):
    csv_bytes = (fixtures_dir / "hoc_results_sample.csv").read_bytes()
    df = parse_hoc_results(csv_bytes)
    # 5 constituencies × 8 parties = 40 rows (each party gets a row, share=0 if not present)
    assert len(df) == 5 * 8
    # All required columns
    assert set(df.columns) >= {
        "ons_code", "constituency_name", "region", "nation",
        "party", "votes", "share",
    }


def test_shares_in_each_constituency_sum_to_about_100(fixtures_dir: Path):
    csv_bytes = (fixtures_dir / "hoc_results_sample.csv").read_bytes()
    df = parse_hoc_results(csv_bytes)
    sums = df.groupby("ons_code")["share"].sum()
    for ons, total in sums.items():
        assert 99.0 <= total <= 101.0, f"{ons} sums to {total}"


def test_nation_values(fixtures_dir: Path):
    csv_bytes = (fixtures_dir / "hoc_results_sample.csv").read_bytes()
    df = parse_hoc_results(csv_bytes)
    nations = set(df["nation"].unique())
    assert nations <= {n.value for n in Nation}


def test_parties_use_party_codes(fixtures_dir: Path):
    csv_bytes = (fixtures_dir / "hoc_results_sample.csv").read_bytes()
    df = parse_hoc_results(csv_bytes)
    parties = set(df["party"].unique())
    assert parties <= {p.value for p in PartyCode}


def test_lab_votes_for_gorton(fixtures_dir: Path):
    csv_bytes = (fixtures_dir / "hoc_results_sample.csv").read_bytes()
    df = parse_hoc_results(csv_bytes)
    gorton_lab = df[(df["ons_code"] == "E14001234") & (df["party"] == "lab")]
    assert len(gorton_lab) == 1
    assert int(gorton_lab["votes"].iloc[0]) == 18234


def test_unknown_numeric_columns_roll_into_other(fixtures_dir: Path):
    """Workers Party isn't in _PARTY_ALIASES, so its votes should land in 'other'."""
    csv_bytes = (fixtures_dir / "hoc_results_sample.csv").read_bytes()
    df = parse_hoc_results(csv_bytes)
    # Gorton: Other (750) + Workers Party (350) = 1100 expected
    gorton_other = df[(df["ons_code"] == "E14001234") & (df["party"] == "other")]
    assert int(gorton_other["votes"].iloc[0]) == 1100


def test_of_which_subtotal_columns_excluded_from_rollup(fixtures_dir: Path):
    """Columns starting with 'of which' are sub-totals; must NOT be added into 'other'.
    North Down has 'Other'=21000 and 'of which other winner'=21000; the parser must
    not double-count the sub-total."""
    csv_bytes = (fixtures_dir / "hoc_results_sample.csv").read_bytes()
    df = parse_hoc_results(csv_bytes)
    nd_other = df[(df["ons_code"] == "N06000010") & (df["party"] == "other")]
    # Other (21000) + Workers Party (345) = 21345; "of which other winner" (21000) excluded
    assert int(nd_other["votes"].iloc[0]) == 21345
