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
