"""Sanity checks on the tiny snapshot fixture.

Plan B's downstream tests rely on the seed producing a specific matrix and seat layout.
These checks fail loudly if the YAML is edited in a way that breaks the assumptions
documented in Task 2's design constraints.
"""
import sqlite3
from contextlib import closing

import pandas as pd
import pytest


def _read(path, table):
    with closing(sqlite3.connect(str(path))) as conn:
        return pd.read_sql_query(f"SELECT * FROM {table}", conn)


def test_fixture_has_six_seats(tiny_snapshot_path):
    r = _read(tiny_snapshot_path, "results_2024")
    assert sorted(r["ons_code"].unique()) == [
        "TST00001", "TST00002", "TST00003", "TST00004", "TST00005", "TST00006",
    ]


def test_fixture_each_seat_sums_to_100(tiny_snapshot_path):
    r = _read(tiny_snapshot_path, "results_2024")
    sums = r.groupby("ons_code")["share"].sum()
    for ons, total in sums.items():
        assert total == pytest.approx(100.0, abs=0.5), f"{ons}: {total}"


def test_fixture_matrix_has_expected_cells(tiny_snapshot_path):
    tw = _read(tiny_snapshot_path, "transfer_weights")
    keys = sorted(zip(tw["nation"], tw["consolidator"], tw["source"]))
    assert ("england", "lab", "ld")    in keys
    assert ("england", "lab", "green") in keys
    assert ("england", "lab", "con")   in keys
    assert ("wales",   "plaid", "lab") in keys
    assert ("wales",   "plaid", "ld")  in keys
    # Scotland deliberately empty.
    assert not any(n == "scotland" for n, _, _ in keys)


def test_fixture_matrix_weights_are_correct(tiny_snapshot_path):
    """Verify the derived matrix matches the hand-computed flows from the design constraint."""
    tw = _read(tiny_snapshot_path, "transfer_weights").set_index(
        ["nation", "consolidator", "source"]
    )
    assert tw.loc[("england", "lab", "ld"),    "weight"] == pytest.approx(0.6,  abs=1e-6)
    assert tw.loc[("england", "lab", "green"), "weight"] == pytest.approx(0.5,  abs=1e-6)
    assert tw.loc[("england", "lab", "con"),   "weight"] == pytest.approx(0.4,  abs=1e-6)
    assert tw.loc[("wales", "plaid", "lab"),   "weight"] == pytest.approx(0.6,  abs=1e-6)
    assert tw.loc[("wales", "plaid", "green"), "weight"] == pytest.approx(0.5,  abs=1e-6)
    assert tw.loc[("wales", "plaid", "con"),   "weight"] == pytest.approx(0.6,  abs=1e-6)
    assert tw.loc[("wales", "plaid", "ld"),    "weight"] == pytest.approx(2/3,  abs=1e-3)


def test_fixture_provenance_links_back_to_events(tiny_snapshot_path):
    prov = _read(tiny_snapshot_path, "transfer_weights_provenance")
    pairs = sorted(zip(prov["nation"], prov["consolidator"], prov["event_id"]))
    assert ("england", "lab",   "tst_eng_2025") in pairs
    assert ("wales",   "plaid", "tst_wal_2025") in pairs
