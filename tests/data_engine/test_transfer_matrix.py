import pandas as pd
import pytest
from data_engine.transforms.transfer_matrix import (
    derive_transfer_matrix,
    PRIOR_SHARE_THRESHOLD,
)
from schema.common import PartyCode, Nation


def _fake_byelections() -> tuple[pd.DataFrame, pd.DataFrame]:
    # One Welsh event: Caerphilly-style. Reform threat. Plaid consolidator (+19pp).
    # Lab fell from 46 → 11 (flow rate 35/46 ≈ 0.761).
    # LD fell from 2.4 → 1.2 (1.2/2.4 = 0.5).
    # Con fell from 17.3 → 2.5 (14.8/17.3 ≈ 0.855).
    # Green prior 1.3% — below threshold, excluded.
    events = pd.DataFrame([{
        "event_id": "caer_test",
        "name": "Caer test",
        "date": "2025-10-23",
        "event_type": "senedd",
        "nation": "wales",
        "region": "X",
        "threat_party": "reform",
        "exclude_from_matrix": False,
        "narrative_url": None,
    }])
    results = pd.DataFrame([
        {"event_id": "caer_test", "party": "plaid",  "votes": 0, "actual_share": 47.4, "prior_share": 28.4},
        {"event_id": "caer_test", "party": "reform", "votes": 0, "actual_share": 36.0, "prior_share":  1.7},
        {"event_id": "caer_test", "party": "lab",    "votes": 0, "actual_share": 11.0, "prior_share": 46.0},
        {"event_id": "caer_test", "party": "con",    "votes": 0, "actual_share":  2.5, "prior_share": 17.3},
        {"event_id": "caer_test", "party": "ld",     "votes": 0, "actual_share":  1.2, "prior_share":  2.4},
        {"event_id": "caer_test", "party": "green",  "votes": 0, "actual_share":  1.0, "prior_share":  1.3},
    ])
    return events, results


def test_derives_consolidator_from_biggest_left_bloc_gainer():
    events, results = _fake_byelections()
    cells, prov = derive_transfer_matrix(events, results)
    assert (cells["consolidator"] == "plaid").all()


def test_lab_to_plaid_flow_rate():
    """Flow is the source party's proportional loss scaled by the consolidator's
    share of the freed vote.

    caer_test: plaid gains 19.0. Losses across every non-consolidator party are
    lab 35.0 + con 14.8 + ld 1.2 + green 0.3 = 51.3. scale = 19.0 / 51.3.
    """
    events, results = _fake_byelections()
    cells, _ = derive_transfer_matrix(events, results)
    lab_row = cells[(cells["consolidator"] == "plaid") & (cells["source"] == "lab")].iloc[0]
    scale = 19.0 / 51.3
    expected = ((46.0 - 11.0) / 46.0) * scale
    assert abs(lab_row["weight"] - expected) < 1e-6
    assert abs(lab_row["weight"] - 0.2818035) < 1e-6
    assert lab_row["nation"] == "wales"
    assert lab_row["n"] == 1


def test_every_source_scaled_by_the_same_factor():
    """Relative ordering between sources within an event is preserved."""
    events, results = _fake_byelections()
    cells, _ = derive_transfer_matrix(events, results)
    by_source = cells.set_index("source")["weight"]
    assert abs(by_source["lab"] - 0.2818035) < 1e-6
    assert abs(by_source["con"] - 0.3168486) < 1e-6
    assert abs(by_source["ld"] - 0.1851852) < 1e-6


def test_transferred_total_cannot_exceed_consolidator_gain():
    """Sum of moved vote equals the consolidator's actual gain, within float error."""
    events, results = _fake_byelections()
    cells, _ = derive_transfer_matrix(events, results)
    priors = {"lab": 46.0, "con": 17.3, "ld": 2.4}
    moved = sum(
        priors[row["source"]] * row["weight"]
        for _, row in cells.iterrows()
        if row["source"] in priors
    )
    plaid_gain = 47.4 - 28.4
    assert moved <= plaid_gain + 1e-6
    assert abs(moved - plaid_gain) < 0.5  # green's sub-threshold loss is the only shortfall


def test_scale_clipped_to_one_when_gain_exceeds_loss():
    """A consolidator gaining more than the total loss (turnout effects) cannot
    produce a weight above the unscaled proportional loss."""
    events, results = _fake_byelections()
    results.loc[results["party"] == "plaid", "actual_share"] = 95.0
    cells, _ = derive_transfer_matrix(events, results)
    lab_row = cells[cells["source"] == "lab"].iloc[0]
    assert abs(lab_row["weight"] - (46.0 - 11.0) / 46.0) < 1e-6


def test_shrinking_reform_counted_in_total_loss():
    """Reform is never a flow source, but a shrinking Reform still enlarges the
    denominator and therefore shrinks every weight."""
    events, results = _fake_byelections()
    baseline, _ = derive_transfer_matrix(events, results)
    baseline_lab = baseline[baseline["source"] == "lab"].iloc[0]["weight"]

    results.loc[results["party"] == "reform", "prior_share"] = 46.0
    results.loc[results["party"] == "reform", "actual_share"] = 36.0
    shrunk, _ = derive_transfer_matrix(events, results)
    shrunk_lab = shrunk[shrunk["source"] == "lab"].iloc[0]["weight"]

    assert shrunk_lab < baseline_lab
    assert len(shrunk[shrunk["source"] == "reform"]) == 0


def test_no_flows_when_nothing_shrank():
    """total_loss == 0 returns no cells rather than dividing by zero."""
    events, results = _fake_byelections()
    results["actual_share"] = results["prior_share"]
    results.loc[results["party"] == "plaid", "actual_share"] = 30.0
    cells, _ = derive_transfer_matrix(events, results)
    assert len(cells) == 0


def test_below_threshold_source_excluded():
    events, results = _fake_byelections()
    cells, _ = derive_transfer_matrix(events, results)
    # Green's prior 1.3% < threshold (2%) → no row for green-as-source
    green_rows = cells[cells["source"] == "green"]
    assert len(green_rows) == 0


def test_provenance_links_cell_to_event():
    events, results = _fake_byelections()
    _, prov = derive_transfer_matrix(events, results)
    plaid_prov = prov[(prov["nation"] == "wales") & (prov["consolidator"] == "plaid")]
    assert "caer_test" in set(plaid_prov["event_id"])


def test_event_excluded_when_excluded_flag_true():
    events, results = _fake_byelections()
    events.loc[0, "exclude_from_matrix"] = True
    cells, _ = derive_transfer_matrix(events, results)
    assert len(cells) == 0


def test_event_excluded_when_threat_not_reform():
    events, results = _fake_byelections()
    events.loc[0, "threat_party"] = "con"
    cells, _ = derive_transfer_matrix(events, results)
    assert len(cells) == 0


def test_two_events_average():
    events, results = _fake_byelections()
    # Add a second English event with Lab as consolidator and Green→Lab observed flow.
    events2 = pd.DataFrame([{
        "event_id": "ev2",
        "name": "ev2",
        "date": "2026-02-26",
        "event_type": "westminster_byelection",
        "nation": "england",
        "region": "X",
        "threat_party": "reform",
        "exclude_from_matrix": False,
        "narrative_url": None,
    }])
    results2 = pd.DataFrame([
        {"event_id": "ev2", "party": "lab",    "votes": 0, "actual_share": 50.0, "prior_share": 30.0},
        {"event_id": "ev2", "party": "reform", "votes": 0, "actual_share": 30.0, "prior_share": 10.0},
        {"event_id": "ev2", "party": "green",  "votes": 0, "actual_share":  5.0, "prior_share": 20.0},
        {"event_id": "ev2", "party": "ld",     "votes": 0, "actual_share":  5.0, "prior_share":  20.0},
        {"event_id": "ev2", "party": "con",    "votes": 0, "actual_share": 10.0, "prior_share": 20.0},
    ])
    events_all = pd.concat([events, events2], ignore_index=True)
    results_all = pd.concat([results, results2], ignore_index=True)
    cells, _ = derive_transfer_matrix(events_all, results_all)
    england_lab = cells[(cells["nation"] == "england") & (cells["consolidator"] == "lab")]
    assert set(england_lab["source"]) == {"green", "ld", "con"}


def test_restore_never_a_flow_source():
    """Restore sits right of Reform — even with a large prior share it must not
    appear as a tactical-flow source in the matrix."""
    events, results = _fake_byelections()
    results = pd.concat([results, pd.DataFrame([
        {"event_id": "caer_test", "party": "restore", "votes": 0,
         "actual_share": 0.9, "prior_share": 3.2},
    ])], ignore_index=True)
    cells, _ = derive_transfer_matrix(events, results)
    assert len(cells[cells["source"] == "restore"]) == 0


def _overrides(*rows) -> pd.DataFrame:
    return pd.DataFrame(
        list(rows),
        columns=["nation", "consolidator", "source", "weight", "rationale"],
    )


def test_override_replaces_derived_cell():
    events, results = _fake_byelections()
    ovr = _overrides(("wales", "plaid", "lab", 0.15, "curated"))
    cells, _ = derive_transfer_matrix(events, results, overrides=ovr)
    row = cells[(cells["consolidator"] == "plaid") & (cells["source"] == "lab")].iloc[0]
    assert row["weight"] == pytest.approx(0.15)
    assert row["n"] == 0


def test_override_creates_absent_cell():
    """green's prior is below threshold so no derived cell exists; the override
    creates one."""
    events, results = _fake_byelections()
    ovr = _overrides(("wales", "plaid", "green", 0.4, "curated"))
    cells, _ = derive_transfer_matrix(events, results, overrides=ovr)
    row = cells[(cells["consolidator"] == "plaid") & (cells["source"] == "green")].iloc[0]
    assert row["weight"] == pytest.approx(0.4)
    assert row["n"] == 0


def test_non_overridden_cells_keep_derived_values():
    events, results = _fake_byelections()
    baseline, _ = derive_transfer_matrix(events, results)
    baseline_con = baseline[baseline["source"] == "con"].iloc[0]["weight"]

    ovr = _overrides(("wales", "plaid", "lab", 0.15, "curated"))
    cells, _ = derive_transfer_matrix(events, results, overrides=ovr)
    con_row = cells[cells["source"] == "con"].iloc[0]
    assert con_row["weight"] == pytest.approx(baseline_con)
    assert con_row["n"] == 1


def test_override_adds_hand_curated_provenance():
    events, results = _fake_byelections()
    ovr = _overrides(
        ("wales", "plaid", "lab", 0.15, "curated"),
        ("wales", "plaid", "con", 0.25, "curated"),
    )
    _, prov = derive_transfer_matrix(events, results, overrides=ovr)
    rows = prov[(prov["nation"] == "wales") & (prov["consolidator"] == "plaid")]
    hand = rows[rows["event_id"] == "hand_curated"]
    assert len(hand) == 1  # one row per (nation, consolidator), not per cell
    assert "caer_test" in set(rows["event_id"])  # derived provenance survives


def test_override_for_consolidator_with_no_derived_cells():
    """An override can stand up a whole block the data never produced."""
    events, results = _fake_byelections()
    ovr = _overrides(("england", "lab", "con", 0.2, "curated"))
    cells, prov = derive_transfer_matrix(events, results, overrides=ovr)
    row = cells[(cells["nation"] == "england") & (cells["consolidator"] == "lab")].iloc[0]
    assert row["source"] == "con"
    assert row["weight"] == pytest.approx(0.2)
    assert row["n"] == 0
    assert ("england", "lab", "hand_curated") in set(
        zip(prov["nation"], prov["consolidator"], prov["event_id"])
    )


def test_overrides_none_reproduces_current_behaviour():
    events, results = _fake_byelections()
    a, pa = derive_transfer_matrix(events, results)
    b, pb = derive_transfer_matrix(events, results, overrides=None)
    pd.testing.assert_frame_equal(a, b)
    pd.testing.assert_frame_equal(pa, pb)


def test_overrides_apply_even_when_no_events_are_eligible():
    """Overrides are not conditional on the derivation producing anything."""
    events, results = _fake_byelections()
    events.loc[0, "exclude_from_matrix"] = True
    ovr = _overrides(("england", "lab", "con", 0.2, "curated"))
    cells, prov = derive_transfer_matrix(events, results, overrides=ovr)
    assert len(cells) == 1
    assert cells.iloc[0]["weight"] == pytest.approx(0.2)
    assert cells.iloc[0]["n"] == 0
    assert len(prov) == 1
    assert prov.iloc[0]["event_id"] == "hand_curated"
    # An override standing up a cell on the empty-derivation path must still
    # produce numeric dtypes, not object dtype from the empty starting frame.
    assert pd.api.types.is_float_dtype(cells["weight"])
    assert pd.api.types.is_integer_dtype(cells["n"])
