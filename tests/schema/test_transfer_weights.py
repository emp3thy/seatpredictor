import pytest
from pydantic import ValidationError
from schema.transfer_weights import TransferWeightCell, TransferWeightProvenance, TransferWeightOverride
from schema.common import PartyCode, Nation


def test_cell_valid():
    cell = TransferWeightCell(
        nation=Nation.WALES,
        consolidator=PartyCode.PLAID,
        source=PartyCode.LAB,
        weight=0.6,
        n=1,
    )
    assert cell.weight == 0.6


def test_cell_weight_in_unit_interval():
    with pytest.raises(ValidationError):
        TransferWeightCell(
            nation=Nation.WALES,
            consolidator=PartyCode.PLAID,
            source=PartyCode.LAB,
            weight=1.2,
            n=1,
        )
    with pytest.raises(ValidationError):
        TransferWeightCell(
            nation=Nation.WALES,
            consolidator=PartyCode.PLAID,
            source=PartyCode.LAB,
            weight=-0.1,
            n=1,
        )


def test_cell_n_may_be_zero_for_hand_curated():
    """n == 0 marks a hand-curated cell with no supporting by-election events."""
    cell = TransferWeightCell(
        nation=Nation.ENGLAND,
        consolidator=PartyCode.LAB,
        source=PartyCode.CON,
        weight=0.2,
        n=0,
    )
    assert cell.n == 0


def test_cell_n_must_not_be_negative():
    with pytest.raises(ValidationError):
        TransferWeightCell(
            nation=Nation.WALES,
            consolidator=PartyCode.PLAID,
            source=PartyCode.LAB,
            weight=0.5,
            n=-1,
        )


def test_override_valid():
    o = TransferWeightOverride(
        nation=Nation.ENGLAND,
        consolidator=PartyCode.LAB,
        source=PartyCode.CON,
        weight=0.2,
        rationale="Most of the collapsing Conservative vote goes to Reform.",
    )
    assert o.weight == 0.2
    assert o.source is PartyCode.CON


def test_override_weight_in_unit_interval():
    for bad in (1.2, -0.1):
        with pytest.raises(ValidationError):
            TransferWeightOverride(
                nation=Nation.ENGLAND,
                consolidator=PartyCode.LAB,
                source=PartyCode.CON,
                weight=bad,
                rationale="x",
            )


def test_override_requires_rationale():
    with pytest.raises(ValidationError):
        TransferWeightOverride(
            nation=Nation.ENGLAND,
            consolidator=PartyCode.LAB,
            source=PartyCode.CON,
            weight=0.2,
            rationale="",
        )


def test_override_rejects_unknown_party():
    with pytest.raises(ValidationError):
        TransferWeightOverride(
            nation=Nation.ENGLAND,
            consolidator=PartyCode.LAB,
            source="monster_raving_loony",
            weight=0.2,
            rationale="x",
        )


def test_provenance_valid():
    p = TransferWeightProvenance(
        nation=Nation.WALES,
        consolidator=PartyCode.PLAID,
        event_id="caerphilly_senedd_2025",
    )
    assert p.event_id == "caerphilly_senedd_2025"
