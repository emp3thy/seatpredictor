import pytest
from pydantic import ValidationError
from schema.transfer_weights import TransferWeightCell, TransferWeightProvenance
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


def test_cell_n_must_be_positive():
    with pytest.raises(ValidationError):
        TransferWeightCell(
            nation=Nation.WALES,
            consolidator=PartyCode.PLAID,
            source=PartyCode.LAB,
            weight=0.5,
            n=0,
        )


def test_provenance_valid():
    p = TransferWeightProvenance(
        nation=Nation.WALES,
        consolidator=PartyCode.PLAID,
        event_id="caerphilly_senedd_2025",
    )
    assert p.event_id == "caerphilly_senedd_2025"
