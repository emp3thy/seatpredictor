from schema.common import PartyCode, Nation, LEFT_BLOC


def test_party_code_values():
    assert PartyCode.LAB.value == "lab"
    assert PartyCode.CON.value == "con"
    assert PartyCode.LD.value == "ld"
    assert PartyCode.REFORM.value == "reform"
    assert PartyCode.GREEN.value == "green"
    assert PartyCode.SNP.value == "snp"
    assert PartyCode.PLAID.value == "plaid"
    assert PartyCode.OTHER.value == "other"


def test_nation_values():
    assert {n.value for n in Nation} == {"england", "wales", "scotland", "northern_ireland"}


def test_left_bloc_membership_by_nation():
    assert LEFT_BLOC[Nation.ENGLAND] == {PartyCode.LAB, PartyCode.LD, PartyCode.GREEN}
    assert LEFT_BLOC[Nation.WALES] == {
        PartyCode.LAB, PartyCode.LD, PartyCode.GREEN, PartyCode.PLAID
    }
    assert LEFT_BLOC[Nation.SCOTLAND] == {
        PartyCode.LAB, PartyCode.LD, PartyCode.GREEN, PartyCode.SNP
    }
    assert LEFT_BLOC[Nation.NORTHERN_IRELAND] == set()
