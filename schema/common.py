from enum import Enum


class PartyCode(str, Enum):
    LAB = "lab"
    CON = "con"
    LD = "ld"
    REFORM = "reform"
    GREEN = "green"
    SNP = "snp"
    PLAID = "plaid"
    OTHER = "other"


class Nation(str, Enum):
    ENGLAND = "england"
    WALES = "wales"
    SCOTLAND = "scotland"
    NORTHERN_IRELAND = "northern_ireland"


LEFT_BLOC: dict[Nation, set[PartyCode]] = {
    Nation.ENGLAND: {PartyCode.LAB, PartyCode.LD, PartyCode.GREEN},
    Nation.WALES: {PartyCode.LAB, PartyCode.LD, PartyCode.GREEN, PartyCode.PLAID},
    Nation.SCOTLAND: {PartyCode.LAB, PartyCode.LD, PartyCode.GREEN, PartyCode.SNP},
    Nation.NORTHERN_IRELAND: set(),
}
