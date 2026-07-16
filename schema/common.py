from enum import Enum


class PartyCode(str, Enum):
    LAB = "lab"
    CON = "con"
    LD = "ld"
    REFORM = "reform"
    # Restore Britain — formed 2026, right of Reform. No 2024 GE baseline (share_2024
    # is 0 everywhere); polls give it a national share, so uniform swing spreads it
    # evenly. Not in LEFT_BLOC; excluded as a tactical-flow source (its voters do not
    # consolidate behind left-bloc candidates).
    RESTORE = "restore"
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
