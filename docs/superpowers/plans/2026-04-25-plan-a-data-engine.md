# Plan A — Foundations + Data Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the project foundation (scaffolding, shared Pydantic schema, SQLite I/O) and the data engine — three sources plus derived transfer matrix — culminating in a working `seatpredict-data snapshot` CLI that produces a single-file SQLite snapshot.

**Architecture:** Two-layer data architecture — raw cache (file-based, idempotent fetches) feeds a transform pipeline that produces single-file SQLite snapshots keyed by content hash over inputs. Pydantic models in `schema/` are the contract between data engine and prediction engine; both engines import them.

**Tech Stack:** Python 3.11+, uv (env mgmt), Pydantic v2, SQLite (stdlib + SQLAlchemy core), pandas, BeautifulSoup4 + lxml, httpx, PyYAML, Click, pytest, respx (HTTP mocking).

**Spec reference:** `docs/superpowers/specs/2026-04-25-seat-predictor-design.md` — sections 3, 4, 7, 8.

**Successor plans:** Plan B (prediction engine), Plan C (analysis layer). This plan does NOT cover the prediction engine, strategies, notebooks, or analysis CLI.

---

## File structure produced by this plan

```
seatpredictor/
  pyproject.toml                                # uv-managed project
  .python-version                               # 3.11
  .gitignore                                    # already exists
  README.md                                     # quickstart + commands

  schema/
    __init__.py                                 # re-exports public models
    common.py                                   # PartyCode enum, Nation enum, helpers
    poll.py                                     # Poll, Geography
    constituency.py                             # ConstituencyResult
    byelection.py                               # ByElectionEvent, ByElectionResult
    transfer_weights.py                         # TransferWeightCell, TransferWeightProvenance
    snapshot.py                                 # SnapshotManifest, SnapshotWrapper

  data_engine/
    __init__.py
    sqlite_io.py                                # write_table, read_table, content-hash helpers
    raw_cache.py                                # cache fetched artifacts by URL+date
    sources/
      __init__.py
      hoc_results.py                            # download + parse 2024 GE CSV
      byelections.py                            # YAML loader
      wikipedia_polls.py                        # fetch + parse polling tables
    transforms/
      __init__.py
      transfer_matrix.py                        # derive matrix from byelections
    snapshot.py                                 # orchestrator: build snapshot from sources
    cli.py                                      # `seatpredict-data` entrypoint

  data/
    hand_curated/
      by_elections.yaml                         # seed data for 4 events

  tests/
    __init__.py
    conftest.py                                 # shared fixtures
    schema/
      __init__.py
      test_poll.py
      test_constituency.py
      test_byelection.py
      test_transfer_weights.py
      test_snapshot.py
    data_engine/
      __init__.py
      test_sqlite_io.py
      test_raw_cache.py
      test_hoc_results.py
      test_byelections.py
      test_wikipedia_polls.py
      test_transfer_matrix.py
      test_snapshot_orchestrator.py
      test_cli.py
    fixtures/
      hoc_results_sample.csv                    # 5 constituencies
      wikipedia_polls_sample.html               # frozen snapshot for parser tests
      by_elections_sample.yaml                  # synthetic events for matrix tests
```

---

## Task 1: Project scaffolding

**Files:**
- Create: `pyproject.toml`, `.python-version`, `README.md`
- Create: package skeletons `schema/__init__.py`, `data_engine/__init__.py`, `tests/__init__.py`, `tests/conftest.py`
- Modify: `.gitignore` (already exists; verify)

- [ ] **Step 1: Create `.python-version`**

```
3.11
```

- [ ] **Step 2: Create `pyproject.toml`**

```toml
[project]
name = "seatpredictor"
version = "0.0.1"
description = "UK Westminster seat predictor with tactical-consolidation modelling"
requires-python = ">=3.11"
dependencies = [
    "pydantic>=2.6,<3",
    "pandas>=2.2,<3",
    "sqlalchemy>=2.0,<3",
    "beautifulsoup4>=4.12,<5",
    "lxml>=5.1,<6",
    "httpx>=0.27,<1",
    "pyyaml>=6.0,<7",
    "click>=8.1,<9",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0,<9",
    "respx>=0.21,<1",
]

[project.scripts]
seatpredict-data = "data_engine.cli:main"

[tool.setuptools.packages.find]
where = ["."]
include = ["schema*", "data_engine*"]
exclude = ["tests*", "data*", "docs*", "notebooks*"]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
addopts = "-v --tb=short"

[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"
```

- [ ] **Step 3: Create empty package files**

```bash
touch schema/__init__.py
touch data_engine/__init__.py
touch data_engine/sources/__init__.py
touch data_engine/transforms/__init__.py
touch tests/__init__.py
touch tests/schema/__init__.py
touch tests/data_engine/__init__.py
mkdir -p tests/fixtures
```

(On Windows bash this works; on PowerShell use `New-Item -ItemType File -Path ...`.)

- [ ] **Step 4: Create `tests/conftest.py`**

```python
import pytest
from pathlib import Path


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def tmp_snapshot_path(tmp_path: Path) -> Path:
    return tmp_path / "test_snapshot.sqlite"
```

- [ ] **Step 5: Create `README.md`**

```markdown
# seatpredictor

UK Westminster seat predictor with tactical-consolidation modelling.

See `docs/superpowers/specs/2026-04-25-seat-predictor-design.md` for the full design.

## Setup

```bash
uv venv
uv pip install -e ".[dev]"
```

## Run tests

```bash
uv run pytest
```

## Generate a snapshot

```bash
uv run seatpredict-data snapshot
```
```

- [ ] **Step 6: Create venv, install, and run a smoke test**

```bash
uv venv
uv pip install -e ".[dev]"
uv run pytest
```

Expected: `pytest` exits 0 with "no tests ran" (or similar). The smoke test is that the package installed and pytest runs.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml .python-version README.md schema data_engine tests
git commit -m "chore: project scaffolding (pyproject, package skeletons, conftest)"
```

---

## Task 2: Schema — common enums and helpers

**Files:**
- Create: `schema/common.py`
- Test: `tests/schema/test_common.py`

- [ ] **Step 1: Write the failing test**

`tests/schema/test_common.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/schema/test_common.py -v
```

Expected: `ImportError: cannot import name 'PartyCode' from 'schema.common'` (module doesn't exist yet).

- [ ] **Step 3: Write minimal implementation**

`schema/common.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/schema/test_common.py -v
```

Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add schema/common.py tests/schema/test_common.py
git commit -m "feat(schema): add PartyCode, Nation, LEFT_BLOC constants"
```

---

## Task 3: Schema — Poll model

**Files:**
- Create: `schema/poll.py`
- Test: `tests/schema/test_poll.py`

- [ ] **Step 1: Write the failing test**

`tests/schema/test_poll.py`:

```python
from datetime import date
import pytest
from pydantic import ValidationError
from schema.poll import Poll, Geography


def _valid_poll_payload() -> dict:
    return {
        "pollster": "YouGov",
        "fieldwork_start": date(2026, 4, 18),
        "fieldwork_end": date(2026, 4, 20),
        "published_date": date(2026, 4, 21),
        "sample_size": 1842,
        "geography": "GB",
        "con": 22.0,
        "lab": 28.0,
        "ld": 11.0,
        "reform": 24.0,
        "green": 8.0,
        "snp": 3.0,
        "plaid": 1.0,
        "other": 3.0,
    }


def test_poll_accepts_valid_payload():
    poll = Poll.model_validate(_valid_poll_payload())
    assert poll.pollster == "YouGov"
    assert poll.geography == Geography.GB
    assert poll.lab == 28.0


def test_poll_rejects_shares_summing_far_from_100():
    payload = _valid_poll_payload()
    payload["lab"] = 50.0  # now sums to 122
    with pytest.raises(ValidationError, match="shares must sum to ~100"):
        Poll.model_validate(payload)


def test_poll_geography_values():
    assert {g.value for g in Geography} == {"GB", "Scotland", "Wales", "London"}


def test_poll_round_trip_via_dict():
    poll = Poll.model_validate(_valid_poll_payload())
    raw = poll.model_dump(mode="json")
    restored = Poll.model_validate(raw)
    assert restored == poll


def test_poll_fieldwork_dates_must_be_ordered():
    payload = _valid_poll_payload()
    payload["fieldwork_start"] = date(2026, 4, 25)
    payload["fieldwork_end"] = date(2026, 4, 20)
    with pytest.raises(ValidationError, match="fieldwork_start"):
        Poll.model_validate(payload)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/schema/test_poll.py -v
```

Expected: ImportError for `schema.poll`.

- [ ] **Step 3: Write minimal implementation**

`schema/poll.py`:

```python
from datetime import date
from enum import Enum
from pydantic import BaseModel, Field, model_validator


class Geography(str, Enum):
    GB = "GB"
    SCOTLAND = "Scotland"
    WALES = "Wales"
    LONDON = "London"


class Poll(BaseModel):
    pollster: str = Field(min_length=1)
    fieldwork_start: date
    fieldwork_end: date
    published_date: date
    sample_size: int = Field(gt=0)
    geography: Geography
    con: float = Field(ge=0, le=100)
    lab: float = Field(ge=0, le=100)
    ld: float = Field(ge=0, le=100)
    reform: float = Field(ge=0, le=100)
    green: float = Field(ge=0, le=100)
    snp: float = Field(ge=0, le=100)
    plaid: float = Field(ge=0, le=100)
    other: float = Field(ge=0, le=100)

    @model_validator(mode="after")
    def _check_shares_and_dates(self) -> "Poll":
        total = self.con + self.lab + self.ld + self.reform + self.green + self.snp + self.plaid + self.other
        if not (99.0 <= total <= 101.0):
            raise ValueError(f"shares must sum to ~100 (got {total:.2f})")
        if self.fieldwork_start > self.fieldwork_end:
            raise ValueError("fieldwork_start must be on or before fieldwork_end")
        return self
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/schema/test_poll.py -v
```

Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add schema/poll.py tests/schema/test_poll.py
git commit -m "feat(schema): add Poll and Geography models with validation"
```

---

## Task 4: Schema — ConstituencyResult model

**Files:**
- Create: `schema/constituency.py`
- Test: `tests/schema/test_constituency.py`

- [ ] **Step 1: Write the failing test**

`tests/schema/test_constituency.py`:

```python
import pytest
from pydantic import ValidationError
from schema.constituency import ConstituencyResult
from schema.common import PartyCode, Nation


def _valid_row() -> dict:
    return {
        "ons_code": "E14001234",
        "constituency_name": "Gorton and Denton",
        "region": "North West",
        "nation": "england",
        "party": "lab",
        "votes": 18234,
        "share": 49.7,
    }


def test_constituency_result_valid():
    row = ConstituencyResult.model_validate(_valid_row())
    assert row.ons_code == "E14001234"
    assert row.party == PartyCode.LAB
    assert row.nation == Nation.ENGLAND


def test_share_must_be_between_zero_and_one_hundred():
    payload = _valid_row()
    payload["share"] = 110.0
    with pytest.raises(ValidationError):
        ConstituencyResult.model_validate(payload)


def test_votes_must_be_non_negative():
    payload = _valid_row()
    payload["votes"] = -1
    with pytest.raises(ValidationError):
        ConstituencyResult.model_validate(payload)


def test_round_trip():
    row = ConstituencyResult.model_validate(_valid_row())
    restored = ConstituencyResult.model_validate(row.model_dump(mode="json"))
    assert restored == row
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/schema/test_constituency.py -v
```

Expected: ImportError.

- [ ] **Step 3: Write minimal implementation**

`schema/constituency.py`:

```python
from pydantic import BaseModel, Field
from schema.common import PartyCode, Nation


class ConstituencyResult(BaseModel):
    ons_code: str = Field(min_length=1)
    constituency_name: str = Field(min_length=1)
    region: str = Field(min_length=1)
    nation: Nation
    party: PartyCode
    votes: int = Field(ge=0)
    share: float = Field(ge=0, le=100)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/schema/test_constituency.py -v
```

Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add schema/constituency.py tests/schema/test_constituency.py
git commit -m "feat(schema): add ConstituencyResult model"
```

---

## Task 5: Schema — ByElectionEvent and ByElectionResult models

**Files:**
- Create: `schema/byelection.py`
- Test: `tests/schema/test_byelection.py`

- [ ] **Step 1: Write the failing test**

`tests/schema/test_byelection.py`:

```python
from datetime import date
import pytest
from pydantic import ValidationError
from schema.byelection import ByElectionEvent, ByElectionResult, EventType
from schema.common import PartyCode, Nation


def _valid_event() -> dict:
    return {
        "event_id": "caerphilly_senedd_2025",
        "name": "Caerphilly Senedd by-election",
        "date": date(2025, 10, 23),
        "event_type": "senedd",
        "nation": "wales",
        "region": "South Wales East",
        "threat_party": "reform",
        "exclude_from_matrix": False,
        "narrative_url": "https://en.wikipedia.org/wiki/2025_Caerphilly_by-election",
    }


def _valid_result() -> dict:
    return {
        "event_id": "caerphilly_senedd_2025",
        "party": "plaid",
        "votes": 15961,
        "actual_share": 47.4,
        "prior_share": 28.4,
    }


def test_event_valid():
    ev = ByElectionEvent.model_validate(_valid_event())
    assert ev.event_id == "caerphilly_senedd_2025"
    assert ev.event_type == EventType.SENEDD
    assert ev.threat_party == PartyCode.REFORM


def test_event_null_threat_party_implies_exclude():
    payload = _valid_event()
    payload["threat_party"] = None
    payload["exclude_from_matrix"] = False
    ev = ByElectionEvent.model_validate(payload)
    assert ev.exclude_from_matrix is True  # auto-coerced


def test_result_valid():
    r = ByElectionResult.model_validate(_valid_result())
    assert r.party == PartyCode.PLAID
    assert r.actual_share == 47.4


def test_event_types():
    assert {e.value for e in EventType} == {
        "westminster_byelection", "senedd", "holyrood"
    }


def test_round_trip_event():
    ev = ByElectionEvent.model_validate(_valid_event())
    restored = ByElectionEvent.model_validate(ev.model_dump(mode="json"))
    assert restored == ev
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/schema/test_byelection.py -v
```

Expected: ImportError.

- [ ] **Step 3: Write minimal implementation**

`schema/byelection.py`:

```python
from datetime import date
from enum import Enum
from pydantic import BaseModel, Field, HttpUrl, model_validator
from schema.common import PartyCode, Nation


class EventType(str, Enum):
    WESTMINSTER_BYELECTION = "westminster_byelection"
    SENEDD = "senedd"
    HOLYROOD = "holyrood"


class ByElectionEvent(BaseModel):
    event_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    date: date
    event_type: EventType
    nation: Nation
    region: str
    threat_party: PartyCode | None = None
    exclude_from_matrix: bool = False
    narrative_url: HttpUrl | None = None

    @model_validator(mode="after")
    def _coerce_exclusion(self) -> "ByElectionEvent":
        if self.threat_party is None:
            object.__setattr__(self, "exclude_from_matrix", True)
        return self


class ByElectionResult(BaseModel):
    event_id: str = Field(min_length=1)
    party: PartyCode
    votes: int = Field(ge=0)
    actual_share: float = Field(ge=0, le=100)
    prior_share: float = Field(ge=0, le=100)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/schema/test_byelection.py -v
```

Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add schema/byelection.py tests/schema/test_byelection.py
git commit -m "feat(schema): add ByElectionEvent and ByElectionResult models"
```

---

## Task 6: Schema — TransferWeights models

**Files:**
- Create: `schema/transfer_weights.py`
- Test: `tests/schema/test_transfer_weights.py`

- [ ] **Step 1: Write the failing test**

`tests/schema/test_transfer_weights.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/schema/test_transfer_weights.py -v
```

Expected: ImportError.

- [ ] **Step 3: Write minimal implementation**

`schema/transfer_weights.py`:

```python
from pydantic import BaseModel, Field
from schema.common import PartyCode, Nation


class TransferWeightCell(BaseModel):
    nation: Nation
    consolidator: PartyCode
    source: PartyCode
    weight: float = Field(ge=0.0, le=1.0)
    n: int = Field(gt=0)


class TransferWeightProvenance(BaseModel):
    nation: Nation
    consolidator: PartyCode
    event_id: str = Field(min_length=1)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/schema/test_transfer_weights.py -v
```

Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add schema/transfer_weights.py tests/schema/test_transfer_weights.py
git commit -m "feat(schema): add TransferWeightCell and TransferWeightProvenance"
```

---

## Task 7: Schema — Snapshot manifest model

**Files:**
- Create: `schema/snapshot.py`
- Modify: `schema/__init__.py` (re-exports)
- Test: `tests/schema/test_snapshot.py`

- [ ] **Step 1: Write the failing test**

`tests/schema/test_snapshot.py`:

```python
from datetime import datetime, date, timezone
import pytest
from pydantic import ValidationError
from schema.snapshot import SnapshotManifest


def _valid() -> dict:
    return {
        "as_of_date": date(2026, 4, 25),
        "schema_version": 1,
        "content_hash": "a3f2b00c0011",
        "generated_at": datetime(2026, 4, 25, 14, 30, tzinfo=timezone.utc),
        "source_versions": {
            "wikipedia_polls": "fetched_2026-04-25",
            "hoc_results": "ge_2024",
            "byelections": "yaml_sha:1234abcd",
        },
    }


def test_manifest_valid():
    m = SnapshotManifest.model_validate(_valid())
    assert m.schema_version == 1
    assert m.source_versions["hoc_results"] == "ge_2024"


def test_schema_version_must_be_positive():
    payload = _valid()
    payload["schema_version"] = 0
    with pytest.raises(ValidationError):
        SnapshotManifest.model_validate(payload)


def test_content_hash_required_nonempty():
    payload = _valid()
    payload["content_hash"] = ""
    with pytest.raises(ValidationError):
        SnapshotManifest.model_validate(payload)


def test_round_trip():
    m = SnapshotManifest.model_validate(_valid())
    restored = SnapshotManifest.model_validate(m.model_dump(mode="json"))
    assert restored == m
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/schema/test_snapshot.py -v
```

Expected: ImportError.

- [ ] **Step 3: Write minimal implementation**

`schema/snapshot.py`:

```python
from datetime import date, datetime
from pydantic import BaseModel, Field


class SnapshotManifest(BaseModel):
    as_of_date: date
    schema_version: int = Field(gt=0)
    content_hash: str = Field(min_length=1)
    generated_at: datetime
    source_versions: dict[str, str] = Field(default_factory=dict)
```

- [ ] **Step 4: Update `schema/__init__.py` to re-export**

`schema/__init__.py`:

```python
from schema.common import PartyCode, Nation, LEFT_BLOC
from schema.poll import Poll, Geography
from schema.constituency import ConstituencyResult
from schema.byelection import ByElectionEvent, ByElectionResult, EventType
from schema.transfer_weights import TransferWeightCell, TransferWeightProvenance
from schema.snapshot import SnapshotManifest

__all__ = [
    "PartyCode",
    "Nation",
    "LEFT_BLOC",
    "Poll",
    "Geography",
    "ConstituencyResult",
    "ByElectionEvent",
    "ByElectionResult",
    "EventType",
    "TransferWeightCell",
    "TransferWeightProvenance",
    "SnapshotManifest",
]
```

- [ ] **Step 5: Run all schema tests**

```bash
uv run pytest tests/schema/ -v
```

Expected: All schema tests pass (4+5+4+5+4 = 22+).

- [ ] **Step 6: Commit**

```bash
git add schema/snapshot.py schema/__init__.py tests/schema/test_snapshot.py
git commit -m "feat(schema): add SnapshotManifest and package re-exports"
```

---

## Task 8: SQLite I/O helpers

**Files:**
- Create: `data_engine/sqlite_io.py`
- Test: `tests/data_engine/test_sqlite_io.py`

- [ ] **Step 1: Write the failing test**

`tests/data_engine/test_sqlite_io.py`:

```python
from datetime import date, datetime, timezone
from pathlib import Path
import sqlite3
import pandas as pd
import pytest
from data_engine.sqlite_io import (
    open_snapshot_db,
    write_dataframe,
    read_dataframe,
    write_manifest,
    read_manifest,
    compute_input_hash,
)
from schema.snapshot import SnapshotManifest


def test_open_creates_file(tmp_path: Path):
    db_path = tmp_path / "test.sqlite"
    with open_snapshot_db(db_path) as conn:
        assert isinstance(conn, sqlite3.Connection)
    assert db_path.exists()


def test_dataframe_round_trip(tmp_path: Path):
    df = pd.DataFrame({
        "name": ["a", "b", "c"],
        "value": [1.5, 2.5, 3.5],
        "count": [10, 20, 30],
    })
    db_path = tmp_path / "test.sqlite"
    with open_snapshot_db(db_path) as conn:
        write_dataframe(conn, "things", df)
    with open_snapshot_db(db_path) as conn:
        restored = read_dataframe(conn, "things")
    pd.testing.assert_frame_equal(
        restored.sort_values("name").reset_index(drop=True),
        df.sort_values("name").reset_index(drop=True),
        check_dtype=False,
    )


def test_manifest_round_trip(tmp_path: Path):
    db_path = tmp_path / "test.sqlite"
    m = SnapshotManifest(
        as_of_date=date(2026, 4, 25),
        schema_version=1,
        content_hash="abc123",
        generated_at=datetime(2026, 4, 25, tzinfo=timezone.utc),
        source_versions={"x": "y"},
    )
    with open_snapshot_db(db_path) as conn:
        write_manifest(conn, m)
    with open_snapshot_db(db_path) as conn:
        restored = read_manifest(conn)
    assert restored == m


def test_input_hash_deterministic():
    h1 = compute_input_hash(
        as_of_date=date(2026, 4, 25),
        schema_version=1,
        source_versions={"a": "v1", "b": "v2"},
    )
    h2 = compute_input_hash(
        as_of_date=date(2026, 4, 25),
        schema_version=1,
        source_versions={"b": "v2", "a": "v1"},  # key order differs
    )
    assert h1 == h2
    assert len(h1) == 12  # short hash


def test_input_hash_changes_with_inputs():
    h1 = compute_input_hash(
        as_of_date=date(2026, 4, 25), schema_version=1, source_versions={"a": "v1"}
    )
    h2 = compute_input_hash(
        as_of_date=date(2026, 4, 26), schema_version=1, source_versions={"a": "v1"}
    )
    h3 = compute_input_hash(
        as_of_date=date(2026, 4, 25), schema_version=2, source_versions={"a": "v1"}
    )
    h4 = compute_input_hash(
        as_of_date=date(2026, 4, 25), schema_version=1, source_versions={"a": "v2"}
    )
    assert len({h1, h2, h3, h4}) == 4
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/data_engine/test_sqlite_io.py -v
```

Expected: ImportError.

- [ ] **Step 3: Write implementation**

`data_engine/sqlite_io.py`:

```python
import contextlib
import hashlib
import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Iterator

import pandas as pd
from schema.snapshot import SnapshotManifest


@contextlib.contextmanager
def open_snapshot_db(path: Path) -> Iterator[sqlite3.Connection]:
    """Open a SQLite connection; create file if missing. Commits on clean exit."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def write_dataframe(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> None:
    """Write DataFrame to table, replacing if it exists."""
    df.to_sql(table, conn, if_exists="replace", index=False)


def read_dataframe(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    """Read full table as DataFrame."""
    return pd.read_sql_query(f"SELECT * FROM {table}", conn)


def write_manifest(conn: sqlite3.Connection, manifest: SnapshotManifest) -> None:
    """Persist manifest to a single-row 'manifest' table."""
    payload = manifest.model_dump(mode="json")
    payload["source_versions"] = json.dumps(payload["source_versions"], sort_keys=True)
    df = pd.DataFrame([payload])
    write_dataframe(conn, "manifest", df)


def read_manifest(conn: sqlite3.Connection) -> SnapshotManifest:
    """Read the single-row manifest back into a SnapshotManifest."""
    df = read_dataframe(conn, "manifest")
    if len(df) != 1:
        raise ValueError(f"manifest table must have exactly 1 row, found {len(df)}")
    row = df.iloc[0].to_dict()
    row["source_versions"] = json.loads(row["source_versions"])
    return SnapshotManifest.model_validate(row)


def compute_input_hash(
    *,
    as_of_date: date,
    schema_version: int,
    source_versions: dict[str, str],
) -> str:
    """12-char hash over inputs that determine snapshot content. Stable across key order."""
    canonical = json.dumps(
        {
            "as_of_date": as_of_date.isoformat(),
            "schema_version": schema_version,
            "source_versions": dict(sorted(source_versions.items())),
        },
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/data_engine/test_sqlite_io.py -v
```

Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add data_engine/sqlite_io.py tests/data_engine/test_sqlite_io.py
git commit -m "feat(data_engine): add SQLite I/O helpers and content-hash utility"
```

---

## Task 9: Raw cache layer

**Files:**
- Create: `data_engine/raw_cache.py`
- Test: `tests/data_engine/test_raw_cache.py`

- [ ] **Step 1: Write the failing test**

`tests/data_engine/test_raw_cache.py`:

```python
from datetime import date
from pathlib import Path
import pytest
from data_engine.raw_cache import RawCache


def test_cache_miss_then_hit(tmp_path: Path):
    cache = RawCache(root=tmp_path)
    key = cache.key("wikipedia_polls", date(2026, 4, 25))
    assert not cache.exists(key)
    cache.put(key, b"<html>polls</html>", meta={"url": "https://example.com"})
    assert cache.exists(key)
    data = cache.get_bytes(key)
    assert data == b"<html>polls</html>"
    meta = cache.get_meta(key)
    assert meta["url"] == "https://example.com"
    assert "fetched_at" in meta


def test_cache_keys_distinguish_source_and_date(tmp_path: Path):
    cache = RawCache(root=tmp_path)
    k1 = cache.key("wikipedia_polls", date(2026, 4, 25))
    k2 = cache.key("wikipedia_polls", date(2026, 4, 26))
    k3 = cache.key("hoc_results", date(2026, 4, 25))
    assert k1 != k2
    assert k1 != k3


def test_force_refresh(tmp_path: Path):
    cache = RawCache(root=tmp_path)
    key = cache.key("hoc_results", date(2026, 4, 25))
    cache.put(key, b"v1", meta={})
    assert cache.get_bytes(key) == b"v1"
    cache.put(key, b"v2", meta={})  # overwrites
    assert cache.get_bytes(key) == b"v2"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/data_engine/test_raw_cache.py -v
```

Expected: ImportError.

- [ ] **Step 3: Write implementation**

`data_engine/raw_cache.py`:

```python
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class CacheKey:
    source: str
    fetch_date: date

    def relpath(self) -> str:
        return f"{self.source}/{self.fetch_date.isoformat()}"


class RawCache:
    """File-based cache for fetched artifacts. Idempotent per (source, fetch_date)."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def key(self, source: str, fetch_date: date) -> CacheKey:
        return CacheKey(source=source, fetch_date=fetch_date)

    def _dir(self, key: CacheKey) -> Path:
        return self.root / key.relpath()

    def exists(self, key: CacheKey) -> bool:
        return (self._dir(key) / "content.bin").exists()

    def put(self, key: CacheKey, data: bytes, meta: dict) -> None:
        d = self._dir(key)
        d.mkdir(parents=True, exist_ok=True)
        (d / "content.bin").write_bytes(data)
        meta_with_ts = {**meta, "fetched_at": datetime.now(tz=timezone.utc).isoformat()}
        (d / "meta.json").write_text(json.dumps(meta_with_ts, sort_keys=True, indent=2))

    def get_bytes(self, key: CacheKey) -> bytes:
        return (self._dir(key) / "content.bin").read_bytes()

    def get_meta(self, key: CacheKey) -> dict:
        return json.loads((self._dir(key) / "meta.json").read_text())
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/data_engine/test_raw_cache.py -v
```

Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add data_engine/raw_cache.py tests/data_engine/test_raw_cache.py
git commit -m "feat(data_engine): add file-based RawCache with (source, fetch_date) keys"
```

---

## Task 10: HoC results source — fixture + parser

**Files:**
- Create: `tests/fixtures/hoc_results_sample.csv`
- Create: `data_engine/sources/hoc_results.py`
- Test: `tests/data_engine/test_hoc_results.py`

- [ ] **Step 1: Create fixture file**

`tests/fixtures/hoc_results_sample.csv`:

```csv
ONS ID,Constituency name,Country name,Region name,First party,Lab,Con,LD,Reform,Green,SNP,PC,Other,Valid votes
E14001234,Gorton and Denton,England,North West,Lab,18234,2310,1422,8120,3110,0,0,1100,34296
E14005678,Caerphilly,Wales,Wales,Lab,15234,3120,1100,2010,890,0,9234,540,32128
S14000041,Hamilton Larkhall and Stonehouse,Scotland,Scotland,SNP,11234,2200,1500,7800,820,12340,0,500,36394
W07000058,Caerphilly Westminster,Wales,Wales,Lab,16100,2200,900,1700,750,0,8900,400,30950
N06000010,North Down,Northern Ireland,Northern Ireland,Other,500,200,150,100,80,0,0,21345,22375
```

(Note: the real HoC CSV has more columns; this is a minimal subset. The parser must handle whatever columns the real file has.)

- [ ] **Step 2: Write the failing test**

`tests/data_engine/test_hoc_results.py`:

```python
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
```

- [ ] **Step 3: Run test to verify it fails**

```bash
uv run pytest tests/data_engine/test_hoc_results.py -v
```

Expected: ImportError.

- [ ] **Step 4: Write implementation (robust to varied real-world headers)**

The real HoC CSV has 100+ columns and uses party-name strings (e.g. "Labour", "Conservative") or abbreviations (e.g. "Lab", "Con"). The parser matches columns by case-insensitive, whitespace-tolerant fuzzy lookup against a list of aliases per party, and silently skips unrecognised party columns rather than erroring.

`data_engine/sources/hoc_results.py`:

```python
import io
import pandas as pd
from schema.common import Nation, PartyCode


# Aliases the parser will accept for each party column. Lower-cased, trimmed.
_PARTY_ALIASES: dict[PartyCode, set[str]] = {
    PartyCode.LAB: {"lab", "labour", "lab co-op", "labour co-operative", "lab/co-op"},
    PartyCode.CON: {"con", "conservative", "conservatives", "conservative & unionist"},
    PartyCode.LD: {"ld", "lib dem", "libdem", "liberal democrat", "liberal democrats"},
    PartyCode.REFORM: {"reform", "ref", "ruk", "reform uk"},
    PartyCode.GREEN: {"green", "grn", "green party"},
    PartyCode.SNP: {"snp", "scottish national party"},
    PartyCode.PLAID: {"pc", "plaid", "plaid cymru"},
}

# All other identified parties roll up into "other".
_NATION_ALIASES: dict[str, Nation] = {
    "england": Nation.ENGLAND,
    "wales": Nation.WALES,
    "scotland": Nation.SCOTLAND,
    "northern ireland": Nation.NORTHERN_IRELAND,
}

# Identifying columns the parser expects. First match wins; checked case-insensitively.
_ONS_COL_CANDIDATES = ("ons id", "ons_id", "constituency id", "constituency_id", "pcon code")
_NAME_COL_CANDIDATES = ("constituency name", "constituency", "constituency_name")
_REGION_COL_CANDIDATES = ("region name", "region", "european region")
_NATION_COL_CANDIDATES = ("country name", "country", "nation")
_VALID_VOTES_CANDIDATES = ("valid votes", "valid_votes", "total votes", "valid vote")


def parse_hoc_results(csv_bytes: bytes) -> pd.DataFrame:
    """Parse the HoC Library 2024 GE results CSV into a tidy long DataFrame.

    Robust to column-name variation. Skips party columns it doesn't recognise and rolls
    them into 'other'. Returns one row per (constituency, party) with columns:
    ons_code, constituency_name, region, nation, party, votes, share.
    """
    raw = pd.read_csv(io.BytesIO(csv_bytes))
    raw.columns = [c.strip() for c in raw.columns]
    lower_to_actual = {c.lower(): c for c in raw.columns}

    ons_col = _first_match(_ONS_COL_CANDIDATES, lower_to_actual)
    name_col = _first_match(_NAME_COL_CANDIDATES, lower_to_actual)
    region_col = _first_match(_REGION_COL_CANDIDATES, lower_to_actual)
    nation_col = _first_match(_NATION_COL_CANDIDATES, lower_to_actual)
    valid_col = _first_match(_VALID_VOTES_CANDIDATES, lower_to_actual)
    if not all([ons_col, name_col, region_col, nation_col, valid_col]):
        raise ValueError(
            f"HoC CSV missing required columns. "
            f"ons={ons_col} name={name_col} region={region_col} nation={nation_col} valid={valid_col} "
            f"available={list(raw.columns)[:30]}..."
        )

    # Map each PartyCode to the actual CSV column it corresponds to (if any).
    party_col_for: dict[PartyCode, str | None] = {}
    matched_columns: set[str] = {ons_col, name_col, region_col, nation_col, valid_col}
    for party, aliases in _PARTY_ALIASES.items():
        for alias in aliases:
            if alias in lower_to_actual:
                party_col_for[party] = lower_to_actual[alias]
                matched_columns.add(lower_to_actual[alias])
                break
        else:
            party_col_for[party] = None

    # Any numeric column NOT matched by a known party rolls up into "other".
    other_cols = [
        c for c in raw.columns
        if c not in matched_columns
        and pd.api.types.is_numeric_dtype(raw[c])
        and c.lower() not in {"electorate", "valid votes", "valid_votes",
                              "total votes", "rejected ballots", "majority"}
    ]

    rows: list[dict] = []
    for _, r in raw.iterrows():
        ons = str(r[ons_col]).strip()
        name = str(r[name_col]).strip()
        region = str(r[region_col]).strip()
        nation_str = str(r[nation_col]).strip().lower()
        if nation_str not in _NATION_ALIASES:
            continue  # skip unknown nation rows
        nation = _NATION_ALIASES[nation_str].value
        valid = float(r[valid_col]) if pd.notna(r[valid_col]) else 0.0

        # Per-party rows
        other_votes = 0
        for party, col in party_col_for.items():
            votes = int(r[col]) if col and pd.notna(r[col]) else 0
            share = (votes / valid * 100.0) if valid > 0 else 0.0
            rows.append({
                "ons_code": ons, "constituency_name": name, "region": region,
                "nation": nation, "party": party.value,
                "votes": votes, "share": round(share, 2),
            })

        # Roll up other columns into "other"
        for c in other_cols:
            v = r[c]
            if pd.notna(v):
                other_votes += int(v)
        share = (other_votes / valid * 100.0) if valid > 0 else 0.0
        rows.append({
            "ons_code": ons, "constituency_name": name, "region": region,
            "nation": nation, "party": PartyCode.OTHER.value,
            "votes": other_votes, "share": round(share, 2),
        })
    return pd.DataFrame(rows)


def _first_match(candidates: tuple[str, ...], lower_to_actual: dict[str, str]) -> str | None:
    for c in candidates:
        if c in lower_to_actual:
            return lower_to_actual[c]
    return None
```

- [ ] **Step 5: Run fixture test to verify it passes**

```bash
uv run pytest tests/data_engine/test_hoc_results.py -v
```

Expected: 5 tests PASS.

- [ ] **Step 6: Real-world verification — fetch the actual CSV and inspect**

The fixture uses invented column names. Before declaring this task done, fetch the real CSV and sanity-check the parser handles it.

```bash
mkdir -p data/raw_cache/hoc_results/$(date +%Y-%m-%d)
curl -L -A "seatpredictor/0.0.1" \
  "https://researchbriefings.files.parliament.uk/documents/CBP-10009/HoC-GE2024-results-by-constituency.csv" \
  -o data/raw_cache/hoc_results/$(date +%Y-%m-%d)/content.bin
echo '{"url":"hoc-real"}' > data/raw_cache/hoc_results/$(date +%Y-%m-%d)/meta.json
```

Then run:

```python
uv run python -c "
from data_engine.sources.hoc_results import parse_hoc_results
from pathlib import Path
import sys
csv_bytes = next(Path('data/raw_cache/hoc_results').rglob('content.bin')).read_bytes()
df = parse_hoc_results(csv_bytes)
n_seats = df['ons_code'].nunique()
print(f'Constituencies parsed: {n_seats}')
assert n_seats == 650, f'Expected 650 UK constituencies, got {n_seats}'
print('Parties present:', sorted(df['party'].unique()))
print('Nations present:', sorted(df['nation'].unique()))
sums = df.groupby('ons_code')['share'].sum()
out_of_range = sums[(sums < 99.0) | (sums > 101.0)]
assert out_of_range.empty, f'Constituencies with shares not summing to 100: {out_of_range.head()}'
print('All shares sum to ~100%. OK.')
"
```

Expected: `Constituencies parsed: 650` and `All shares sum to ~100%`. If party-column matching fails (you see lots of "other" votes that should be Lab/Con/etc), inspect the CSV's headers (`csvkit` or `head -1`), and add the missing alias to `_PARTY_ALIASES` in `hoc_results.py`. Re-run.

- [ ] **Step 7: Commit**

```bash
git add data_engine/sources/hoc_results.py tests/fixtures/hoc_results_sample.csv tests/data_engine/test_hoc_results.py
git commit -m "feat(data_engine): add HoC results CSV parser robust to header variation"
```

---

## Task 11: By-elections source — seed YAML + loader

**Files:**
- Create: `data/hand_curated/by_elections.yaml`
- Create: `data_engine/sources/byelections.py`
- Test: `tests/data_engine/test_byelections.py`

- [ ] **Step 1: Create the curated seed YAML**

`data/hand_curated/by_elections.yaml`:

```yaml
# UK by-elections since the July 2024 GE.
# threat_party: party the tactical consolidation was *against* (typically the polling-frontrunner heading in,
# even if they didn't win on the day). Set null for events with no clear threat dynamic.
# exclude_from_matrix: set true for atypical events (e.g. scandal-driven contests) where flows aren't generalisable.

events:
  - event_id: runcorn_helsby_2025
    name: Runcorn and Helsby by-election
    date: 2025-05-01
    event_type: westminster_byelection
    nation: england
    region: North West
    threat_party: reform
    exclude_from_matrix: false
    narrative_url: https://en.wikipedia.org/wiki/2025_Runcorn_and_Helsby_by-election
    candidates:
      - { party: reform, votes: 12645, actual_share: 38.72, prior_share: 18.10 }
      - { party: lab,    votes: 12639, actual_share: 38.70, prior_share: 52.90 }
      - { party: con,    votes: 2341,  actual_share:  7.16, prior_share: 16.00 }
      - { party: ld,     votes: 1450,  actual_share:  4.44, prior_share:  4.00 }
      - { party: green,  votes:  870,  actual_share:  2.66, prior_share:  4.00 }
      - { party: other,  votes: 2705,  actual_share:  8.32, prior_share:  5.00 }

  - event_id: hamilton_larkhall_stonehouse_2025
    name: Hamilton, Larkhall and Stonehouse Holyrood by-election
    date: 2025-06-05
    event_type: holyrood
    nation: scotland
    region: Central Scotland
    threat_party: reform
    exclude_from_matrix: false
    narrative_url: https://en.wikipedia.org/wiki/2025_Hamilton,_Larkhall_and_Stonehouse_by-election
    candidates:
      - { party: lab,    votes: 8559, actual_share: 31.6, prior_share: 33.0 }
      - { party: snp,    votes: 7957, actual_share: 29.4, prior_share: 46.0 }
      - { party: reform, votes: 7088, actual_share: 26.1, prior_share:  0.0 }
      - { party: con,    votes: 1621, actual_share:  6.0, prior_share: 17.0 }
      - { party: ld,     votes:  598, actual_share:  2.2, prior_share:  2.5 }
      - { party: green,  votes:  609, actual_share:  2.3, prior_share:  1.5 }
      - { party: other,  votes:  650, actual_share:  2.4, prior_share:  0.0 }

  - event_id: caerphilly_senedd_2025
    name: Caerphilly Senedd by-election
    date: 2025-10-23
    event_type: senedd
    nation: wales
    region: South Wales East
    threat_party: reform
    exclude_from_matrix: false
    narrative_url: https://en.wikipedia.org/wiki/2025_Caerphilly_by-election
    candidates:
      - { party: plaid,  votes: 15961, actual_share: 47.4, prior_share: 28.4 }
      - { party: reform, votes: 12113, actual_share: 36.0, prior_share:  1.7 }
      - { party: lab,    votes:  3713, actual_share: 11.0, prior_share: 46.0 }
      - { party: con,    votes:   850, actual_share:  2.5, prior_share: 17.3 }
      - { party: ld,     votes:   400, actual_share:  1.2, prior_share:  2.4 }
      - { party: green,  votes:   330, actual_share:  1.0, prior_share:  1.3 }
      - { party: other,  votes:   295, actual_share:  0.9, prior_share:  2.9 }

  - event_id: gorton_denton_2026
    name: Gorton and Denton by-election
    date: 2026-02-26
    event_type: westminster_byelection
    nation: england
    region: North West
    threat_party: reform
    exclude_from_matrix: false
    narrative_url: https://en.wikipedia.org/wiki/2026_Gorton_and_Denton_by-election
    candidates:
      - { party: green,  votes: 13205, actual_share: 40.7, prior_share: 13.2 }
      - { party: reform, votes:  9314, actual_share: 28.7, prior_share:  9.8 }
      - { party: lab,    votes:  8245, actual_share: 25.4, prior_share: 50.8 }
      - { party: con,    votes:   650, actual_share:  2.0, prior_share: 12.4 }
      - { party: ld,     votes:   430, actual_share:  1.3, prior_share:  3.3 }
      - { party: other,  votes:   605, actual_share:  1.9, prior_share: 10.5 }
```

(Vote totals and prior shares above are approximate to the agent's verified facts; refine as the curator checks each event against the source.)

- [ ] **Step 2: Write the failing test**

`tests/data_engine/test_byelections.py`:

```python
from datetime import date
from pathlib import Path
import pytest
from data_engine.sources.byelections import load_byelections
from schema.byelection import EventType
from schema.common import Nation, PartyCode


def test_loads_committed_yaml():
    events_df, results_df = load_byelections(Path("data/hand_curated/by_elections.yaml"))
    # 4 events as of plan date
    assert len(events_df) >= 4
    expected_ids = {
        "runcorn_helsby_2025",
        "hamilton_larkhall_stonehouse_2025",
        "caerphilly_senedd_2025",
        "gorton_denton_2026",
    }
    assert expected_ids <= set(events_df["event_id"])


def test_caerphilly_threat_is_reform():
    events_df, _ = load_byelections(Path("data/hand_curated/by_elections.yaml"))
    row = events_df[events_df["event_id"] == "caerphilly_senedd_2025"].iloc[0]
    assert row["threat_party"] == PartyCode.REFORM.value
    assert row["nation"] == Nation.WALES.value
    assert row["event_type"] == EventType.SENEDD.value


def test_results_per_event_present():
    _, results_df = load_byelections(Path("data/hand_curated/by_elections.yaml"))
    caerphilly = results_df[results_df["event_id"] == "caerphilly_senedd_2025"]
    assert len(caerphilly) >= 6  # plaid, reform, lab, con, ld, green at minimum
    plaid = caerphilly[caerphilly["party"] == "plaid"].iloc[0]
    assert abs(plaid["actual_share"] - 47.4) < 0.1


def test_loader_filters_by_as_of_date():
    events_df, _ = load_byelections(
        Path("data/hand_curated/by_elections.yaml"),
        as_of=date(2025, 12, 31),
    )
    # Gorton (Feb 2026) excluded
    assert "gorton_denton_2026" not in set(events_df["event_id"])
    assert "caerphilly_senedd_2025" in set(events_df["event_id"])


def test_loader_rejects_event_with_actual_shares_not_summing_to_100(tmp_path: Path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("""
events:
  - event_id: bad_event
    name: Bad event
    date: 2026-01-01
    event_type: westminster_byelection
    nation: england
    region: X
    threat_party: reform
    exclude_from_matrix: false
    narrative_url: https://example.com
    candidates:
      - { party: reform, votes: 100, actual_share: 50.0, prior_share: 30.0 }
      - { party: lab,    votes: 100, actual_share: 30.0, prior_share: 50.0 }
""", encoding="utf-8")
    with pytest.raises(ValueError, match="actual_share entries sum"):
        load_byelections(bad)
```

- [ ] **Step 3: Run test to verify it fails**

```bash
uv run pytest tests/data_engine/test_byelections.py -v
```

Expected: ImportError.

- [ ] **Step 4: Write implementation with per-event share validation**

The Pydantic models validate one row at a time, but cross-row invariants — actual_shares per event sum to ~100%, prior_shares sum to ~100%, all parties have a non-negative `prior_share` even if absent (defaulted to 0) — must be enforced in the loader.

`data_engine/sources/byelections.py`:

```python
from datetime import date
from pathlib import Path

import pandas as pd
import yaml
from schema.byelection import ByElectionEvent, ByElectionResult


def load_byelections(
    yaml_path: Path,
    as_of: date | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load by_elections.yaml. Validates each event against Pydantic models AND
    cross-result invariants (shares sum to ~100% per event).
    Returns (events_df, results_df). If as_of is given, filters events with date <= as_of.
    """
    with yaml_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    event_rows: list[dict] = []
    result_rows: list[dict] = []

    for entry in raw["events"]:
        candidates = entry.pop("candidates", [])
        event = ByElectionEvent.model_validate(entry)
        if as_of is not None and event.date > as_of:
            continue

        validated = []
        for c in candidates:
            r = ByElectionResult.model_validate({"event_id": event.event_id, **c})
            validated.append(r)

        # Cross-result validation: shares sum to ~100% (±0.5pp tolerance per spec §4.3)
        actual_total = sum(r.actual_share for r in validated)
        prior_total = sum(r.prior_share for r in validated)
        if not (99.5 <= actual_total <= 100.5):
            raise ValueError(
                f"event {event.event_id}: actual_share entries sum to {actual_total:.2f} "
                f"(expected 99.5-100.5). Check vote tallies in by_elections.yaml."
            )
        if not (99.5 <= prior_total <= 100.5):
            raise ValueError(
                f"event {event.event_id}: prior_share entries sum to {prior_total:.2f} "
                f"(expected 99.5-100.5). Check prior_share values; missing parties should "
                f"be listed with prior_share: 0.0."
            )

        event_rows.append(event.model_dump(mode="json"))
        for r in validated:
            result_rows.append(r.model_dump(mode="json"))

    events_df = pd.DataFrame(event_rows)
    results_df = pd.DataFrame(result_rows)
    return events_df, results_df
```

- [ ] **Step 5: Run test to verify it passes**

```bash
uv run pytest tests/data_engine/test_byelections.py -v
```

Expected: 4 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add data/hand_curated/by_elections.yaml data_engine/sources/byelections.py tests/data_engine/test_byelections.py
git commit -m "feat(data_engine): add by-elections YAML loader with seeded 4 events"
```

---

## Task 12: Wikipedia polls source — fixture + parser + fetcher

**Files:**
- Create: `tests/fixtures/wikipedia_polls_sample.html`
- Create: `data_engine/sources/wikipedia_polls.py`
- Test: `tests/data_engine/test_wikipedia_polls.py`

- [ ] **Step 1: Create a minimal HTML fixture**

`tests/fixtures/wikipedia_polls_sample.html`:

```html
<!DOCTYPE html>
<html><body>
<h2>National voting intention</h2>
<table class="wikitable">
<tr>
  <th>Pollster</th><th>Client</th><th>Date(s) conducted</th><th>Sample size</th>
  <th>Lab</th><th>Con</th><th>Reform</th><th>LD</th><th>Grn</th><th>SNP</th><th>PC</th><th>Others</th><th>Lead</th>
</tr>
<tr>
  <td>YouGov</td><td>Times</td><td>18&ndash;20 Apr 2026</td><td>1,842</td>
  <td>28%</td><td>22%</td><td>24%</td><td>11%</td><td>8%</td><td>3%</td><td>1%</td><td>3%</td><td>4</td>
</tr>
<tr>
  <td>Ipsos</td><td>Standard</td><td>15&ndash;17 Apr 2026</td><td>1,200</td>
  <td>27%</td><td>23%</td><td>25%</td><td>10%</td><td>7%</td><td>4%</td><td>1%</td><td>3%</td><td>2</td>
</tr>
<tr>
  <td>Survation</td><td>None</td><td>10&ndash;12 Apr 2026</td><td>1,500</td>
  <td>29%</td><td>21%</td><td>23%</td><td>12%</td><td>8%</td><td>3%</td><td>1%</td><td>3%</td><td>6</td>
</tr>
</table>
</body></html>
```

- [ ] **Step 2: Write the failing test**

`tests/data_engine/test_wikipedia_polls.py`:

```python
from datetime import date
from pathlib import Path
import pytest
import respx
import httpx
from data_engine.sources.wikipedia_polls import (
    parse_polls_html,
    fetch_polls_html,
    POLLS_URL,
)


def test_parse_returns_one_row_per_poll(fixtures_dir: Path):
    html = (fixtures_dir / "wikipedia_polls_sample.html").read_text(encoding="utf-8")
    df = parse_polls_html(html, geography="GB")
    assert len(df) == 3
    assert set(df["pollster"]) == {"YouGov", "Ipsos", "Survation"}


def test_parse_extracts_published_dates(fixtures_dir: Path):
    html = (fixtures_dir / "wikipedia_polls_sample.html").read_text(encoding="utf-8")
    df = parse_polls_html(html, geography="GB")
    # We use the END date of fieldwork as published_date proxy in the parser
    yougov = df[df["pollster"] == "YouGov"].iloc[0]
    assert yougov["fieldwork_start"] == "2026-04-18"
    assert yougov["fieldwork_end"] == "2026-04-20"


def test_parse_party_shares(fixtures_dir: Path):
    html = (fixtures_dir / "wikipedia_polls_sample.html").read_text(encoding="utf-8")
    df = parse_polls_html(html, geography="GB")
    yougov = df[df["pollster"] == "YouGov"].iloc[0]
    assert yougov["lab"] == 28.0
    assert yougov["con"] == 22.0
    assert yougov["reform"] == 24.0


def test_parse_geography_column_set(fixtures_dir: Path):
    html = (fixtures_dir / "wikipedia_polls_sample.html").read_text(encoding="utf-8")
    df = parse_polls_html(html, geography="Wales")
    assert (df["geography"] == "Wales").all()


@respx.mock
def test_fetch_uses_user_agent_and_returns_text():
    route = respx.get(POLLS_URL).mock(
        return_value=httpx.Response(200, text="<html>ok</html>")
    )
    text = fetch_polls_html(POLLS_URL)
    assert text == "<html>ok</html>"
    assert route.called
    sent = route.calls[0].request
    assert "User-Agent" in sent.headers
    assert "seatpredictor" in sent.headers["User-Agent"]
```

- [ ] **Step 3: Run test to verify it fails**

```bash
uv run pytest tests/data_engine/test_wikipedia_polls.py -v
```

Expected: ImportError.

- [ ] **Step 4: Write implementation (table-targeted, footnote-tolerant, multi-format-date)**

The real Wikipedia polling page contains multiple `wikitable` instances — national VI tables (one per year of polling), seat-projection tables, and assorted others. We restrict parsing to **national voting-intention tables**: those whose header row contains "Pollster" *and* every party we expect (`Lab`, `Con`, `Reform`). Cells are cleaned of footnote refs (`[1]`, `[a]`), asterisks, and "—"/"N/A" empty markers. Date parsing handles five common formats.

`data_engine/sources/wikipedia_polls.py`:

```python
import re
from datetime import date

import httpx
import pandas as pd
from bs4 import BeautifulSoup


POLLS_URL = "https://en.wikipedia.org/wiki/Opinion_polling_for_the_next_United_Kingdom_general_election"
USER_AGENT = "seatpredictor/0.0.1 (research; contact: see repository)"

# Header text → internal column. Match is case-insensitive and exact (after strip).
_PARTY_HEADER_MAP = {
    "lab": "lab", "labour": "lab",
    "con": "con", "conservative": "con",
    "ld": "ld", "lib dem": "ld", "liberal democrats": "ld",
    "reform": "reform", "ref": "reform", "ruk": "reform",
    "grn": "green", "green": "green",
    "snp": "snp",
    "pc": "plaid", "plaid": "plaid",
    "others": "other", "other": "other",
}

# Tables we accept must contain at least these party columns (post-normalisation).
_REQUIRED_PARTIES_FOR_VI_TABLE = {"lab", "con", "reform"}

_MONTH = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


def fetch_polls_html(url: str) -> str:
    """Fetch a Wikipedia polling page; return raw HTML text. Honours a custom UA."""
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30.0) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.text


def parse_polls_html(html: str, geography: str) -> pd.DataFrame:
    """Parse polls from a Wikipedia polling page.

    Returns one row per poll with columns:
    pollster, fieldwork_start, fieldwork_end, published_date,
    sample_size, geography, con, lab, ld, reform, green, snp, plaid, other.

    Strategy: walk every <table class="wikitable">; admit only tables whose header
    contains a Pollster column AND at least Lab/Con/Reform party columns. Skip rows
    that fail date parsing. Tolerates footnote refs, asterisks, "—" cells.
    """
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict] = []
    for table in soup.find_all("table", class_="wikitable"):
        header_row = _find_header_row(table)
        if header_row is None:
            continue
        header_cells = [_clean(th) for th in header_row.find_all(["th", "td"])]
        if not header_cells:
            continue

        # Map header index → internal key (party slot or sentinel).
        col_map = _build_column_map(header_cells)
        if not col_map.get("pollster"):
            continue
        party_keys_present = {v for k, v in col_map.items() if v in {"lab", "con", "ld", "reform", "green", "snp", "plaid", "other"}}
        if not _REQUIRED_PARTIES_FOR_VI_TABLE <= party_keys_present:
            continue  # not a national-VI-shaped table

        for tr in table.find_all("tr"):
            if tr is header_row:
                continue
            tds = [_clean(td) for td in tr.find_all(["td", "th"])]
            if len(tds) < len(header_cells):
                continue
            poll = _parse_row(tds, col_map, geography=geography)
            if poll is not None:
                rows.append(poll)
    return pd.DataFrame(rows)


# --- helpers ---

def _find_header_row(table) -> object | None:
    """Pick the row that contains the column headers (usually the first <tr>)."""
    rows = table.find_all("tr")
    for tr in rows:
        # Heuristic: header row has more <th> than <td>.
        ths = len(tr.find_all("th"))
        tds = len(tr.find_all("td"))
        if ths > tds and ths >= 4:
            return tr
    return rows[0] if rows else None


def _build_column_map(headers: list[str]) -> dict[str, str]:
    """index → internal key. Returns dict of {pollster|sample|date|<party>: idx_str}.
    The keys store stringified indices so we can look them up easily.
    """
    out: dict[str, str] = {}
    for idx, h in enumerate(headers):
        nl = _norm_header(h)
        if nl == "pollster":
            out["pollster"] = str(idx)
        elif nl in {"sample size", "sample"}:
            out["sample"] = str(idx)
        elif nl in {"dates conducted", "date(s) conducted", "date conducted", "date", "fieldwork", "fieldwork dates"}:
            out["date"] = str(idx)
        elif nl in _PARTY_HEADER_MAP:
            out[str(idx)] = _PARTY_HEADER_MAP[nl]
    return out


def _norm_header(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"\[[^\]]*\]", "", s)  # remove footnote refs
    s = re.sub(r"\s+", " ", s)
    return s


def _clean(node) -> str:
    text = node.get_text()
    text = re.sub(r"\[[^\]]*\]", "", text)   # footnote refs e.g. [1] [a]
    text = text.replace(" ", " ").replace("–", "-").replace("—", "-")
    text = text.replace("&ndash;", "-").replace("&mdash;", "-").replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = text.rstrip("*").strip()
    return text


def _parse_row(tds: list[str], col_map: dict[str, str], *, geography: str) -> dict | None:
    pollster = tds[int(col_map["pollster"])].strip() if "pollster" in col_map else ""
    if not pollster or pollster.lower().startswith("source"):
        return None
    date_text = tds[int(col_map["date"])] if "date" in col_map else ""
    fws, fwe = _parse_date_range(date_text)
    if fws is None or fwe is None:
        return None
    sample = _parse_int(tds[int(col_map["sample"])]) if "sample" in col_map else 0
    out = {
        "pollster": pollster,
        "fieldwork_start": fws.isoformat(),
        "fieldwork_end": fwe.isoformat(),
        "published_date": fwe.isoformat(),
        "sample_size": sample,
        "geography": geography,
        "con": 0.0, "lab": 0.0, "ld": 0.0, "reform": 0.0,
        "green": 0.0, "snp": 0.0, "plaid": 0.0, "other": 0.0,
    }
    for k, v in col_map.items():
        if k in {"pollster", "sample", "date"}:
            continue
        idx = int(k)
        if idx >= len(tds):
            continue
        out[v] = _parse_pct(tds[idx])
    return out


def _parse_date_range(text: str) -> tuple[date | None, date | None]:
    """Handles: '18-20 Apr 2026', '29 Mar - 1 Apr 2026', '18 Apr 2026',
    '18 Apr 2026 - 1 May 2026', '29 Mar 2025 - 1 Apr 2026'.
    """
    text = text.strip()
    # Single day: "18 Apr 2026"
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", text)
    if m:
        d, mon, yr = m.groups()
        if (k := mon.lower()[:max(3, len(mon))]) and (mon.lower() in _MONTH or mon.lower()[:3] in _MONTH):
            month = _MONTH.get(mon.lower(), _MONTH.get(mon.lower()[:3]))
            if month:
                dd = date(int(yr), month, int(d))
                return dd, dd

    # Range same month: "18-20 Apr 2026"
    m = re.match(r"^(\d{1,2})\s*-\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", text)
    if m:
        d1, d2, mon, yr = m.groups()
        month = _MONTH.get(mon.lower(), _MONTH.get(mon.lower()[:3]))
        if month:
            return date(int(yr), month, int(d1)), date(int(yr), month, int(d2))

    # Range cross-month same year: "29 Mar - 1 Apr 2026"
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s*-\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", text)
    if m:
        d1, mon1, d2, mon2, yr = m.groups()
        m1 = _MONTH.get(mon1.lower(), _MONTH.get(mon1.lower()[:3]))
        m2 = _MONTH.get(mon2.lower(), _MONTH.get(mon2.lower()[:3]))
        if m1 and m2:
            return date(int(yr), m1, int(d1)), date(int(yr), m2, int(d2))

    # Range cross-year: "29 Dec 2025 - 3 Jan 2026"
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s*-\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", text)
    if m:
        d1, mon1, yr1, d2, mon2, yr2 = m.groups()
        m1 = _MONTH.get(mon1.lower(), _MONTH.get(mon1.lower()[:3]))
        m2 = _MONTH.get(mon2.lower(), _MONTH.get(mon2.lower()[:3]))
        if m1 and m2:
            return date(int(yr1), m1, int(d1)), date(int(yr2), m2, int(d2))

    return None, None


def _parse_int(s: str) -> int:
    s = re.sub(r"[^\d]", "", s)
    try:
        return int(s) if s else 0
    except ValueError:
        return 0


def _parse_pct(s: str) -> float:
    s = s.replace("%", "").strip()
    if s in {"", "-", "—", "N/A", "n/a"}:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0
```

- [ ] **Step 5: Run fixture test to verify it passes**

```bash
uv run pytest tests/data_engine/test_wikipedia_polls.py -v
```

Expected: 5 tests PASS.

- [ ] **Step 6: Real-world verification — fetch the live page**

The fixture is synthetic. Before declaring this task done, fetch the live Wikipedia page and verify the parser extracts a plausible number of polls.

```bash
uv run python -c "
from data_engine.sources.wikipedia_polls import fetch_polls_html, parse_polls_html, POLLS_URL
html = fetch_polls_html(POLLS_URL)
df = parse_polls_html(html, geography='GB')
print(f'Polls extracted: {len(df)}')
assert len(df) >= 30, f'Expected >=30 polls since GE 2024, got {len(df)}'
print('Pollsters:', sorted(df[\"pollster\"].unique())[:10], '...')
print('Date range:', df[\"published_date\"].min(), '→', df[\"published_date\"].max())
sums = df[['con', 'lab', 'ld', 'reform', 'green', 'snp', 'plaid', 'other']].sum(axis=1)
in_range = ((sums >= 95) & (sums <= 105)).sum()
print(f'Polls with shares summing 95-105: {in_range} / {len(df)}')
assert in_range >= len(df) * 0.9, 'Too many polls with implausible share sums; check parser'
print('OK.')
"
```

Expected: ≥30 polls extracted, ≥90% with share sums in [95, 105]. If the count is way off, the most likely culprits are:
- A new column appeared in real tables not in `_PARTY_HEADER_MAP` — add it.
- A new date format — extend `_parse_date_range`.
- The header heuristic in `_find_header_row` rejected a real header row — relax the `ths >= 4` threshold.

- [ ] **Step 7: Commit**

```bash
git add data_engine/sources/wikipedia_polls.py tests/fixtures/wikipedia_polls_sample.html tests/data_engine/test_wikipedia_polls.py
git commit -m "feat(data_engine): add Wikipedia polls parser robust to footnotes/varied dates"
```

---

## Task 13: Transfer matrix derivation

**Files:**
- Create: `data_engine/transforms/transfer_matrix.py`
- Test: `tests/data_engine/test_transfer_matrix.py`

- [ ] **Step 1: Write the failing test**

`tests/data_engine/test_transfer_matrix.py`:

```python
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
    events, results = _fake_byelections()
    cells, _ = derive_transfer_matrix(events, results)
    lab_row = cells[(cells["consolidator"] == "plaid") & (cells["source"] == "lab")].iloc[0]
    expected = (46.0 - 11.0) / 46.0
    assert abs(lab_row["weight"] - expected) < 1e-6
    assert lab_row["nation"] == "wales"
    assert lab_row["n"] == 1


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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/data_engine/test_transfer_matrix.py -v
```

Expected: ImportError.

- [ ] **Step 3: Write implementation**

`data_engine/transforms/transfer_matrix.py`:

```python
import pandas as pd
from schema.common import LEFT_BLOC, Nation, PartyCode


PRIOR_SHARE_THRESHOLD = 2.0  # percentage points


def derive_transfer_matrix(
    events: pd.DataFrame,
    results: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Derive the reform_threat transfer matrix from by-election data.

    Returns:
      cells: DataFrame with columns nation, consolidator, source, weight, n.
      provenance: DataFrame with columns nation, consolidator, event_id.
    """
    cell_records: list[dict] = []
    prov_records: list[dict] = []

    eligible = events[
        (events["threat_party"] == PartyCode.REFORM.value)
        & (events["exclude_from_matrix"] == False)  # noqa: E712
    ]

    for _, ev in eligible.iterrows():
        event_id = ev["event_id"]
        nation = Nation(ev["nation"])
        ev_results = results[results["event_id"] == event_id]
        consolidator = _identify_consolidator(ev_results, nation)
        if consolidator is None:
            continue
        flows = _compute_flows(ev_results, consolidator)
        for source, flow in flows.items():
            cell_records.append({
                "nation": nation.value,
                "consolidator": consolidator.value,
                "source": source.value,
                "weight": flow,
                "n_event": 1,
                "event_id": event_id,
            })
        prov_records.append({
            "nation": nation.value,
            "consolidator": consolidator.value,
            "event_id": event_id,
        })

    if not cell_records:
        empty_cells = pd.DataFrame(columns=["nation", "consolidator", "source", "weight", "n"])
        empty_prov = pd.DataFrame(columns=["nation", "consolidator", "event_id"])
        return empty_cells, empty_prov

    raw = pd.DataFrame(cell_records)
    cells = (
        raw.groupby(["nation", "consolidator", "source"], as_index=False)
        .agg(weight=("weight", "mean"), n=("event_id", "nunique"))
    )
    provenance = pd.DataFrame(prov_records).drop_duplicates()
    return cells, provenance


def _identify_consolidator(
    ev_results: pd.DataFrame,
    nation: Nation,
) -> PartyCode | None:
    """Return the left-bloc party with the largest gain over its prior share.
    Deterministic tie-break: when two parties tie on gain, pick the one with the
    higher actual_share (the locally-stronger party); if still tied, pick by
    party-code alphabetical order (final fallback so the function is total).
    """
    left = LEFT_BLOC[nation]
    if not left:
        return None
    candidates = ev_results[ev_results["party"].isin([p.value for p in left])].copy()
    if candidates.empty:
        return None
    candidates["gain"] = candidates["actual_share"] - candidates["prior_share"]
    candidates = candidates.sort_values(
        by=["gain", "actual_share", "party"],
        ascending=[False, False, True],
    )
    best = candidates.iloc[0]
    if best["gain"] <= 0:
        return None
    return PartyCode(best["party"])


def _compute_flows(
    ev_results: pd.DataFrame,
    consolidator: PartyCode,
) -> dict[PartyCode, float]:
    """For each non-Reform, non-consolidator party with prior_share above threshold,
    compute (prior - actual) / prior, clamped [0, 1]."""
    flows: dict[PartyCode, float] = {}
    for _, r in ev_results.iterrows():
        party = PartyCode(r["party"])
        if party == PartyCode.REFORM or party == consolidator:
            continue
        prior = float(r["prior_share"])
        actual = float(r["actual_share"])
        if prior <= PRIOR_SHARE_THRESHOLD:
            continue
        if prior <= 0:
            continue
        raw_flow = (prior - actual) / prior
        flows[party] = max(0.0, min(1.0, raw_flow))
    return flows
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/data_engine/test_transfer_matrix.py -v
```

Expected: 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add data_engine/transforms/transfer_matrix.py tests/data_engine/test_transfer_matrix.py
git commit -m "feat(data_engine): derive reform_threat transfer matrix from by-elections"
```

---

## Task 14: Snapshot orchestrator

**Files:**
- Create: `data_engine/snapshot.py`
- Test: `tests/data_engine/test_snapshot_orchestrator.py`

- [ ] **Step 1: Write the failing test**

`tests/data_engine/test_snapshot_orchestrator.py`:

```python
from datetime import date, datetime, timezone
from pathlib import Path
import pandas as pd
import pytest
import respx
import httpx
from data_engine.snapshot import build_snapshot, BuildSnapshotConfig, SCHEMA_VERSION
from data_engine.sqlite_io import open_snapshot_db, read_dataframe, read_manifest
from data_engine.raw_cache import RawCache
from data_engine.sources.wikipedia_polls import POLLS_URL


HOC_URL = "https://researchbriefings.files.parliament.uk/documents/CBP-10009/HoC-GE2024-results-by-constituency.csv"


@pytest.fixture
def primed_cache(tmp_path: Path, fixtures_dir: Path) -> RawCache:
    cache = RawCache(root=tmp_path / "raw_cache")
    today = date(2026, 4, 25)
    cache.put(
        cache.key("wikipedia_polls", today),
        (fixtures_dir / "wikipedia_polls_sample.html").read_bytes(),
        meta={"url": POLLS_URL},
    )
    cache.put(
        cache.key("hoc_results", today),
        (fixtures_dir / "hoc_results_sample.csv").read_bytes(),
        meta={"url": HOC_URL},
    )
    return cache


def test_builds_snapshot_with_all_tables(tmp_path: Path, primed_cache: RawCache):
    out = tmp_path / "snapshots"
    cfg = BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=Path("data/hand_curated/by_elections.yaml"),
    )
    path = build_snapshot(cfg)
    assert path.exists()
    with open_snapshot_db(path) as conn:
        polls = read_dataframe(conn, "polls")
        results_2024 = read_dataframe(conn, "results_2024")
        events = read_dataframe(conn, "byelections_events")
        ev_results = read_dataframe(conn, "byelections_results")
        weights = read_dataframe(conn, "transfer_weights")
        provenance = read_dataframe(conn, "transfer_weights_provenance")
        manifest = read_manifest(conn)
    assert len(polls) > 0
    assert len(results_2024) > 0
    assert len(events) >= 4
    assert len(ev_results) > 0
    assert len(weights) > 0
    assert len(provenance) > 0
    assert manifest.schema_version == SCHEMA_VERSION
    assert manifest.as_of_date == date(2026, 4, 25)


def test_snapshot_filename_includes_input_hash(tmp_path: Path, primed_cache: RawCache):
    out = tmp_path / "snapshots"
    cfg = BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=Path("data/hand_curated/by_elections.yaml"),
    )
    path = build_snapshot(cfg)
    assert path.name.startswith("2026-04-25__v1__")
    assert path.suffix == ".sqlite"


def test_idempotent_rerun_returns_same_path(tmp_path: Path, primed_cache: RawCache):
    out = tmp_path / "snapshots"
    cfg = BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=Path("data/hand_curated/by_elections.yaml"),
    )
    p1 = build_snapshot(cfg)
    p2 = build_snapshot(cfg)
    assert p1 == p2  # same content hash → same filename → reuse


def test_as_of_filter_changes_input_hash(tmp_path: Path, primed_cache: RawCache):
    # Prime cache for both dates
    primed_cache.put(
        primed_cache.key("wikipedia_polls", date(2025, 12, 31)),
        primed_cache.get_bytes(primed_cache.key("wikipedia_polls", date(2026, 4, 25))),
        meta={},
    )
    primed_cache.put(
        primed_cache.key("hoc_results", date(2025, 12, 31)),
        primed_cache.get_bytes(primed_cache.key("hoc_results", date(2026, 4, 25))),
        meta={},
    )
    out = tmp_path / "snapshots"
    p_apr = build_snapshot(BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=Path("data/hand_curated/by_elections.yaml"),
    ))
    p_dec = build_snapshot(BuildSnapshotConfig(
        as_of_date=date(2025, 12, 31),
        raw_cache=primed_cache,
        out_dir=out,
        byelections_yaml=Path("data/hand_curated/by_elections.yaml"),
    ))
    assert p_apr != p_dec
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/data_engine/test_snapshot_orchestrator.py -v
```

Expected: ImportError.

- [ ] **Step 3: Write implementation**

`data_engine/snapshot.py`:

```python
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
from data_engine.raw_cache import RawCache
from data_engine.sources.byelections import load_byelections
from data_engine.sources.hoc_results import parse_hoc_results
from data_engine.sources.wikipedia_polls import parse_polls_html
from data_engine.sqlite_io import (
    compute_input_hash,
    open_snapshot_db,
    write_dataframe,
    write_manifest,
)
from data_engine.transforms.transfer_matrix import derive_transfer_matrix
from schema.snapshot import SnapshotManifest


SCHEMA_VERSION = 1


@dataclass
class BuildSnapshotConfig:
    """Inputs for build_snapshot.

    polls_geographies: which geographies to include in the polls table. v1 default is
    ("GB",) only — the GB-wide page is the single fetch. Regional sub-pages
    (Scotland/Wales/London) require additional fetches; they're plumbed through this
    field but not wired up in v1's CLI to keep scope tight. Plan B may add them when
    a strategy actually consumes regional swing data.
    """
    as_of_date: date
    raw_cache: RawCache
    out_dir: Path
    byelections_yaml: Path
    polls_geographies: tuple[str, ...] = ("GB",)


def build_snapshot(cfg: BuildSnapshotConfig) -> Path:
    """Read raw cache, transform, write a single SQLite snapshot. Idempotent.

    If a snapshot with the same input hash already exists, returns that path
    without re-running the transform.
    """
    # Source versions feed into the input hash
    source_versions = _source_versions(cfg)
    input_hash = compute_input_hash(
        as_of_date=cfg.as_of_date,
        schema_version=SCHEMA_VERSION,
        source_versions=source_versions,
    )
    out_path = (
        cfg.out_dir
        / f"{cfg.as_of_date.isoformat()}__v{SCHEMA_VERSION}__{input_hash}.sqlite"
    )
    if out_path.exists():
        return out_path

    # Parse each source
    polls_df = _build_polls_df(cfg)
    results_df = _build_results_df(cfg)
    events_df, ev_results_df = load_byelections(cfg.byelections_yaml, as_of=cfg.as_of_date)
    cells_df, provenance_df = derive_transfer_matrix(events_df, ev_results_df)

    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    with open_snapshot_db(out_path) as conn:
        write_dataframe(conn, "polls", polls_df)
        write_dataframe(conn, "results_2024", results_df)
        write_dataframe(conn, "byelections_events", events_df)
        write_dataframe(conn, "byelections_results", ev_results_df)
        write_dataframe(conn, "transfer_weights", cells_df)
        write_dataframe(conn, "transfer_weights_provenance", provenance_df)
        manifest = SnapshotManifest(
            as_of_date=cfg.as_of_date,
            schema_version=SCHEMA_VERSION,
            content_hash=input_hash,
            generated_at=datetime.now(tz=timezone.utc),
            source_versions=source_versions,
        )
        write_manifest(conn, manifest)
    return out_path


def _build_polls_df(cfg: BuildSnapshotConfig) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for geo in cfg.polls_geographies:
        key = cfg.raw_cache.key("wikipedia_polls", cfg.as_of_date)
        if not cfg.raw_cache.exists(key):
            raise FileNotFoundError(
                f"raw cache miss for wikipedia_polls@{cfg.as_of_date}; run `seatpredict-data fetch` first"
            )
        html = cfg.raw_cache.get_bytes(key).decode("utf-8")
        df = parse_polls_html(html, geography=geo)
        parts.append(df)
    polls = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if not polls.empty:
        polls = polls[polls["published_date"] <= cfg.as_of_date.isoformat()]
    return polls


def _build_results_df(cfg: BuildSnapshotConfig) -> pd.DataFrame:
    key = cfg.raw_cache.key("hoc_results", cfg.as_of_date)
    if not cfg.raw_cache.exists(key):
        raise FileNotFoundError(
            f"raw cache miss for hoc_results@{cfg.as_of_date}; run `seatpredict-data fetch` first"
        )
    csv_bytes = cfg.raw_cache.get_bytes(key)
    return parse_hoc_results(csv_bytes)


def _source_versions(cfg: BuildSnapshotConfig) -> dict[str, str]:
    yaml_bytes = cfg.byelections_yaml.read_bytes()
    import hashlib
    yaml_hash = hashlib.sha256(yaml_bytes).hexdigest()[:12]
    return {
        "wikipedia_polls": cfg.as_of_date.isoformat(),
        "hoc_results": "ge_2024",
        "byelections_yaml": yaml_hash,
        "polls_geographies": ",".join(cfg.polls_geographies),
    }
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/data_engine/test_snapshot_orchestrator.py -v
```

Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add data_engine/snapshot.py tests/data_engine/test_snapshot_orchestrator.py
git commit -m "feat(data_engine): add snapshot orchestrator with input-hash idempotency"
```

---

## Task 15: CLI — `seatpredict-data` entry point

**Files:**
- Create: `data_engine/cli.py`
- Test: `tests/data_engine/test_cli.py`

- [ ] **Step 1: Write the failing test**

`tests/data_engine/test_cli.py`:

```python
from datetime import date
from pathlib import Path
import shutil
import pytest
from click.testing import CliRunner
from data_engine.cli import main


@pytest.fixture
def primed_repo(tmp_path: Path, fixtures_dir: Path) -> Path:
    """Create a temp project root with raw cache primed and the YAML present."""
    root = tmp_path / "project"
    root.mkdir()
    raw = root / "data" / "raw_cache"
    today = date(2026, 4, 25)
    src_dir = raw / "wikipedia_polls" / today.isoformat()
    src_dir.mkdir(parents=True)
    (src_dir / "content.bin").write_bytes(
        (fixtures_dir / "wikipedia_polls_sample.html").read_bytes()
    )
    (src_dir / "meta.json").write_text("{}")
    src_dir = raw / "hoc_results" / today.isoformat()
    src_dir.mkdir(parents=True)
    (src_dir / "content.bin").write_bytes(
        (fixtures_dir / "hoc_results_sample.csv").read_bytes()
    )
    (src_dir / "meta.json").write_text("{}")
    hand = root / "data" / "hand_curated"
    hand.mkdir(parents=True)
    shutil.copy(Path("data/hand_curated/by_elections.yaml"), hand / "by_elections.yaml")
    return root


def test_cli_snapshot_creates_file(primed_repo: Path, monkeypatch):
    monkeypatch.chdir(primed_repo)
    runner = CliRunner()
    result = runner.invoke(main, ["snapshot", "--as-of", "2026-04-25"])
    assert result.exit_code == 0, result.output
    snaps = list((primed_repo / "data" / "snapshots").glob("*.sqlite"))
    assert len(snaps) == 1
    assert "2026-04-25__v1__" in snaps[0].name


def test_cli_snapshot_is_idempotent(primed_repo: Path, monkeypatch):
    monkeypatch.chdir(primed_repo)
    runner = CliRunner()
    runner.invoke(main, ["snapshot", "--as-of", "2026-04-25"])
    snaps_before = sorted((primed_repo / "data" / "snapshots").glob("*.sqlite"))
    runner.invoke(main, ["snapshot", "--as-of", "2026-04-25"])
    snaps_after = sorted((primed_repo / "data" / "snapshots").glob("*.sqlite"))
    assert snaps_before == snaps_after  # no new file


def test_cli_help_lists_subcommands():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert "fetch" in result.output
    assert "snapshot" in result.output
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/data_engine/test_cli.py -v
```

Expected: ImportError.

- [ ] **Step 3: Write implementation**

`data_engine/cli.py`:

```python
from datetime import date, datetime, timezone
from pathlib import Path

import click
import httpx
from data_engine.raw_cache import RawCache
from data_engine.snapshot import BuildSnapshotConfig, build_snapshot
from data_engine.sources.wikipedia_polls import POLLS_URL, fetch_polls_html


HOC_URL = "https://researchbriefings.files.parliament.uk/documents/CBP-10009/HoC-GE2024-results-by-constituency.csv"
USER_AGENT = "seatpredictor/0.0.1 (research)"


def _project_root() -> Path:
    return Path.cwd()


def _raw_cache() -> RawCache:
    return RawCache(root=_project_root() / "data" / "raw_cache")


@click.group()
def main():
    """Data engine: fetch sources, build snapshots."""


@main.command()
@click.option("--refresh", is_flag=True, default=False, help="Force re-fetch even if cached.")
def fetch(refresh: bool):
    """Refresh raw cache for today: download polls + HoC results."""
    today = date.today()
    cache = _raw_cache()

    polls_key = cache.key("wikipedia_polls", today)
    if refresh or not cache.exists(polls_key):
        click.echo(f"Fetching polls from {POLLS_URL}")
        html = fetch_polls_html(POLLS_URL)
        cache.put(polls_key, html.encode("utf-8"), meta={"url": POLLS_URL})
    else:
        click.echo(f"Polls cached for {today}; skipping (use --refresh to force)")

    hoc_key = cache.key("hoc_results", today)
    if refresh or not cache.exists(hoc_key):
        click.echo(f"Fetching HoC results from {HOC_URL}")
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=60.0) as client:
            resp = client.get(HOC_URL)
            resp.raise_for_status()
            cache.put(hoc_key, resp.content, meta={"url": HOC_URL})
    else:
        click.echo(f"HoC results cached for {today}; skipping")


@main.command()
@click.option(
    "--as-of",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="As-of date (YYYY-MM-DD). Defaults to today.",
)
def snapshot(as_of):
    """Build a snapshot from the raw cache + by-elections YAML."""
    as_of_date = (as_of.date() if as_of else date.today())
    cfg = BuildSnapshotConfig(
        as_of_date=as_of_date,
        raw_cache=_raw_cache(),
        out_dir=_project_root() / "data" / "snapshots",
        byelections_yaml=_project_root() / "data" / "hand_curated" / "by_elections.yaml",
    )
    path = build_snapshot(cfg)
    click.echo(f"Snapshot at {path}")


@main.command()
@click.option("--since", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--every-days", type=int, default=7)
def backfill(since, every_days: int):
    """One-time: produce snapshots back to --since, every --every-days."""
    from datetime import timedelta
    start = since.date()
    today = date.today()
    cur = start
    cache = _raw_cache()
    out_dir = _project_root() / "data" / "snapshots"
    yaml_path = _project_root() / "data" / "hand_curated" / "by_elections.yaml"
    while cur <= today:
        cfg = BuildSnapshotConfig(
            as_of_date=cur,
            raw_cache=cache,
            out_dir=out_dir,
            byelections_yaml=yaml_path,
        )
        try:
            path = build_snapshot(cfg)
            click.echo(f"  {cur} → {path.name}")
        except FileNotFoundError as e:
            click.echo(f"  {cur} → SKIP (cache miss: {e})")
        cur = cur + timedelta(days=every_days)
```

- [ ] **Step 4: Re-install (entry point changed)**

```bash
uv pip install -e ".[dev]"
```

- [ ] **Step 5: Run test to verify it passes**

```bash
uv run pytest tests/data_engine/test_cli.py -v
```

Expected: 3 tests PASS.

- [ ] **Step 6: Smoke-test the CLI binary**

```bash
uv run seatpredict-data --help
```

Expected: subcommands `fetch`, `snapshot`, `backfill` listed.

- [ ] **Step 7: Run the full test suite**

```bash
uv run pytest -v
```

Expected: All tests pass (60+ across schema and data_engine).

- [ ] **Step 8: Commit**

```bash
git add data_engine/cli.py tests/data_engine/test_cli.py
git commit -m "feat(data_engine): add seatpredict-data CLI (fetch, snapshot, backfill)"
```

---

## Task 16: End-to-end smoke verification

**Files:**
- Create: `scripts/smoke_verify.py` — runnable verification with explicit assertions
- Modify: `README.md` (add quick-start verification)

- [ ] **Step 1: From a clean state, do a real fetch and snapshot**

```bash
uv run seatpredict-data fetch
uv run seatpredict-data snapshot
```

Expected output:
- `data/raw_cache/wikipedia_polls/<today>/content.bin` exists
- `data/raw_cache/hoc_results/<today>/content.bin` exists
- `data/snapshots/<today>__v1__<hash>.sqlite` exists

If `fetch` errors with HTTP 4xx/5xx, the most likely cause is the Wikipedia or HoC URL has changed; check `POLLS_URL` in `wikipedia_polls.py` and `HOC_URL` in `cli.py`.

- [ ] **Step 2: Create an automated verification script**

`scripts/smoke_verify.py`:

```python
"""End-to-end smoke verification. Run after `seatpredict-data fetch` + `snapshot`.

Asserts:
- Snapshot file exists, contains all expected tables
- 650 UK constituencies in results_2024 (full Westminster set)
- Per-constituency shares sum to ~100%
- Polls table has >=30 rows since GE 2024
- Transfer matrix has at least one non-null cell
- All four seeded by-elections present
"""

import sqlite3
import sys
from pathlib import Path

import pandas as pd


EXPECTED_TABLES = {
    "manifest", "polls", "results_2024",
    "byelections_events", "byelections_results",
    "transfer_weights", "transfer_weights_provenance",
}
EXPECTED_BYELECTIONS = {
    "runcorn_helsby_2025", "hamilton_larkhall_stonehouse_2025",
    "caerphilly_senedd_2025", "gorton_denton_2026",
}


def main() -> int:
    snap_dir = Path("data/snapshots")
    snaps = sorted(snap_dir.glob("*.sqlite"))
    if not snaps:
        print("FAIL: no snapshots found in data/snapshots/", file=sys.stderr)
        return 1
    snap = snaps[-1]
    print(f"Verifying {snap}")
    with sqlite3.connect(str(snap)) as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        missing = EXPECTED_TABLES - tables
        if missing:
            print(f"FAIL: missing tables {missing}", file=sys.stderr)
            return 1
        print(f"  Tables present: OK ({len(EXPECTED_TABLES)} expected)")

        results = pd.read_sql_query("SELECT * FROM results_2024", conn)
        n_seats = results["ons_code"].nunique()
        if n_seats != 650:
            print(f"FAIL: expected 650 constituencies, got {n_seats}", file=sys.stderr)
            return 1
        print(f"  Constituencies: 650 OK")

        share_sums = results.groupby("ons_code")["share"].sum()
        bad = share_sums[(share_sums < 99.0) | (share_sums > 101.0)]
        if not bad.empty:
            print(f"FAIL: {len(bad)} constituencies with shares not summing 99-101", file=sys.stderr)
            print(bad.head(), file=sys.stderr)
            return 1
        print(f"  Share sums in 99-101: all 650 OK")

        polls = pd.read_sql_query("SELECT * FROM polls", conn)
        if len(polls) < 30:
            print(f"FAIL: only {len(polls)} polls extracted (expected >=30)", file=sys.stderr)
            return 1
        print(f"  Polls extracted: {len(polls)} OK")

        events = pd.read_sql_query("SELECT * FROM byelections_events", conn)
        present = set(events["event_id"])
        missing_evs = EXPECTED_BYELECTIONS - present
        if missing_evs:
            print(f"FAIL: missing by-elections {missing_evs}", file=sys.stderr)
            return 1
        print(f"  By-elections seeded: 4 OK")

        weights = pd.read_sql_query("SELECT * FROM transfer_weights", conn)
        if len(weights) == 0:
            print("FAIL: transfer_weights is empty", file=sys.stderr)
            return 1
        print(f"  Transfer matrix cells: {len(weights)} OK")

    print("\nAll smoke checks PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Run smoke verification**

```bash
uv run python scripts/smoke_verify.py
```

Expected: exits 0, prints "All smoke checks PASSED."

If any assertion fails, the script prints which check failed and which file/values caused it. Common diagnoses:
- "expected 650 constituencies": HoC parser dropped rows — inspect Country-name normalisation.
- "X polls with implausible sums": Wikipedia parser missed a column — inspect headers in `_PARTY_HEADER_MAP`.
- "transfer_weights is empty": all by-elections were filtered out — check `threat_party` values in `by_elections.yaml`.

- [ ] **Step 4: Inspect the snapshot from the SQLite CLI**

```bash
sqlite3 data/snapshots/*.sqlite ".tables"
sqlite3 data/snapshots/*.sqlite "SELECT consolidator, source, weight, n FROM transfer_weights ORDER BY nation, consolidator, source;"
```

Expected: rows for england/green, england/lab, scotland/lab, wales/plaid with their source-party flow rates.

- [ ] **Step 5: Re-run snapshot — verify no-op**

```bash
ls data/snapshots/*.sqlite | wc -l
uv run seatpredict-data snapshot
ls data/snapshots/*.sqlite | wc -l
```

Expected: count unchanged.

- [ ] **Step 6: Update README quick-start**

Append to `README.md`:

```markdown
## Quick verification

After install, run:

```bash
uv run seatpredict-data fetch
uv run seatpredict-data snapshot
uv run python scripts/smoke_verify.py
```

The smoke verification asserts: 650 constituencies parsed, shares sum to ~100% per seat, ≥30 polls extracted, all four by-elections seeded, transfer matrix non-empty. If any check fails, the error message indicates which parser to inspect.
```

- [ ] **Step 7: Commit**

```bash
git add scripts/smoke_verify.py README.md
git commit -m "feat: add scripts/smoke_verify.py and document end-to-end check"
```

---

## Self-review notes

**Spec coverage** — every section relevant to Plan A is implemented:
- §3 Project layout — Tasks 1, 7
- §4.1 Wikipedia polls — Task 12
- §4.2 HoC Library 2024 results — Task 10
- §4.3 By-elections YAML — Task 11
- §4.4 Transfer matrix — Task 13
- §4.5 Raw cache — Task 9
- §4.6 Snapshot layout — Tasks 7, 8, 14
- §4.7 CLI surface — Task 15
- §7.1 Schema/contract tests — Tasks 2–7
- §7.2 Data engine tests — Tasks 8–15
- §8 Error handling (source-side fail-loud) — Task 14 raises `FileNotFoundError` on cache miss

**Out of scope (covered by Plan B / Plan C):**
- `schema/prediction.py` (only needed by prediction engine)
- Strategy ABC, uniform_swing, reform_threat_consolidation
- Prediction CLI, runner
- Notebooks
- Drilldown / flips analysis CLIs

**Type/name consistency check** — sweep the public API used across tasks:
- `PartyCode`, `Nation`, `LEFT_BLOC` from `schema.common` — used in tasks 2, 4, 5, 6, 10, 11, 13.
- `Poll`, `Geography` — task 3 only.
- `ConstituencyResult` — task 4 only.
- `ByElectionEvent`, `ByElectionResult`, `EventType` — task 5, used in 11.
- `TransferWeightCell`, `TransferWeightProvenance` — task 6, used implicitly in 13 (DataFrame shape matches).
- `SnapshotManifest` — task 7, used in 8, 14.
- `RawCache`, `CacheKey` — task 9, used in 14, 15.
- `open_snapshot_db`, `write_dataframe`, `read_dataframe`, `write_manifest`, `read_manifest`, `compute_input_hash` — task 8, used in 14.
- `parse_hoc_results` — task 10, used in 14.
- `load_byelections` — task 11, used in 14.
- `parse_polls_html`, `fetch_polls_html`, `POLLS_URL` — task 12, used in 14, 15.
- `derive_transfer_matrix`, `PRIOR_SHARE_THRESHOLD` — task 13, used in 14.
- `BuildSnapshotConfig`, `build_snapshot`, `SCHEMA_VERSION` — task 14, used in 15.

All names consistent across tasks.
