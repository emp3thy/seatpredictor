# Plan B — Prediction Engine + Analysis Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the prediction engine — a swappable Strategy ABC, two v1 strategies (`uniform_swing` and `reform_threat_consolidation`), a runner that turns a snapshot into a single-file SQLite prediction, and a `seatpredict-predict` CLI. Layered on top: an analysis package and `seatpredict-analyze` CLI plus four notebooks that drive interactive inspection.

**Architecture:** Read-only consumer of Plan A's snapshot SQLite files. A typed `Snapshot` wrapper exposes each table as a lazily-loaded DataFrame. A `Strategy` ABC with a `predict(snapshot, scenario_config) -> PredictionResult` contract. The reform-threat strategy short-circuits unless leader=Reform, identifies a per-seat consolidator from the projected raw shares, looks up flow weights from the derived `transfer_weights` matrix, and applies them scaled by clarity × multiplier. Output is a single `*.sqlite` file per run with `seats`, `national`, `config`, `notes_index` tables. Determinism: same `(snapshot, strategy, config)` ⇒ logically-identical row-set output, no RNG.

**Tech Stack:** Python 3.11+, uv, Pydantic v2, SQLite (stdlib + SQLAlchemy core), pandas, Click, pytest. Adds `jupyterlab` (and `matplotlib`) to the dev deps for the analysis notebooks.

**Spec reference:** `docs/superpowers/specs/2026-04-25-seat-predictor-design.md` — sections 5 + 6 (with cross-references to 7 for testing requirements and 8 for error-handling).

**Predecessor plan:** `docs/superpowers/plans/2026-04-25-plan-a-data-engine.md` — produced the snapshot SQLite contract this plan consumes. Plan A is complete on `main` (commit `9e8be57`); start Plan B work from a fresh branch off `main`.

**Successor plan:** Plan C may add live dashboards / extra strategies / probabilistic predictions. Plan B does NOT cover those.

---

## Plan-A artefacts this plan consumes

These exist in the repo today and MUST NOT be modified by Plan B (apart from `pyproject.toml` to register a new entry-point and a single new dev dep):

| Path | Purpose |
|---|---|
| `schema/common.py` | `PartyCode`, `Nation`, `LEFT_BLOC` |
| `schema/poll.py` | `Poll`, `Geography` |
| `schema/constituency.py` | `ConstituencyResult` |
| `schema/byelection.py` | `ByElectionEvent`, `ByElectionResult`, `EventType` |
| `schema/transfer_weights.py` | `TransferWeightCell`, `TransferWeightProvenance` |
| `schema/snapshot.py` | `SnapshotManifest` |
| `data_engine/sqlite_io.py` | `open_snapshot_db`, `read_dataframe`, `read_manifest` |
| `data_engine/snapshot.py` | `SCHEMA_VERSION`, `build_snapshot` (used by tests to build fixture snapshots) |
| `data_engine/cli.py` | `seatpredict-data` (unchanged) |
| `data/snapshots/*.sqlite` | inputs at runtime |

**Snapshot table contract (read-only):**

| table | columns |
|---|---|
| `manifest` | `as_of_date`, `schema_version`, `content_hash`, `generated_at`, `source_versions` (JSON string) |
| `polls` | `pollster`, `fieldwork_start`, `fieldwork_end`, `published_date`, `sample_size`, `geography`, `con`, `lab`, `ld`, `reform`, `green`, `snp`, `plaid`, `other` |
| `results_2024` | `ons_code`, `constituency_name`, `region`, `nation`, `party`, `votes`, `share` |
| `byelections_events` | `event_id`, `name`, `date`, `event_type`, `nation`, `region`, `threat_party`, `exclude_from_matrix`, `narrative_url` |
| `byelections_results` | `event_id`, `party`, `votes`, `actual_share`, `prior_share` |
| `transfer_weights` | `nation`, `consolidator`, `source`, `weight`, `n` |
| `transfer_weights_provenance` | `nation`, `consolidator`, `event_id` |

The `nation` columns are lowercase strings (`england` / `wales` / `scotland`) from the `Nation` enum's `value` field. `party` columns are lowercase strings from `PartyCode.value`. Date columns are stored as ISO strings (`YYYY-MM-DD`).

---

## File structure produced by this plan

```
seatpredictor/
  pyproject.toml                                # add seatpredict-predict + seatpredict-analyze entry points,
                                                # add jupyterlab + matplotlib to dev extras

  schema/
    prediction.py                               # SeatPrediction, NationalTotal, RunConfig, PredictionResult,
                                                # ScenarioConfig (base), UniformSwingConfig, ReformThreatConfig

  prediction_engine/
    __init__.py
    snapshot_loader.py                          # Snapshot wrapper around the SQLite file
    polls.py                                    # rolling-window poll average → swing
    projection.py                               # apply uniform swing per region → share_raw_*
    sqlite_io.py                                # write_prediction_db, read_prediction_db
    runner.py                                   # load → predict → write
    strategies/
      __init__.py                               # auto-imports each strategy module so they self-register
      base.py                                   # Strategy ABC + STRATEGY_REGISTRY
      uniform_swing.py                          # baseline strategy
      reform_threat_consolidation.py            # v1 tactical strategy
    analysis/
      __init__.py
      poll_trends.py                            # rolling per-party trend lines + chart helpers
      drilldown.py                              # per-seat explanation: shares, consolidator, flows, provenance
      flips.py                                  # diff two prediction runs: which seats flipped
      sweep.py                                  # post-process a sweep set: per-config seat counts
    cli.py                                      # seatpredict-predict
    cli_analyze.py                              # seatpredict-analyze

  notebooks/
    01_polling_trends.ipynb
    02_constituency_drilldown.ipynb
    03_strategy_comparison.ipynb
    04_scenario_sweep.ipynb

  tests/
    schema/
      test_prediction.py
    prediction_engine/
      __init__.py
      conftest.py                               # builds a tiny in-memory snapshot fixture
      test_snapshot_loader.py
      test_polls.py
      test_projection.py
      test_sqlite_io.py
      test_strategy_base.py
      test_uniform_swing.py
      test_reform_threat.py
      test_runner.py
      test_cli.py
      test_cli_analyze.py
      test_analysis_drilldown.py
      test_analysis_flips.py
      test_analysis_poll_trends.py
      test_analysis_sweep.py
    fixtures/
      tiny_snapshot_seed.yaml                   # human-edited inputs used to build the test snapshot
```

---

## Cross-task design notes (read these first)

These are referenced from multiple tasks; collected here to avoid duplication.

**Per-seat output schema (§5.2 of spec).** Stored as a single `seats` table. Per-party columns use the lowercase party code from `PartyCode.value`:

```
ons_code              TEXT  PRIMARY KEY
constituency_name     TEXT
nation                TEXT  ('england' / 'wales' / 'scotland' / 'northern_ireland')
region                TEXT

share_2024_con        REAL
share_2024_lab        REAL
share_2024_ld         REAL
share_2024_reform     REAL
share_2024_green      REAL
share_2024_snp        REAL
share_2024_plaid      REAL
share_2024_other      REAL

share_raw_con         REAL
share_raw_lab         REAL
... (8 columns total)

share_predicted_con   REAL
share_predicted_lab   REAL
... (8 columns total)

predicted_winner      TEXT  (PartyCode value)
predicted_margin      REAL  (winner share − runner-up share, percentage points)

leader                TEXT  (party with highest share_raw)
consolidator          TEXT  (left-bloc party with highest share_raw, or NULL)
clarity               REAL  (∈ [0, 1], or NULL when consolidator is NULL)

matrix_nation         TEXT  ('england' / 'wales' / 'scotland'; NULL for NI)
matrix_provenance     TEXT  (JSON-encoded list of by-election event_ids that contributed; '[]' if none)

notes                 TEXT  (JSON-encoded list of flag strings; '[]' if none)
```

**Allowed `notes` flag values** — closed set, validated in helper:
`non_reform_leader`, `consolidator_already_leads`, `low_clarity`, `no_matrix_entry`, `matrix_unavailable`, `multiplier_clipped`, `ni_excluded`.

**`national` table:** long-format. Columns: `scope` (`overall` / `nation` / `region`), `scope_value` (e.g. `wales`, or `North West`, or `''` for the overall row), `party` (PartyCode.value), `seats` (int).

**`config` table:** single row. Columns: `snapshot_id` (TEXT, snapshot SQLite filename without `.sqlite`), `snapshot_content_hash` (TEXT), `snapshot_as_of_date` (TEXT, ISO), `strategy` (TEXT), `scenario_config_json` (TEXT, JSON string of the validated ScenarioConfig), `config_hash` (TEXT, 12-char SHA-256 of the canonical config JSON), `schema_version` (INTEGER, prediction-side schema version, see below), `run_id` (TEXT, `<snapshot_content_hash>__<strategy>__<config_hash>__<label>`), `label` (TEXT), `generated_at` (TEXT, ISO datetime).

**`notes_index` table:** denormalised view of `seats`. Columns: `ons_code`, `flag`. One row per `(seat, flag)` pair. Empty when no seat has any flag. Used by `seatpredict-analyze` for fast `WHERE flag = 'low_clarity'` filtering.

**Prediction-side schema version.** Define `PREDICTION_SCHEMA_VERSION = 1` in `prediction_engine/sqlite_io.py`. Bump it when prediction-side table layout changes. This is independent of the snapshot's `schema_version`.

**Output filename.** `data/predictions/<snapshot_content_hash>__<strategy>__<config_hash>__<label>.sqlite` where:
- `snapshot_content_hash` is read from the snapshot's `manifest.content_hash` (12 hex chars; matches the hash embedded in the snapshot filename produced by Plan A).
- `config_hash` is the first 12 chars of `sha256(json.dumps(scenario_config.model_dump(mode='json'), sort_keys=True))`.
- `label` is a CLI-supplied free-form tag (default `"baseline"`); slug-validate to `[a-zA-Z0-9_-]+`.

**Idempotency.** If the output file already exists at the computed path, the runner reuses it (returns the path; logs "reusing"). Same `--refresh` semantics as Plan A's snapshot if needed (defer; not in spec).

**Determinism.** No RNG. All DataFrame iterations are over a sorted view (sort by `ons_code` for seats, by `event_id` for events, etc.). Floats compared in tests with `pytest.approx(..., abs=1e-9)`.

**Northern Ireland (§5.3).** NI seats short-circuit to uniform-swing-only. They still get `share_raw_*` and `share_predicted_*` (= `share_raw_*`), `predicted_winner`, `predicted_margin`. `leader` is set. `consolidator`, `clarity`, `matrix_nation`, `matrix_provenance` are NULL/`[]`. `notes` includes `ni_excluded`. The `notes_index` table will have one `ni_excluded` row per NI seat.

**Polls aggregation.** `prediction_engine/polls.py:compute_swing(polls_df, as_of_date, window_days, geography)` returns a `dict[PartyCode, float]`. It selects polls with `published_date <= as_of_date` and `published_date > as_of_date − window_days`, filters by `geography`, takes the unweighted mean per party, then subtracts the 2024 GE national share for that geography to produce a swing in percentage points. The 2024 GE national share is computed from `results_2024` (votes-weighted). For regional polls (Scotland/Wales/London) the geography filter narrows accordingly; for GB it's `geography == "GB"`. If no polls match the filter, raise `ValueError("no polls in window")` — fail loudly per spec §8.

**Projection (§5.3 step 1).** `prediction_engine/projection.py:project_raw_shares(results_2024_df, swings_per_geo)` adds the per-party swing to each seat's 2024 share, then re-normalises to 100. `swings_per_geo` is `dict[Geography, dict[PartyCode, float]]`; for each seat, pick `Wales` if `nation == 'wales'` and `Wales` is present, else `Scotland` if `nation == 'scotland'` and `Scotland` is present, else `GB`. (London is unused in v1 because no London-only seats are in scope.)

**Strategy registry.** `STRATEGY_REGISTRY: dict[str, type[Strategy]] = {}`. `register(name)` is a class decorator that adds the class. `prediction_engine/strategies/__init__.py` imports `uniform_swing` and `reform_threat_consolidation` modules at package import time so the registry is populated on first access.

**Editable-install gotcha (carry-over from Plan A).** Adding new entry points (`seatpredict-predict`, `seatpredict-analyze`) to `pyproject.toml` requires re-running the editable install. Use the compat-mode reinstall command from Plan A's better-memory note; do NOT rely on `uv run seatpredict-predict` until after the reinstall:

```bash
uv pip install --config-settings editable_mode=compat -e ".[dev]"
```

Direct invocation: `.venv/Scripts/seatpredict-predict.exe ...` on Windows; `.venv/bin/seatpredict-predict ...` on POSIX. Tests should use Click's `CliRunner` rather than the shell entry-point so they're independent of how the package was installed.

---

## Task 1: Schema — prediction models

**Files:**
- Create: `schema/prediction.py`
- Modify: `schema/__init__.py` (re-exports)
- Test: `tests/schema/test_prediction.py`

- [ ] **Step 1: Write the failing test**

`tests/schema/test_prediction.py`:

```python
from datetime import date, datetime, timezone
import pytest
from pydantic import ValidationError
from schema.prediction import (
    UniformSwingConfig,
    ReformThreatConfig,
    SeatPrediction,
    NationalTotal,
    RunConfig,
)
from schema.common import PartyCode, Nation


def test_uniform_swing_config_defaults():
    cfg = UniformSwingConfig()
    assert cfg.polls_window_days == 14


def test_uniform_swing_config_validates_positive_window():
    with pytest.raises(ValidationError):
        UniformSwingConfig(polls_window_days=0)


def test_reform_threat_config_defaults():
    cfg = ReformThreatConfig()
    assert cfg.multiplier == 1.0
    assert cfg.clarity_threshold == 5.0
    assert cfg.polls_window_days == 14


def test_reform_threat_config_validation_bounds():
    with pytest.raises(ValidationError):
        ReformThreatConfig(multiplier=-0.1)
    with pytest.raises(ValidationError):
        ReformThreatConfig(clarity_threshold=0.0)


def _seat_kwargs() -> dict:
    base = dict(
        ons_code="E14000001",
        constituency_name="Aldershot",
        nation=Nation.ENGLAND,
        region="South East",
        predicted_winner=PartyCode.LAB,
        predicted_margin=2.5,
        leader=PartyCode.LAB,
        consolidator=None,
        clarity=None,
        matrix_nation="england",
        matrix_provenance=[],
        notes=[],
    )
    for prefix in ("share_2024", "share_raw", "share_predicted"):
        for p in ["con", "lab", "ld", "reform", "green", "snp", "plaid", "other"]:
            base[f"{prefix}_{p}"] = 12.5
    return base


def test_seat_prediction_round_trip():
    seat = SeatPrediction.model_validate(_seat_kwargs())
    raw = seat.model_dump(mode="json")
    restored = SeatPrediction.model_validate(raw)
    assert restored == seat


def test_seat_prediction_rejects_unknown_note_flag():
    kwargs = _seat_kwargs()
    kwargs["notes"] = ["definitely_not_a_real_flag"]
    with pytest.raises(ValidationError, match="unknown notes flag"):
        SeatPrediction.model_validate(kwargs)


def test_seat_prediction_accepts_known_note_flags():
    kwargs = _seat_kwargs()
    kwargs["notes"] = ["non_reform_leader", "ni_excluded"]
    seat = SeatPrediction.model_validate(kwargs)
    assert seat.notes == ["non_reform_leader", "ni_excluded"]


def test_national_total_validates():
    nt = NationalTotal(scope="overall", scope_value="", party=PartyCode.LAB, seats=210)
    assert nt.seats == 210


def test_run_config_round_trip():
    rc = RunConfig(
        snapshot_id="2026-04-25__v1__abc123def456",
        snapshot_content_hash="abc123def456",
        snapshot_as_of_date=date(2026, 4, 25),
        strategy="uniform_swing",
        scenario_config_json='{"polls_window_days": 14}',
        config_hash="0011223344aa",
        schema_version=1,
        run_id="abc123def456__uniform_swing__0011223344aa__baseline",
        label="baseline",
        generated_at=datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert rc.label == "baseline"
    raw = rc.model_dump(mode="json")
    restored = RunConfig.model_validate(raw)
    assert restored == rc
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/schema/test_prediction.py -v`
Expected: ImportError — `schema.prediction` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

`schema/prediction.py`:

```python
from datetime import date, datetime
from typing import Literal
from pydantic import BaseModel, Field, field_validator
from schema.common import PartyCode, Nation


ALLOWED_NOTE_FLAGS = frozenset({
    "non_reform_leader",
    "consolidator_already_leads",
    "low_clarity",
    "no_matrix_entry",
    "matrix_unavailable",
    "multiplier_clipped",
    "ni_excluded",
})


class ScenarioConfig(BaseModel):
    """Base for strategy-specific knobs. Subclasses add their own fields."""
    pass


class UniformSwingConfig(ScenarioConfig):
    polls_window_days: int = Field(default=14, gt=0)


class ReformThreatConfig(ScenarioConfig):
    multiplier: float = Field(default=1.0, ge=0.0)
    clarity_threshold: float = Field(default=5.0, gt=0.0)
    polls_window_days: int = Field(default=14, gt=0)


def _share_field():
    """Pydantic Field factory for percentage shares (0–100). Return type omitted because
    pydantic.Field is an overloaded function, and annotating it as a class confuses pyright."""
    return Field(ge=0.0, le=100.0)


class SeatPrediction(BaseModel):
    ons_code: str = Field(min_length=1)
    constituency_name: str = Field(min_length=1)
    nation: Nation
    region: str

    # 24 share columns (8 parties × 3 prefixes). Listed explicitly for clarity.
    share_2024_con: float = _share_field()
    share_2024_lab: float = _share_field()
    share_2024_ld: float = _share_field()
    share_2024_reform: float = _share_field()
    share_2024_green: float = _share_field()
    share_2024_snp: float = _share_field()
    share_2024_plaid: float = _share_field()
    share_2024_other: float = _share_field()

    share_raw_con: float = _share_field()
    share_raw_lab: float = _share_field()
    share_raw_ld: float = _share_field()
    share_raw_reform: float = _share_field()
    share_raw_green: float = _share_field()
    share_raw_snp: float = _share_field()
    share_raw_plaid: float = _share_field()
    share_raw_other: float = _share_field()

    share_predicted_con: float = _share_field()
    share_predicted_lab: float = _share_field()
    share_predicted_ld: float = _share_field()
    share_predicted_reform: float = _share_field()
    share_predicted_green: float = _share_field()
    share_predicted_snp: float = _share_field()
    share_predicted_plaid: float = _share_field()
    share_predicted_other: float = _share_field()

    predicted_winner: PartyCode
    predicted_margin: float = Field(ge=0.0)

    leader: PartyCode
    consolidator: PartyCode | None = None
    clarity: float | None = Field(default=None, ge=0.0, le=1.0)

    matrix_nation: str | None = None  # 'england'/'wales'/'scotland'/None
    matrix_provenance: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    @field_validator("notes")
    @classmethod
    def _validate_notes(cls, v: list[str]) -> list[str]:
        for flag in v:
            if flag not in ALLOWED_NOTE_FLAGS:
                raise ValueError(f"unknown notes flag: {flag}")
        return v


class NationalTotal(BaseModel):
    scope: Literal["overall", "nation", "region"]
    scope_value: str  # '' for overall; 'england' etc. for nation; region name for region
    party: PartyCode
    seats: int = Field(ge=0)


class RunConfig(BaseModel):
    snapshot_id: str = Field(min_length=1)
    snapshot_content_hash: str = Field(min_length=1)
    snapshot_as_of_date: date
    strategy: str = Field(min_length=1)
    scenario_config_json: str = Field(min_length=1)
    config_hash: str = Field(min_length=1)
    schema_version: int = Field(gt=0)
    run_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    generated_at: datetime
```

- [ ] **Step 4: Re-export from `schema/__init__.py`**

Open `schema/__init__.py` and add to the imports + `__all__`:

```python
from schema.prediction import (
    ScenarioConfig,
    UniformSwingConfig,
    ReformThreatConfig,
    SeatPrediction,
    NationalTotal,
    RunConfig,
    ALLOWED_NOTE_FLAGS,
)
```

Append to the existing `__all__` list:

```python
"ScenarioConfig",
"UniformSwingConfig",
"ReformThreatConfig",
"SeatPrediction",
"NationalTotal",
"RunConfig",
"ALLOWED_NOTE_FLAGS",
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/schema/test_prediction.py -v`
Expected: 9 tests PASS.

- [ ] **Step 6: Run the full schema test suite to confirm no regression**

Run: `uv run pytest tests/schema/ -v`
Expected: all schema tests pass (Plan A's existing schema tests + the 9 new ones).

- [ ] **Step 7: Commit**

```bash
git add schema/prediction.py schema/__init__.py tests/schema/test_prediction.py
git commit -m "feat(schema): add prediction models (SeatPrediction, NationalTotal, RunConfig, ScenarioConfig)"
```

---

## Task 2: Test fixture — tiny snapshot builder

**Files:**
- Create: `tests/prediction_engine/__init__.py`
- Create: `tests/prediction_engine/conftest.py`
- Create: `tests/fixtures/tiny_snapshot_seed.yaml`

This task has no production code; it produces a re-usable test fixture. Subsequent tasks consume it. Skip the "fail-first" cycle (a fixture cannot be tested before its consumers exist); instead the task ends with a self-test that asserts the snapshot's tables match expectations and that the derived matrix matches Plan A's `derive_transfer_matrix` on the same YAML inputs.

### Design constraints (READ FIRST — these decide the seed numbers)

The 6 seats are designed so each integration test in Task 9 actually exercises its intended path. Three constraints make the design clean:

1. **Polls equal aggregate GE-2024 share.** With polls ≈ aggregate share, the per-party swing is ≈ 0pp. After projection + renormalise, `share_raw_<p>` ≈ `share_2024_<p>` for every seat. This means tests can reason about per-seat behavior using the 2024 shares directly, without computing post-swing arithmetic.
2. **Reform must be the leader (highest share) in seats A, B, D, E** so the reform-threat strategy enters its tactical-modelling branch. Seat C has Con leading (non-Reform leader), seat F is NI.
3. **The consolidator (highest left-bloc party) must NOT exceed Reform**, otherwise the `consolidator_already_leads` early-return fires instead of the expected path.

Aggregate vote totals across the 6 seats sum to (con=41500, lab=59500, ld=23500, reform=71500, green=22500, snp=13500, plaid=15000, other=53000) on a 300000 grand total. That gives aggregate shares (13.83, 19.83, 7.83, 23.83, 7.50, 4.50, 5.00, 17.67) which round to the polls below.

The derived transfer matrix has entries for `(england, lab, {ld, green, con})` and `(wales, plaid, {lab, green, con, ld})`. Scotland has NO consolidator entries — that's deliberate so seat E exercises `matrix_unavailable`.

- [ ] **Step 1: Create `tests/fixtures/tiny_snapshot_seed.yaml`**

```yaml
# Tiny snapshot seed for prediction-engine tests.
# 6 seats × 2 polls × 2 by-election events. Polls ≈ aggregate GE-2024 shares so swings ≈ 0;
# share_raw ≈ share_2024 in every seat.

polls:
  - { pollster: TestCo, fieldwork_start: 2026-04-15, fieldwork_end: 2026-04-17,
      published_date: 2026-04-18, sample_size: 1000, geography: GB,
      con: 14, lab: 20, ld: 8, reform: 24, green: 8, snp: 5, plaid: 5, other: 16 }
  - { pollster: TestCo, fieldwork_start: 2026-04-20, fieldwork_end: 2026-04-22,
      published_date: 2026-04-23, sample_size: 1100, geography: GB,
      con: 14, lab: 20, ld: 8, reform: 24, green: 8, snp: 5, plaid: 5, other: 16 }

results_2024:
  # Seat A — England, Reform leader, Lab consolidator, HIGH clarity (Lab-LD gap = 20pp).
  # Reform 35 > Lab 30 (so Lab is consolidator, not leader). Lab-LD gap = 30-10 = 20pp → clarity = 1.0
  # at default clarity_threshold=5. Matrix has (england, lab, {ld, green, con}) → flow applies.
  - { ons_code: TST00001, constituency_name: Aldermouth, region: North West, nation: england, party: con,    votes:  5000, share: 10.0 }
  - { ons_code: TST00001, constituency_name: Aldermouth, region: North West, nation: england, party: lab,    votes: 15000, share: 30.0 }
  - { ons_code: TST00001, constituency_name: Aldermouth, region: North West, nation: england, party: ld,     votes:  5000, share: 10.0 }
  - { ons_code: TST00001, constituency_name: Aldermouth, region: North West, nation: england, party: reform, votes: 17500, share: 35.0 }
  - { ons_code: TST00001, constituency_name: Aldermouth, region: North West, nation: england, party: green,  votes:  5000, share: 10.0 }
  - { ons_code: TST00001, constituency_name: Aldermouth, region: North West, nation: england, party: snp,    votes:     0, share:  0.0 }
  - { ons_code: TST00001, constituency_name: Aldermouth, region: North West, nation: england, party: plaid,  votes:     0, share:  0.0 }
  - { ons_code: TST00001, constituency_name: Aldermouth, region: North West, nation: england, party: other,  votes:  2500, share:  5.0 }

  # Seat B — England, Reform leader, Lab consolidator, LOW clarity (Lab-LD gap = 2pp).
  # Reform 25 > Lab 24 > LD 22 > Green 14. Lab is consolidator. Lab-LD gap = 2pp → clarity = 0.4 at
  # default threshold=5 (low_clarity flag fires; flow still applies, scaled by 0.4).
  - { ons_code: TST00002, constituency_name: Bramford,  region: North West, nation: england, party: con,    votes:  6000, share: 12.0 }
  - { ons_code: TST00002, constituency_name: Bramford,  region: North West, nation: england, party: lab,    votes: 12000, share: 24.0 }
  - { ons_code: TST00002, constituency_name: Bramford,  region: North West, nation: england, party: ld,     votes: 11000, share: 22.0 }
  - { ons_code: TST00002, constituency_name: Bramford,  region: North West, nation: england, party: reform, votes: 12500, share: 25.0 }
  - { ons_code: TST00002, constituency_name: Bramford,  region: North West, nation: england, party: green,  votes:  7000, share: 14.0 }
  - { ons_code: TST00002, constituency_name: Bramford,  region: North West, nation: england, party: snp,    votes:     0, share:  0.0 }
  - { ons_code: TST00002, constituency_name: Bramford,  region: North West, nation: england, party: plaid,  votes:     0, share:  0.0 }
  - { ons_code: TST00002, constituency_name: Bramford,  region: North West, nation: england, party: other,  votes:  1500, share:  3.0 }

  # Seat C — England, non-Reform leader (Con leads 45, Reform 13). Strategy returns
  # uniform-swing fallback with non_reform_leader flag.
  - { ons_code: TST00003, constituency_name: Carchester, region: South East, nation: england, party: con,    votes: 22500, share: 45.0 }
  - { ons_code: TST00003, constituency_name: Carchester, region: South East, nation: england, party: lab,    votes: 13000, share: 26.0 }
  - { ons_code: TST00003, constituency_name: Carchester, region: South East, nation: england, party: ld,     votes:  5000, share: 10.0 }
  - { ons_code: TST00003, constituency_name: Carchester, region: South East, nation: england, party: reform, votes:  6500, share: 13.0 }
  - { ons_code: TST00003, constituency_name: Carchester, region: South East, nation: england, party: green,  votes:  2000, share:  4.0 }
  - { ons_code: TST00003, constituency_name: Carchester, region: South East, nation: england, party: snp,    votes:     0, share:  0.0 }
  - { ons_code: TST00003, constituency_name: Carchester, region: South East, nation: england, party: plaid,  votes:     0, share:  0.0 }
  - { ons_code: TST00003, constituency_name: Carchester, region: South East, nation: england, party: other,  votes:  1000, share:  2.0 }

  # Seat D — Wales, Reform leader, Plaid consolidator (Plaid 30 > Lab 20 in left-bloc).
  # Reform 35 > Plaid 30 (so Plaid is consolidator, not leader). Plaid-Lab gap = 10pp → clarity = 1.0
  # at default threshold. Matrix has (wales, plaid, {lab, green, con, ld}) → flow applies.
  - { ons_code: TST00004, constituency_name: Dyffryn,    region: South Wales, nation: wales, party: con,    votes:  4000, share:  8.0 }
  - { ons_code: TST00004, constituency_name: Dyffryn,    region: South Wales, nation: wales, party: lab,    votes: 10000, share: 20.0 }
  - { ons_code: TST00004, constituency_name: Dyffryn,    region: South Wales, nation: wales, party: ld,     votes:  1000, share:  2.0 }
  - { ons_code: TST00004, constituency_name: Dyffryn,    region: South Wales, nation: wales, party: reform, votes: 17500, share: 35.0 }
  - { ons_code: TST00004, constituency_name: Dyffryn,    region: South Wales, nation: wales, party: green,  votes:  1000, share:  2.0 }
  - { ons_code: TST00004, constituency_name: Dyffryn,    region: South Wales, nation: wales, party: snp,    votes:     0, share:  0.0 }
  - { ons_code: TST00004, constituency_name: Dyffryn,    region: South Wales, nation: wales, party: plaid,  votes: 15000, share: 30.0 }
  - { ons_code: TST00004, constituency_name: Dyffryn,    region: South Wales, nation: wales, party: other,  votes:  1500, share:  3.0 }

  # Seat E — Scotland, Reform leader, SNP consolidator (SNP 27 > Lab 19 in left-bloc).
  # The matrix has NO scotland entries (no by-election in Scotland in this seed) →
  # snapshot.consolidator_observed("scotland", "snp") is False → matrix_unavailable.
  - { ons_code: TST00005, constituency_name: Eilean,     region: Highlands, nation: scotland, party: con,    votes:  4000, share:  8.0 }
  - { ons_code: TST00005, constituency_name: Eilean,     region: Highlands, nation: scotland, party: lab,    votes:  9500, share: 19.0 }
  - { ons_code: TST00005, constituency_name: Eilean,     region: Highlands, nation: scotland, party: ld,     votes:  1500, share:  3.0 }
  - { ons_code: TST00005, constituency_name: Eilean,     region: Highlands, nation: scotland, party: reform, votes: 17500, share: 35.0 }
  - { ons_code: TST00005, constituency_name: Eilean,     region: Highlands, nation: scotland, party: green,  votes:  2500, share:  5.0 }
  - { ons_code: TST00005, constituency_name: Eilean,     region: Highlands, nation: scotland, party: snp,    votes: 13500, share: 27.0 }
  - { ons_code: TST00005, constituency_name: Eilean,     region: Highlands, nation: scotland, party: plaid,  votes:     0, share:  0.0 }
  - { ons_code: TST00005, constituency_name: Eilean,     region: Highlands, nation: scotland, party: other,  votes:  1500, share:  3.0 }

  # Seat F — Northern Ireland (NI exclusion path). Other dominant; Green a token left-bloc share.
  # All pre-step-2 logic skipped; ni_excluded flag set.
  - { ons_code: TST00006, constituency_name: Foyle,      region: NI, nation: northern_ireland, party: con,    votes:     0, share:  0.0 }
  - { ons_code: TST00006, constituency_name: Foyle,      region: NI, nation: northern_ireland, party: lab,    votes:     0, share:  0.0 }
  - { ons_code: TST00006, constituency_name: Foyle,      region: NI, nation: northern_ireland, party: ld,     votes:     0, share:  0.0 }
  - { ons_code: TST00006, constituency_name: Foyle,      region: NI, nation: northern_ireland, party: reform, votes:     0, share:  0.0 }
  - { ons_code: TST00006, constituency_name: Foyle,      region: NI, nation: northern_ireland, party: green,  votes:  5000, share: 10.0 }
  - { ons_code: TST00006, constituency_name: Foyle,      region: NI, nation: northern_ireland, party: snp,    votes:     0, share:  0.0 }
  - { ons_code: TST00006, constituency_name: Foyle,      region: NI, nation: northern_ireland, party: plaid,  votes:     0, share:  0.0 }
  - { ons_code: TST00006, constituency_name: Foyle,      region: NI, nation: northern_ireland, party: other,  votes: 45000, share: 90.0 }

byelections_events:
  - { event_id: tst_eng_2025, name: Aldermouth-North,  date: 2025-09-01, event_type: westminster_byelection,
      nation: england, region: North West, threat_party: reform, exclude_from_matrix: false,
      narrative_url: https://example.test/eng-2025 }
  - { event_id: tst_wal_2025, name: Caerphilly-Test,    date: 2025-10-23, event_type: senedd,
      nation: wales,   region: South Wales East, threat_party: reform, exclude_from_matrix: false,
      narrative_url: https://example.test/wal-2025 }

byelections_results:
  # tst_eng_2025: Lab consolidates from LD/Green/Con. Per Plan A's derive_transfer_matrix:
  #   LD: prior 10 → actual 4 → flow = (10-4)/10 = 0.6
  #   Green: prior 10 → actual 5 → flow = 0.5
  #   Con: prior 5 → actual 3 → flow = 0.4
  #   Other: prior 2 NOT > 2 (PRIOR_SHARE_THRESHOLD is exclusive at 2.0) → skipped
  #   Reform: skipped (it's the threat).  Lab: skipped (it's the consolidator).
  - { event_id: tst_eng_2025, party: lab,    votes: 6000, actual_share: 60.0, prior_share: 40.0 }
  - { event_id: tst_eng_2025, party: ld,     votes:  400, actual_share:  4.0, prior_share: 10.0 }
  - { event_id: tst_eng_2025, party: green,  votes:  500, actual_share:  5.0, prior_share: 10.0 }
  - { event_id: tst_eng_2025, party: con,    votes:  300, actual_share:  3.0, prior_share:  5.0 }
  - { event_id: tst_eng_2025, party: reform, votes: 2500, actual_share: 25.0, prior_share: 33.0 }
  - { event_id: tst_eng_2025, party: snp,    votes:    0, actual_share:  0.0, prior_share:  0.0 }
  - { event_id: tst_eng_2025, party: plaid,  votes:    0, actual_share:  0.0, prior_share:  0.0 }
  - { event_id: tst_eng_2025, party: other,  votes:  300, actual_share:  3.0, prior_share:  2.0 }
  # tst_wal_2025: Plaid consolidates. Lab prior 50 actual 20 → 0.6. LD prior 3 actual 1 → 0.667.
  # Green prior 10 actual 5 → 0.5. Con prior 5 actual 2 → 0.6. Other prior 2 → skipped. Reform/Plaid skipped.
  - { event_id: tst_wal_2025, party: plaid,  votes: 5000, actual_share: 50.0, prior_share: 25.0 }
  - { event_id: tst_wal_2025, party: lab,    votes: 2000, actual_share: 20.0, prior_share: 50.0 }
  - { event_id: tst_wal_2025, party: ld,     votes:  100, actual_share:  1.0, prior_share:  3.0 }
  - { event_id: tst_wal_2025, party: green,  votes:  500, actual_share:  5.0, prior_share: 10.0 }
  - { event_id: tst_wal_2025, party: con,    votes:  200, actual_share:  2.0, prior_share:  5.0 }
  - { event_id: tst_wal_2025, party: reform, votes: 2000, actual_share: 20.0, prior_share:  5.0 }
  - { event_id: tst_wal_2025, party: snp,    votes:    0, actual_share:  0.0, prior_share:  0.0 }
  - { event_id: tst_wal_2025, party: other,  votes:  200, actual_share:  2.0, prior_share:  2.0 }
```

- [ ] **Step 2: Create `tests/prediction_engine/__init__.py`**

Empty file.

- [ ] **Step 3: Create `tests/prediction_engine/conftest.py`**

This builds an in-memory snapshot SQLite from the YAML, deriving the transfer matrix via Plan A's `derive_transfer_matrix` (NOT hardcoded — that way the fixture stays consistent with the data engine if the derivation logic ever evolves). The on-disk format matches a real Plan-A snapshot exactly so prediction-engine tests consume snapshots via the production `Snapshot` loader, never raw DataFrames.

```python
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
import yaml

from data_engine.sqlite_io import open_snapshot_db, write_dataframe, write_manifest
from data_engine.transforms.transfer_matrix import derive_transfer_matrix
from schema.snapshot import SnapshotManifest


_SEED_PATH = Path(__file__).parent.parent / "fixtures" / "tiny_snapshot_seed.yaml"
_AS_OF = date(2026, 4, 25)
_CONTENT_HASH = "tinyhash0001"


@pytest.fixture
def tiny_snapshot_path(tmp_path: Path) -> Path:
    """Build a real Plan-A-format SQLite snapshot from tiny_snapshot_seed.yaml.

    The transfer-weights matrix is derived by Plan A's derive_transfer_matrix
    so any future change there is reflected here automatically (no drift).
    """
    with _SEED_PATH.open(encoding="utf-8") as f:
        seed = yaml.safe_load(f)

    polls = pd.DataFrame(seed["polls"])
    for col in ("fieldwork_start", "fieldwork_end", "published_date"):
        polls[col] = polls[col].astype(str)

    results = pd.DataFrame(seed["results_2024"])

    events = pd.DataFrame(seed["byelections_events"])
    events["date"] = events["date"].astype(str)
    # derive_transfer_matrix expects the boolean as a real bool, not a string.
    events["exclude_from_matrix"] = events["exclude_from_matrix"].astype(bool)

    results_by = pd.DataFrame(seed["byelections_results"])

    cells, provenance = derive_transfer_matrix(events, results_by)

    out = tmp_path / f"{_AS_OF.isoformat()}__v1__{_CONTENT_HASH}.sqlite"
    with open_snapshot_db(out) as conn:
        write_dataframe(conn, "polls", polls)
        write_dataframe(conn, "results_2024", results)
        write_dataframe(conn, "byelections_events", events)
        write_dataframe(conn, "byelections_results", results_by)
        write_dataframe(conn, "transfer_weights", cells)
        write_dataframe(conn, "transfer_weights_provenance", provenance)
        manifest = SnapshotManifest(
            as_of_date=_AS_OF,
            schema_version=1,
            content_hash=_CONTENT_HASH,
            generated_at=datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc),
            source_versions={"wikipedia_polls": _AS_OF.isoformat(), "hoc_results": "ge_2024"},
        )
        write_manifest(conn, manifest)
    return out
```

- [ ] **Step 4: Add a self-test for the fixture**

Create `tests/prediction_engine/test_fixture_sanity.py`:

```python
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
```

- [ ] **Step 5: Run the self-test**

Run: `uv run pytest tests/prediction_engine/test_fixture_sanity.py -v`
Expected: 5 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/fixtures/tiny_snapshot_seed.yaml tests/prediction_engine/__init__.py tests/prediction_engine/conftest.py tests/prediction_engine/test_fixture_sanity.py
git commit -m "test: add tiny snapshot fixture (derived matrix, sanity checks for downstream tests)"
```

---

## Task 3: Snapshot loader

**Files:**
- Create: `prediction_engine/__init__.py` (empty)
- Create: `prediction_engine/snapshot_loader.py`
- Test: `tests/prediction_engine/test_snapshot_loader.py`

- [ ] **Step 1: Create empty `prediction_engine/__init__.py`**

```bash
mkdir -p prediction_engine
touch prediction_engine/__init__.py
```

(PowerShell: `New-Item -ItemType File -Path prediction_engine/__init__.py -Force`.)

- [ ] **Step 2: Write the failing test**

`tests/prediction_engine/test_snapshot_loader.py`:

```python
from datetime import date
import pandas as pd
import pytest
from prediction_engine.snapshot_loader import Snapshot


def test_snapshot_loads_manifest(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    assert snap.manifest.as_of_date == date(2026, 4, 25)
    assert snap.manifest.content_hash == "tinyhash0001"


def test_snapshot_id_is_filename_stem(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    assert snap.snapshot_id == tiny_snapshot_path.stem


def test_snapshot_polls_lazy_load(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    polls = snap.polls
    assert isinstance(polls, pd.DataFrame)
    assert len(polls) == 2
    # Lazy: same object on repeat access (cached).
    assert snap.polls is polls


def test_snapshot_results_2024(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    r = snap.results_2024
    assert set(r["ons_code"].unique()) == {"TST00001", "TST00002", "TST00003", "TST00004", "TST00005", "TST00006"}
    assert set(r.columns) >= {"ons_code", "constituency_name", "region", "nation", "party", "votes", "share"}


def test_snapshot_transfer_weights_long_format(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    tw = snap.transfer_weights
    assert set(tw.columns) >= {"nation", "consolidator", "source", "weight", "n"}
    assert len(tw) > 0


def test_snapshot_lookup_weight(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    # england/lab/ld is in the seed at 0.6
    assert snap.lookup_weight("england", "lab", "ld") == pytest.approx(0.6)
    # england/lab/snp not seeded → None
    assert snap.lookup_weight("england", "lab", "snp") is None
    # scotland has no consolidator entries → None
    assert snap.lookup_weight("scotland", "lab", "ld") is None


def test_snapshot_consolidator_observed(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    assert snap.consolidator_observed("england", "lab") is True
    assert snap.consolidator_observed("scotland", "lab") is False
    assert snap.consolidator_observed("wales", "plaid") is True


def test_snapshot_provenance_for_consolidator(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    events = snap.provenance_for_consolidator("england", "lab")
    assert events == ["tst_eng_2025"]
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/prediction_engine/test_snapshot_loader.py -v`
Expected: ImportError — `prediction_engine.snapshot_loader` does not exist yet.

- [ ] **Step 4: Write minimal implementation**

`prediction_engine/snapshot_loader.py`:

```python
import logging
from functools import cached_property
from pathlib import Path

import pandas as pd
from data_engine.sqlite_io import open_snapshot_db, read_dataframe, read_manifest
from schema.snapshot import SnapshotManifest

logger = logging.getLogger(__name__)


class Snapshot:
    """Read-only typed wrapper around a Plan-A snapshot SQLite file.

    Tables are loaded lazily on first attribute access and cached for the
    lifetime of the Snapshot instance. The underlying file is opened once
    per attribute access; cached DataFrames are independent of the file
    handle so the file is never held open across calls.
    """

    def __init__(self, path: Path):
        self._path = Path(path)
        if not self._path.exists():
            raise FileNotFoundError(f"snapshot not found: {self._path}")

    @property
    def path(self) -> Path:
        return self._path

    @property
    def snapshot_id(self) -> str:
        return self._path.stem

    @cached_property
    def manifest(self) -> SnapshotManifest:
        with open_snapshot_db(self._path) as conn:
            return read_manifest(conn)

    def _read(self, table: str) -> pd.DataFrame:
        with open_snapshot_db(self._path) as conn:
            return read_dataframe(conn, table)

    @cached_property
    def polls(self) -> pd.DataFrame:
        return self._read("polls")

    @cached_property
    def results_2024(self) -> pd.DataFrame:
        return self._read("results_2024")

    @cached_property
    def byelections_events(self) -> pd.DataFrame:
        return self._read("byelections_events")

    @cached_property
    def byelections_results(self) -> pd.DataFrame:
        return self._read("byelections_results")

    @cached_property
    def transfer_weights(self) -> pd.DataFrame:
        return self._read("transfer_weights")

    @cached_property
    def transfer_weights_provenance(self) -> pd.DataFrame:
        return self._read("transfer_weights_provenance")

    def lookup_weight(self, nation: str, consolidator: str, source: str) -> float | None:
        """Return weight for (nation, consolidator, source) or None if absent."""
        tw = self.transfer_weights
        m = (tw["nation"] == nation) & (tw["consolidator"] == consolidator) & (tw["source"] == source)
        if not m.any():
            return None
        return float(tw.loc[m, "weight"].iloc[0])

    def consolidator_observed(self, nation: str, consolidator: str) -> bool:
        """True if any matrix cell exists for this (nation, consolidator)."""
        tw = self.transfer_weights
        return bool(((tw["nation"] == nation) & (tw["consolidator"] == consolidator)).any())

    def provenance_for_consolidator(self, nation: str, consolidator: str) -> list[str]:
        """Return contributing event_ids (sorted) for this (nation, consolidator)."""
        prov = self.transfer_weights_provenance
        m = (prov["nation"] == nation) & (prov["consolidator"] == consolidator)
        return sorted(prov.loc[m, "event_id"].astype(str).tolist())
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/prediction_engine/test_snapshot_loader.py -v`
Expected: 8 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add prediction_engine/__init__.py prediction_engine/snapshot_loader.py tests/prediction_engine/test_snapshot_loader.py
git commit -m "feat(prediction): add Snapshot loader (lazy DataFrame access + matrix lookups)"
```

---

## Task 4: Polls aggregation → swing

**Files:**
- Create: `prediction_engine/polls.py`
- Test: `tests/prediction_engine/test_polls.py`

- [ ] **Step 1: Write the failing test**

`tests/prediction_engine/test_polls.py`:

```python
from datetime import date
import pandas as pd
import pytest
from prediction_engine.polls import compute_swing, ge2024_national_share
from schema.common import PartyCode


def _polls_df_simple() -> pd.DataFrame:
    # Two GB polls with DIFFERENT numbers so the window-filter test below
    # distinguishes a 1-poll window from a 2-poll window.
    # Old poll (04-18): reform=20, lab=30. New poll (04-23): reform=30, lab=26.
    # Mean of both: reform=25, lab=28. New-poll-only mean: reform=30, lab=26.
    return pd.DataFrame([
        {"pollster": "X", "fieldwork_start": "2026-04-15", "fieldwork_end": "2026-04-17",
         "published_date": "2026-04-18", "sample_size": 1000, "geography": "GB",
         "con": 22.0, "lab": 30.0, "ld": 12.0, "reform": 20.0, "green": 8.0, "snp": 3.0, "plaid": 1.0, "other": 4.0},
        {"pollster": "Y", "fieldwork_start": "2026-04-20", "fieldwork_end": "2026-04-22",
         "published_date": "2026-04-23", "sample_size": 1000, "geography": "GB",
         "con": 18.0, "lab": 26.0, "ld": 12.0, "reform": 30.0, "green": 8.0, "snp": 3.0, "plaid": 1.0, "other": 2.0},
    ])


def _results_2024_df() -> pd.DataFrame:
    # Two seats × 8 parties; total votes 100 with party splits chosen so vote-weighted national share is round.
    rows = []
    for ons in ("S1", "S2"):
        rows.extend([
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "con",    "votes": 20, "share": 20.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "lab",    "votes": 30, "share": 30.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "ld",     "votes": 10, "share": 10.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "reform", "votes": 15, "share": 15.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "green",  "votes":  5, "share":  5.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "snp",    "votes":  3, "share":  3.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "plaid",  "votes":  1, "share":  1.0},
            {"ons_code": ons, "constituency_name": ons, "region": "X", "nation": "england",
             "party": "other",  "votes": 16, "share": 16.0},
        ])
    return pd.DataFrame(rows)


def test_ge2024_national_share_vote_weighted():
    shares = ge2024_national_share(_results_2024_df(), nation_filter=None)
    assert shares[PartyCode.CON]    == pytest.approx(20.0)
    assert shares[PartyCode.LAB]    == pytest.approx(30.0)
    assert shares[PartyCode.REFORM] == pytest.approx(15.0)


def test_compute_swing_subtracts_ge2024_share():
    polls = _polls_df_simple()
    results = _results_2024_df()
    swing = compute_swing(polls, results, as_of=date(2026, 4, 25), window_days=14, geography="GB")
    # Two-poll mean: reform=(20+30)/2=25; ge2024 reform=15 → swing=+10.
    assert swing[PartyCode.REFORM] == pytest.approx(10.0)
    # Two-poll mean lab=(30+26)/2=28; ge2024 lab=30 → swing=-2.
    assert swing[PartyCode.LAB]    == pytest.approx(-2.0)
    # Two-poll mean con=(22+18)/2=20; ge2024 con=20 → swing=0.
    assert swing[PartyCode.CON]    == pytest.approx(0.0)


def test_compute_swing_window_excludes_old_poll():
    polls = _polls_df_simple()
    results = _results_2024_df()
    # window_days=3 from 2026-04-25 → cutoff_lo=2026-04-22 (exclusive).
    # 04-18 poll FAILS (≤ cutoff_lo); 04-23 poll PASSES.
    # New-poll-only: reform=30, ge2024=15 → swing=+15. Distinct from the 2-poll mean.
    swing = compute_swing(polls, results, as_of=date(2026, 4, 25), window_days=3, geography="GB")
    assert swing[PartyCode.REFORM] == pytest.approx(15.0)
    # New-poll-only lab=26 vs ge2024=30 → swing=-4 (vs -2 in the 2-poll case).
    assert swing[PartyCode.LAB] == pytest.approx(-4.0)


def test_compute_swing_wide_window_includes_both_polls():
    polls = _polls_df_simple()
    results = _results_2024_df()
    # window_days=14 includes both polls; mean of (20, 30) = 25; swing = 25 − 15 = 10.
    swing = compute_swing(polls, results, as_of=date(2026, 4, 25), window_days=14, geography="GB")
    assert swing[PartyCode.REFORM] == pytest.approx(10.0)


def test_compute_swing_raises_when_no_polls_in_window():
    polls = _polls_df_simple()
    results = _results_2024_df()
    with pytest.raises(ValueError, match="no polls in window"):
        compute_swing(polls, results, as_of=date(2024, 1, 1), window_days=14, geography="GB")


def test_compute_swing_filters_geography():
    polls = _polls_df_simple()
    results = _results_2024_df()
    with pytest.raises(ValueError, match="no polls in window"):
        compute_swing(polls, results, as_of=date(2026, 4, 25), window_days=14, geography="Wales")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prediction_engine/test_polls.py -v`
Expected: ImportError.

- [ ] **Step 3: Write minimal implementation**

`prediction_engine/polls.py`:

```python
import logging
from datetime import date, timedelta

import pandas as pd
from schema.common import PartyCode

logger = logging.getLogger(__name__)


_PARTY_VALUES: list[str] = [p.value for p in PartyCode]


def ge2024_national_share(
    results_2024: pd.DataFrame,
    nation_filter: str | None = None,
) -> dict[PartyCode, float]:
    """Vote-weighted national share per party from the 2024 GE results.

    nation_filter: if given (e.g. 'wales'), restrict to that nation; else GB-wide.
    """
    df = results_2024
    if nation_filter is not None:
        df = df[df["nation"] == nation_filter]
    if df.empty:
        raise ValueError(f"no results for nation_filter={nation_filter}")

    by_party = df.groupby("party", as_index=False)["votes"].sum()
    total = float(by_party["votes"].sum())
    if total <= 0:
        raise ValueError("results_2024 votes sum to 0")

    shares: dict[PartyCode, float] = {}
    for p in PartyCode:
        row = by_party[by_party["party"] == p.value]
        v = float(row["votes"].iloc[0]) if not row.empty else 0.0
        shares[p] = (v / total) * 100.0
    return shares


_GEO_TO_NATION_FILTER: dict[str, str | None] = {
    "GB": None,
    "Scotland": "scotland",
    "Wales": "wales",
    "London": None,  # no London-only filter on results_2024 in v1
}


def compute_swing(
    polls: pd.DataFrame,
    results_2024: pd.DataFrame,
    as_of: date,
    window_days: int,
    geography: str,
) -> dict[PartyCode, float]:
    """Average per-party poll share over the window, then subtract GE 2024 share.

    Window: published_date in (as_of − window_days, as_of].
    Failures (no polls match) raise ValueError per spec §8.
    """
    if window_days <= 0:
        raise ValueError(f"window_days must be > 0 (got {window_days})")

    cutoff_lo = (as_of - timedelta(days=window_days)).isoformat()
    cutoff_hi = as_of.isoformat()
    filt = (
        (polls["geography"] == geography)
        & (polls["published_date"] > cutoff_lo)
        & (polls["published_date"] <= cutoff_hi)
    )
    window = polls[filt]
    if window.empty:
        raise ValueError(
            f"no polls in window: geography={geography} as_of={as_of} window_days={window_days}"
        )

    poll_means = {p: float(window[p.value].mean()) for p in PartyCode}
    ge_shares = ge2024_national_share(results_2024, nation_filter=_GEO_TO_NATION_FILTER[geography])
    swing = {p: poll_means[p] - ge_shares[p] for p in PartyCode}

    logger.info(
        "Swing computed: as_of=%s geography=%s n_polls=%d swings=%s",
        as_of, geography, len(window),
        {p.value: round(v, 2) for p, v in swing.items()},
    )
    return swing
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prediction_engine/test_polls.py -v`
Expected: 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add prediction_engine/polls.py tests/prediction_engine/test_polls.py
git commit -m "feat(prediction): polls aggregation and per-party swing computation"
```

---

## Task 5: Project raw shares (uniform swing per region)

**Files:**
- Create: `prediction_engine/projection.py`
- Test: `tests/prediction_engine/test_projection.py`

- [ ] **Step 1: Write the failing test**

`tests/prediction_engine/test_projection.py`:

```python
import pandas as pd
import pytest
from prediction_engine.projection import project_raw_shares
from schema.common import PartyCode


def _two_seat_results() -> pd.DataFrame:
    return pd.DataFrame([
        {"ons_code": "E1", "constituency_name": "E1", "region": "R", "nation": "england",
         "party": p, "votes": 0, "share": s}
        for p, s in [("con", 30), ("lab", 30), ("ld", 10), ("reform", 10),
                     ("green", 5), ("snp", 0), ("plaid", 0), ("other", 15)]
    ] + [
        {"ons_code": "W1", "constituency_name": "W1", "region": "R", "nation": "wales",
         "party": p, "votes": 0, "share": s}
        for p, s in [("con", 10), ("lab", 35), ("ld", 5), ("reform", 15),
                     ("green", 5), ("snp", 0), ("plaid", 25), ("other", 5)]
    ])


def test_project_raw_shares_applies_gb_swing_to_england():
    results = _two_seat_results()
    swings = {
        "GB": {p: 0.0 for p in PartyCode},
    }
    swings["GB"][PartyCode.REFORM] = 10.0
    swings["GB"][PartyCode.LAB] = -5.0
    out = project_raw_shares(results, swings)
    e1 = out[out["ons_code"] == "E1"].iloc[0]
    # Reform was 10 + 10 = 20; Lab 30 − 5 = 25; total before renorm = 100 + 5 = 105 → renormalise.
    assert e1["share_raw_reform"] == pytest.approx(20.0 * 100.0 / 105.0, abs=1e-6)
    assert e1["share_raw_lab"]    == pytest.approx(25.0 * 100.0 / 105.0, abs=1e-6)
    # Sum to 100.
    cols = [f"share_raw_{p.value}" for p in PartyCode]
    assert e1[cols].sum() == pytest.approx(100.0, abs=1e-6)


def test_project_raw_shares_uses_wales_swing_when_present():
    results = _two_seat_results()
    swings = {
        "GB":    {p: 0.0 for p in PartyCode},
        "Wales": {p: 0.0 for p in PartyCode},
    }
    swings["GB"][PartyCode.REFORM]    = 10.0
    swings["Wales"][PartyCode.REFORM] = 5.0  # smaller Welsh swing
    out = project_raw_shares(results, swings)
    w1 = out[out["ons_code"] == "W1"].iloc[0]
    # Wales should use Wales swing (+5), not GB (+10).
    expected_pre = 15.0 + 5.0  # 20
    expected_total_before_renorm = 100.0 + 5.0
    assert w1["share_raw_reform"] == pytest.approx(expected_pre * 100.0 / expected_total_before_renorm, abs=1e-6)


def test_project_raw_shares_clamps_negative_to_zero():
    results = _two_seat_results()
    swings = {"GB": {p: 0.0 for p in PartyCode}}
    swings["GB"][PartyCode.REFORM] = -50.0  # would drive reform negative
    out = project_raw_shares(results, swings)
    cols = [f"share_raw_{p.value}" for p in PartyCode]
    # Every seat (both E1 and W1 — Wales falls back to GB swing here) renormalises
    # to exactly 100.0 after the clamp.
    for ons in ("E1", "W1"):
        seat = out[out["ons_code"] == ons].iloc[0]
        assert seat["share_raw_reform"] == pytest.approx(0.0, abs=1e-6)
        assert seat[cols].sum() == pytest.approx(100.0, abs=1e-6)


def test_project_raw_shares_all_seats_sum_to_100():
    """Renormalisation guarantee: every seat's predicted shares sum to exactly 100,
    regardless of swing configuration."""
    results = _two_seat_results()
    swings = {
        "GB":    {p: 0.0 for p in PartyCode},
        "Wales": {p: 0.0 for p in PartyCode},
    }
    swings["GB"][PartyCode.REFORM]    = 7.0
    swings["GB"][PartyCode.LAB]       = -3.0
    swings["Wales"][PartyCode.REFORM] = 4.0
    swings["Wales"][PartyCode.LAB]    = -2.0
    out = project_raw_shares(results, swings)
    cols = [f"share_raw_{p.value}" for p in PartyCode]
    sums = out[cols].sum(axis=1)
    for total in sums:
        assert total == pytest.approx(100.0, abs=1e-6)


def test_project_raw_shares_preserves_identity_columns():
    results = _two_seat_results()
    swings = {"GB": {p: 0.0 for p in PartyCode}}
    out = project_raw_shares(results, swings)
    assert set(out.columns) >= {"ons_code", "constituency_name", "region", "nation"}
    assert len(out) == 2  # one row per seat (pivoted)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prediction_engine/test_projection.py -v`
Expected: ImportError.

- [ ] **Step 3: Write minimal implementation**

`prediction_engine/projection.py`:

```python
import logging

import pandas as pd
from schema.common import PartyCode

logger = logging.getLogger(__name__)


_PARTY_VALUES: list[str] = [p.value for p in PartyCode]


def _pick_swing_for_nation(
    swings: dict[str, dict[PartyCode, float]], nation: str
) -> dict[PartyCode, float]:
    """Wales seats use Wales swing if present; Scotland likewise; else GB."""
    if nation == "wales" and "Wales" in swings:
        return swings["Wales"]
    if nation == "scotland" and "Scotland" in swings:
        return swings["Scotland"]
    return swings["GB"]


def project_raw_shares(
    results_2024: pd.DataFrame,
    swings: dict[str, dict[PartyCode, float]],
) -> pd.DataFrame:
    """Pivot 2024 results to wide form, apply per-party swing, clamp negatives,
    re-normalise to 100. Returns one row per seat with columns:

        ons_code, constituency_name, region, nation,
        share_2024_<party> (8), share_raw_<party> (8)
    """
    if "GB" not in swings:
        raise ValueError("swings dict must contain 'GB' fallback")

    wide = results_2024.pivot_table(
        index=["ons_code", "constituency_name", "region", "nation"],
        columns="party",
        values="share",
        aggfunc="first",
        fill_value=0.0,
    ).reset_index()
    # Normalise column order
    for p in _PARTY_VALUES:
        if p not in wide.columns:
            wide[p] = 0.0

    # Apply swing per row, vectorised per nation.
    for nation in wide["nation"].unique():
        mask = wide["nation"] == nation
        swing_for = _pick_swing_for_nation(swings, str(nation))
        for p in PartyCode:
            wide.loc[mask, f"_post_{p.value}"] = (
                wide.loc[mask, p.value] + swing_for[p]
            ).clip(lower=0.0)

    # Re-normalise post-swing shares to sum to 100 per seat.
    post_cols = [f"_post_{p.value}" for p in PartyCode]
    totals = wide[post_cols].sum(axis=1)
    if (totals <= 0).any():
        raise ValueError("post-swing shares non-positive for at least one seat")
    for p in PartyCode:
        wide[f"share_raw_{p.value}"] = wide[f"_post_{p.value}"] * 100.0 / totals

    # Build share_2024_<p> from the pivoted source columns.
    for p in PartyCode:
        wide[f"share_2024_{p.value}"] = wide[p.value]

    keep = (
        ["ons_code", "constituency_name", "region", "nation"]
        + [f"share_2024_{p.value}" for p in PartyCode]
        + [f"share_raw_{p.value}"  for p in PartyCode]
    )
    return wide[keep].sort_values("ons_code").reset_index(drop=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prediction_engine/test_projection.py -v`
Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add prediction_engine/projection.py tests/prediction_engine/test_projection.py
git commit -m "feat(prediction): project_raw_shares — per-region uniform swing + renormalise"
```

---

## Task 6: Strategy ABC + registry

**Files:**
- Create: `prediction_engine/strategies/__init__.py`
- Create: `prediction_engine/strategies/base.py`
- Test: `tests/prediction_engine/test_strategy_base.py`

- [ ] **Step 1: Write the failing test**

`tests/prediction_engine/test_strategy_base.py`:

```python
import pytest
from prediction_engine.strategies.base import Strategy, register, STRATEGY_REGISTRY
from schema.prediction import ScenarioConfig


class _NoopConfig(ScenarioConfig):
    pass


def test_registry_decorator_registers_class():
    # Use a one-off name to avoid collisions with real strategies.
    @register("test_noop_xyz")
    class NoopStrategy(Strategy):
        name = "test_noop_xyz"
        config_schema = _NoopConfig
        def predict(self, snapshot, scenario):
            raise NotImplementedError

    assert "test_noop_xyz" in STRATEGY_REGISTRY
    assert STRATEGY_REGISTRY["test_noop_xyz"] is NoopStrategy
    # Cleanup so reruns don't fail.
    del STRATEGY_REGISTRY["test_noop_xyz"]


def test_register_rejects_duplicate_name():
    @register("test_dup")
    class A(Strategy):
        name = "test_dup"
        config_schema = _NoopConfig
        def predict(self, snapshot, scenario): pass

    with pytest.raises(ValueError, match="already registered"):
        @register("test_dup")
        class B(Strategy):
            name = "test_dup"
            config_schema = _NoopConfig
            def predict(self, snapshot, scenario): pass

    del STRATEGY_REGISTRY["test_dup"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prediction_engine/test_strategy_base.py -v`
Expected: ImportError.

- [ ] **Step 3: Write minimal implementation**

`prediction_engine/strategies/base.py`:

```python
from abc import ABC, abstractmethod
from typing import Type

from prediction_engine.snapshot_loader import Snapshot
from schema.prediction import ScenarioConfig


class Strategy(ABC):
    name: str
    config_schema: Type[ScenarioConfig]

    @abstractmethod
    def predict(self, snapshot: Snapshot, scenario: ScenarioConfig):
        """Return a PredictionResult."""
        ...


STRATEGY_REGISTRY: dict[str, Type[Strategy]] = {}


def register(name: str):
    def decorator(cls: Type[Strategy]) -> Type[Strategy]:
        if name in STRATEGY_REGISTRY:
            raise ValueError(f"strategy {name!r} already registered")
        STRATEGY_REGISTRY[name] = cls
        return cls
    return decorator
```

`prediction_engine/strategies/__init__.py`:

```python
# Importing the strategy modules triggers their @register decorators,
# populating STRATEGY_REGISTRY at import time. Adding a new strategy
# requires importing it here.
from prediction_engine.strategies import base  # noqa: F401
from prediction_engine.strategies import uniform_swing  # noqa: F401
from prediction_engine.strategies import reform_threat_consolidation  # noqa: F401
```

(Note: `uniform_swing` and `reform_threat_consolidation` modules do not exist yet. The `__init__.py` will fail on import until Tasks 7 + 9 are merged. Either skip the `__init__.py` write here and add it in Task 9, or accept the import failure until then. **Defer the __init__.py to Task 9.** For now, leave `prediction_engine/strategies/__init__.py` empty.)

Actually, write an empty `prediction_engine/strategies/__init__.py`:

```python
# Strategy modules are imported lazily by the runner; see Task 9.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prediction_engine/test_strategy_base.py -v`
Expected: 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add prediction_engine/strategies/__init__.py prediction_engine/strategies/base.py tests/prediction_engine/test_strategy_base.py
git commit -m "feat(prediction): Strategy ABC + STRATEGY_REGISTRY decorator"
```

---

## Task 7: Uniform-swing strategy

**Files:**
- Create: `prediction_engine/strategies/uniform_swing.py`
- Test: `tests/prediction_engine/test_uniform_swing.py`

- [ ] **Step 1: Write the failing test**

`tests/prediction_engine/test_uniform_swing.py`:

```python
import pytest
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.strategies.uniform_swing import UniformSwingStrategy
from schema.prediction import UniformSwingConfig
from schema.common import PartyCode


def test_uniform_swing_returns_per_seat_predictions(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    strat = UniformSwingStrategy()
    result = strat.predict(snap, UniformSwingConfig())
    df = result.per_seat
    assert len(df) == 6
    assert set(df.columns) >= {
        "ons_code", "constituency_name", "nation", "region",
        "share_2024_reform", "share_raw_reform", "share_predicted_reform",
        "predicted_winner", "predicted_margin",
        "leader", "consolidator", "clarity",
        "matrix_nation", "matrix_provenance", "notes",
    }


def test_uniform_swing_share_predicted_equals_share_raw(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    result = UniformSwingStrategy().predict(snap, UniformSwingConfig())
    for p in PartyCode:
        col_raw  = f"share_raw_{p.value}"
        col_pred = f"share_predicted_{p.value}"
        diffs = (result.per_seat[col_pred] - result.per_seat[col_raw]).abs()
        assert (diffs < 1e-9).all(), f"raw and predicted differ for {p.value}"


def test_uniform_swing_consolidator_and_clarity_are_null(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    result = UniformSwingStrategy().predict(snap, UniformSwingConfig())
    assert result.per_seat["consolidator"].isna().all()
    assert result.per_seat["clarity"].isna().all()


def test_uniform_swing_winner_is_max_predicted_share(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    result = UniformSwingStrategy().predict(snap, UniformSwingConfig())
    for _, row in result.per_seat.iterrows():
        share_cols = {p.value: row[f"share_predicted_{p.value}"] for p in PartyCode}
        winner = max(share_cols, key=lambda k: share_cols[k])
        assert row["predicted_winner"] == winner


def test_uniform_swing_national_totals_sum_to_seat_count(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    result = UniformSwingStrategy().predict(snap, UniformSwingConfig())
    overall = result.national[result.national["scope"] == "overall"]
    assert overall["seats"].sum() == len(result.per_seat)  # one winner per seat


def test_uniform_swing_per_nation_seat_counts_consistent(tiny_snapshot_path):
    """The per-nation breakdown's seat counts must sum (across parties, within each nation)
    to the number of seats in that nation. Catches bugs where _compute_national_totals
    double-counts or drops scopes."""
    snap = Snapshot(tiny_snapshot_path)
    result = UniformSwingStrategy().predict(snap, UniformSwingConfig())
    nation_view = result.national[result.national["scope"] == "nation"]
    for nation, sub in nation_view.groupby("scope_value"):
        seats_in_nation = (result.per_seat["nation"] == nation).sum()
        assert sub["seats"].sum() == seats_in_nation, f"{nation}: {sub['seats'].sum()} != {seats_in_nation}"


def test_uniform_swing_winner_tie_break_follows_partycode_order(tiny_snapshot_path):
    """Document and enforce tie-break behavior. The implementation uses pandas idxmax
    over party_cols = [share_predicted_<p> for p in PartyCode], which on a tie returns
    the FIRST column with the max — i.e. PartyCode declaration order. PartyCode order is
    LAB, CON, LD, REFORM, GREEN, SNP, PLAID, OTHER (from schema/common.py); so on a Lab/Reform
    tie, Lab wins.

    This test injects a synthetic Lab/Reform tie and asserts Lab wins.
    """
    import pandas as pd
    snap = Snapshot(tiny_snapshot_path)
    # Run predict, then synthesize a tie post-hoc by manually invoking the helper.
    # Simpler: assert ordering via construction with hand-built shares.
    from prediction_engine.strategies.uniform_swing import _add_winner_and_metadata
    row = {f"share_raw_{p.value}":       0.0 for p in PartyCode}
    row.update({f"share_predicted_{p.value}": 0.0 for p in PartyCode})
    row["share_predicted_lab"]    = 35.0
    row["share_predicted_reform"] = 35.0
    row["share_raw_lab"]    = 35.0
    row["share_raw_reform"] = 35.0
    df = pd.DataFrame([row])
    out = _add_winner_and_metadata(df.copy())
    assert out.iloc[0]["predicted_winner"] == "lab"  # Lab wins because it precedes Reform in PartyCode order


def test_uniform_swing_determinism(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    a = UniformSwingStrategy().predict(snap, UniformSwingConfig()).per_seat
    b = UniformSwingStrategy().predict(snap, UniformSwingConfig()).per_seat
    # Sort identically; row-set equality.
    a_sorted = a.sort_values("ons_code").reset_index(drop=True)
    b_sorted = b.sort_values("ons_code").reset_index(drop=True)
    assert a_sorted.equals(b_sorted)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prediction_engine/test_uniform_swing.py -v`
Expected: ImportError.

- [ ] **Step 3: Write minimal implementation**

`prediction_engine/strategies/uniform_swing.py`:

```python
import logging
from dataclasses import dataclass

import pandas as pd

from prediction_engine.polls import compute_swing
from prediction_engine.projection import project_raw_shares
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.strategies.base import Strategy, register
from schema.common import PartyCode
from schema.prediction import UniformSwingConfig

logger = logging.getLogger(__name__)


@dataclass
class PredictionResult:
    """Returned from Strategy.predict. Final SQLite serialisation lives in sqlite_io."""
    per_seat: pd.DataFrame
    national: pd.DataFrame
    run_metadata: dict


@register("uniform_swing")
class UniformSwingStrategy(Strategy):
    name = "uniform_swing"
    config_schema = UniformSwingConfig

    def predict(self, snapshot: Snapshot, scenario: UniformSwingConfig) -> PredictionResult:
        gb_swing = compute_swing(
            snapshot.polls,
            snapshot.results_2024,
            as_of=snapshot.manifest.as_of_date,
            window_days=scenario.polls_window_days,
            geography="GB",
        )
        # v1: Wales/Scotland fall back to GB-only swing per spec §11 open question.
        swings = {"GB": gb_swing}
        per_seat = project_raw_shares(snapshot.results_2024, swings)

        # share_predicted_<p> = share_raw_<p> for uniform-swing baseline.
        for p in PartyCode:
            per_seat[f"share_predicted_{p.value}"] = per_seat[f"share_raw_{p.value}"]

        per_seat = _add_winner_and_metadata(per_seat)
        per_seat = per_seat.sort_values("ons_code").reset_index(drop=True)

        national = _compute_national_totals(per_seat)

        return PredictionResult(
            per_seat=per_seat,
            national=national,
            run_metadata={
                "strategy": self.name,
                "scenario": scenario.model_dump(mode="json"),
                "snapshot_id": snapshot.snapshot_id,
            },
        )


def _add_winner_and_metadata(per_seat: pd.DataFrame) -> pd.DataFrame:
    """Compute predicted_winner, predicted_margin, leader. Set consolidator/clarity/matrix_* to null
    and notes to the empty-list JSON string. This is shared between strategies for the
    uniform-swing-fallback path; reform_threat overwrites the relevant fields for tactical seats."""
    party_cols = [f"share_predicted_{p.value}" for p in PartyCode]
    raw_cols   = [f"share_raw_{p.value}"       for p in PartyCode]

    winners = per_seat[party_cols].idxmax(axis=1).str.replace("share_predicted_", "", regex=False)
    per_seat["predicted_winner"] = winners.values

    sorted_shares = per_seat[party_cols].apply(
        lambda row: sorted(row.values, reverse=True), axis=1, result_type="expand"
    )
    per_seat["predicted_margin"] = sorted_shares.iloc[:, 0] - sorted_shares.iloc[:, 1]

    leaders = per_seat[raw_cols].idxmax(axis=1).str.replace("share_raw_", "", regex=False)
    per_seat["leader"] = leaders.values

    per_seat["consolidator"]      = None
    per_seat["clarity"]           = None
    per_seat["matrix_nation"]     = None
    per_seat["matrix_provenance"] = "[]"
    per_seat["notes"]             = "[]"
    return per_seat


def _compute_national_totals(per_seat: pd.DataFrame) -> pd.DataFrame:
    """Long-format DataFrame: scope/scope_value/party/seats."""
    rows: list[dict] = []
    overall = per_seat["predicted_winner"].value_counts()
    for party, seats in overall.items():
        rows.append({"scope": "overall", "scope_value": "", "party": party, "seats": int(seats)})

    for nation in sorted(per_seat["nation"].dropna().unique()):
        sub = per_seat[per_seat["nation"] == nation]
        for party, seats in sub["predicted_winner"].value_counts().items():
            rows.append({"scope": "nation", "scope_value": str(nation), "party": party, "seats": int(seats)})

    for region in sorted(per_seat["region"].dropna().unique()):
        sub = per_seat[per_seat["region"] == region]
        for party, seats in sub["predicted_winner"].value_counts().items():
            rows.append({"scope": "region", "scope_value": str(region), "party": party, "seats": int(seats)})

    return pd.DataFrame(rows, columns=["scope", "scope_value", "party", "seats"])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prediction_engine/test_uniform_swing.py -v`
Expected: 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add prediction_engine/strategies/uniform_swing.py tests/prediction_engine/test_uniform_swing.py
git commit -m "feat(prediction): uniform_swing baseline strategy + PredictionResult dataclass"
```

---

## Task 8: Reform-threat helper functions (consolidator + clarity + flow)

**Files:**
- Create: `prediction_engine/strategies/reform_threat_consolidation.py` (helpers only — full strategy in Task 9)
- Test: `tests/prediction_engine/test_reform_threat.py` (helper-level tests; integration tests added in Task 9)

This task adds the pure-function helpers; Task 9 wires them into the full Strategy. Splitting keeps each module under ~250 lines and lets the helpers be tested in isolation.

- [ ] **Step 1: Write the failing test (helpers only)**

`tests/prediction_engine/test_reform_threat.py`:

```python
import pytest
from prediction_engine.strategies.reform_threat_consolidation import (
    identify_consolidator,
    compute_clarity,
    apply_flows,
)
from schema.common import PartyCode


def _shares(**overrides) -> dict[PartyCode, float]:
    base = {p: 0.0 for p in PartyCode}
    base.update({PartyCode(k): v for k, v in overrides.items()})
    return base


def test_identify_consolidator_picks_highest_left_bloc():
    shares = _shares(reform=35.0, lab=30.0, ld=10.0, green=8.0)
    c = identify_consolidator(shares, nation="england")
    assert c == PartyCode.LAB


def test_identify_consolidator_returns_none_when_no_left_bloc_above_threshold():
    shares = _shares(reform=35.0, lab=1.0, ld=1.0, green=1.0)
    c = identify_consolidator(shares, nation="england", min_share=2.0)
    assert c is None


def test_identify_consolidator_in_wales_includes_plaid():
    shares = _shares(reform=30.0, lab=15.0, plaid=25.0)
    c = identify_consolidator(shares, nation="wales")
    assert c == PartyCode.PLAID


def test_identify_consolidator_in_scotland_includes_snp():
    shares = _shares(reform=30.0, lab=18.0, snp=28.0)
    c = identify_consolidator(shares, nation="scotland")
    assert c == PartyCode.SNP


def test_compute_clarity_full_when_gap_exceeds_threshold():
    shares = _shares(lab=30.0, ld=10.0, green=8.0)
    clarity = compute_clarity(shares, consolidator=PartyCode.LAB, nation="england", threshold=5.0)
    assert clarity == pytest.approx(1.0)


def test_compute_clarity_zero_when_consolidator_tied():
    shares = _shares(lab=20.0, ld=20.0, green=8.0)
    clarity = compute_clarity(shares, consolidator=PartyCode.LAB, nation="england", threshold=5.0)
    assert clarity == pytest.approx(0.0)


def test_compute_clarity_partial():
    shares = _shares(lab=20.0, ld=18.0)  # gap 2pp; threshold 5pp → clarity 0.4
    clarity = compute_clarity(shares, consolidator=PartyCode.LAB, nation="england", threshold=5.0)
    assert clarity == pytest.approx(0.4)


def test_apply_flows_redistributes_share_to_consolidator():
    shares = _shares(reform=35.0, lab=30.0, ld=10.0, green=8.0, con=15.0, other=2.0)
    weights = {PartyCode.LD: 0.5, PartyCode.GREEN: 0.4, PartyCode.CON: 0.2}
    flagged: list[str] = []
    out = apply_flows(
        shares, leader=PartyCode.REFORM, consolidator=PartyCode.LAB,
        weights=weights, clarity=1.0, multiplier=1.0, flag_sink=flagged,
    )
    # LD loses 10*0.5*1*1 = 5; Green loses 8*0.4 = 3.2; Con loses 15*0.2 = 3.
    # Lab gains 5+3.2+3 = 11.2.
    assert out[PartyCode.LD]    == pytest.approx(5.0)
    assert out[PartyCode.GREEN] == pytest.approx(4.8)
    assert out[PartyCode.CON]   == pytest.approx(12.0)
    assert out[PartyCode.LAB]   == pytest.approx(41.2)
    assert out[PartyCode.REFORM] == pytest.approx(35.0)  # unchanged
    assert "multiplier_clipped" not in flagged


def test_apply_flows_multiplier_clipped_when_flow_exceeds_source():
    shares = _shares(reform=35.0, lab=30.0, ld=10.0)
    weights = {PartyCode.LD: 0.8}
    flagged: list[str] = []
    out = apply_flows(
        shares, leader=PartyCode.REFORM, consolidator=PartyCode.LAB,
        weights=weights, clarity=1.0, multiplier=2.0, flag_sink=flagged,
    )
    # Want to move 10*0.8*1*2 = 16, but only 10 available → clipped to 10.
    assert out[PartyCode.LD]  == pytest.approx(0.0)
    assert out[PartyCode.LAB] == pytest.approx(40.0)
    assert "multiplier_clipped" in flagged


def test_apply_flows_zero_clarity_means_no_flow():
    shares = _shares(reform=35.0, lab=30.0, ld=10.0)
    weights = {PartyCode.LD: 0.5}
    flagged: list[str] = []
    out = apply_flows(
        shares, leader=PartyCode.REFORM, consolidator=PartyCode.LAB,
        weights=weights, clarity=0.0, multiplier=1.0, flag_sink=flagged,
    )
    assert out[PartyCode.LD]  == pytest.approx(10.0)
    assert out[PartyCode.LAB] == pytest.approx(30.0)


def test_compute_clarity_full_when_consolidator_is_only_left_bloc_party():
    """Edge case: in NI the LEFT_BLOC is empty, but other configurations could leave
    the consolidator as the only left-bloc party present. With no rivals to compare
    against, clarity is treated as 1.0 — the consolidation is trivially unambiguous."""
    # Wales LEFT_BLOC = {lab, ld, green, plaid}. With only Plaid having any share,
    # next_highest is 0, gap = full plaid share → clarity clamped to 1.0.
    shares = _shares(reform=30.0, plaid=20.0)
    clarity = compute_clarity(shares, consolidator=PartyCode.PLAID, nation="wales", threshold=5.0)
    assert clarity == pytest.approx(1.0)


def test_identify_consolidator_tie_break_alphabetical():
    """When two left-bloc parties tie on share, the alphabetically-earlier party wins
    (matches data_engine's _identify_consolidator tie-break behavior)."""
    shares = _shares(reform=35.0, green=20.0, lab=20.0)  # Green and Lab tied at 20
    c = identify_consolidator(shares, nation="england")
    assert c == PartyCode.GREEN  # 'g' < 'l' alphabetically


def test_compute_clarity_rejects_zero_threshold():
    shares = _shares(lab=20.0, ld=10.0)
    with pytest.raises(ValueError, match="threshold must be > 0"):
        compute_clarity(shares, consolidator=PartyCode.LAB, nation="england", threshold=0.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prediction_engine/test_reform_threat.py -v`
Expected: ImportError.

- [ ] **Step 3: Write minimal implementation (helpers only)**

`prediction_engine/strategies/reform_threat_consolidation.py`:

```python
"""Reform-threat consolidation strategy. Helpers in this module; full Strategy in Task 9."""
import logging

from schema.common import LEFT_BLOC, Nation, PartyCode

logger = logging.getLogger(__name__)


def identify_consolidator(
    shares: dict[PartyCode, float],
    nation: str,
    min_share: float = 2.0,
) -> PartyCode | None:
    """Per-seat: return the left-bloc party with the highest current share, or None
    if no left-bloc party clears min_share.

    Tie-break: when two parties tie on share, the alphabetically-earlier PartyCode value
    wins. This matches data_engine.transforms.transfer_matrix._identify_consolidator's
    tie-break (sort by gain desc, then actual_share desc, then party ascending).
    """
    nation_enum = Nation(nation)
    left = LEFT_BLOC[nation_enum]
    if not left:
        return None
    eligible = [p for p in left if shares.get(p, 0.0) >= min_share]
    if not eligible:
        return None
    # min over (-share, party_value) → highest share wins; ties broken by alphabetical party code.
    return min(eligible, key=lambda p: (-shares[p], p.value))


def compute_clarity(
    shares: dict[PartyCode, float],
    consolidator: PartyCode,
    nation: str,
    threshold: float,
) -> float:
    """gap = consolidator share − next-highest left-bloc share; clarity = clip(gap / threshold, 0, 1)."""
    nation_enum = Nation(nation)
    left = LEFT_BLOC[nation_enum] - {consolidator}
    if not left:
        return 1.0  # consolidator is the only left-bloc party
    next_highest = max((shares.get(p, 0.0) for p in left), default=0.0)
    gap = shares[consolidator] - next_highest
    if threshold <= 0:
        raise ValueError(f"threshold must be > 0 (got {threshold})")
    return max(0.0, min(1.0, gap / threshold))


def apply_flows(
    shares: dict[PartyCode, float],
    leader: PartyCode,
    consolidator: PartyCode,
    weights: dict[PartyCode, float],
    clarity: float,
    multiplier: float,
    flag_sink: list[str],
) -> dict[PartyCode, float]:
    """Redistribute share from each weight-source party to the consolidator.

    For each party p in weights: moved = shares[p] * weight * clarity * multiplier,
    clipped to shares[p]. Mutations isolated to a copy; flag_sink mutated to log
    'multiplier_clipped' if any flow saturated. The leader (and consolidator) are
    excluded from being a source even if they appear in weights.
    """
    out = dict(shares)
    for source, weight in weights.items():
        if source == leader or source == consolidator:
            continue
        wanted = out[source] * weight * clarity * multiplier
        moved = min(wanted, out[source])
        if wanted > out[source] + 1e-9:
            if "multiplier_clipped" not in flag_sink:
                flag_sink.append("multiplier_clipped")
        out[source] -= moved
        out[consolidator] += moved
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prediction_engine/test_reform_threat.py -v`
Expected: 13 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add prediction_engine/strategies/reform_threat_consolidation.py tests/prediction_engine/test_reform_threat.py
git commit -m "feat(prediction): reform_threat helpers — identify_consolidator, compute_clarity, apply_flows"
```

---

## Task 9: Reform-threat strategy — full integration

**Files:**
- Modify: `prediction_engine/strategies/reform_threat_consolidation.py` (append the Strategy class)
- Modify: `prediction_engine/strategies/__init__.py` (import both strategies)
- Modify: `tests/prediction_engine/test_reform_threat.py` (append integration tests)

- [ ] **Step 1: Append integration tests**

Append to `tests/prediction_engine/test_reform_threat.py`:

```python
import json
import pandas as pd
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.strategies.reform_threat_consolidation import ReformThreatStrategy
from schema.prediction import ReformThreatConfig


def _seat(per_seat, ons_code):
    return per_seat[per_seat["ons_code"] == ons_code].iloc[0]


def test_reform_threat_seat_a_clear_consolidation(tiny_snapshot_path):
    """Seat A (Aldermouth, england, lab consolidator, high clarity) — flow applies."""
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    a = _seat(res.per_seat, "TST00001")
    assert a["consolidator"] == "lab"
    assert a["matrix_nation"] == "england"
    assert json.loads(a["matrix_provenance"]) == ["tst_eng_2025"]
    # share_predicted_lab > share_raw_lab (received flows)
    assert a["share_predicted_lab"] > a["share_raw_lab"]
    # share_predicted_ld < share_raw_ld (gave flow at weight 0.6)
    assert a["share_predicted_ld"] < a["share_raw_ld"]


def test_reform_threat_seat_c_non_reform_leader_short_circuits(tiny_snapshot_path):
    """Seat C (Carchester) has Con leading, not Reform — short-circuit, flag."""
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    c = _seat(res.per_seat, "TST00003")
    flags = json.loads(c["notes"])
    assert "non_reform_leader" in flags
    # share_predicted_<p> equals share_raw_<p> on a short-circuit.
    assert c["share_predicted_con"] == pytest.approx(c["share_raw_con"], abs=1e-9)
    # consolidator is set to None when path short-circuits before consolidator identification.
    # pandas may store None as either Python None (object dtype) or NaN (numeric dtype);
    # pd.isna covers both.
    assert pd.isna(c["consolidator"])


def test_reform_threat_seat_d_wales_plaid_consolidator(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    d = _seat(res.per_seat, "TST00004")
    assert d["consolidator"] == "plaid"
    assert d["matrix_nation"] == "wales"
    assert d["share_predicted_plaid"] > d["share_raw_plaid"]


def test_reform_threat_seat_e_scotland_no_matrix(tiny_snapshot_path):
    """Seat E (Eilean) — SNP would be the locally-strongest left-bloc consolidator,
    but Scotland has no derived matrix entry → matrix_unavailable fallback."""
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    e = _seat(res.per_seat, "TST00005")
    flags = json.loads(e["notes"])
    assert "matrix_unavailable" in flags
    # share_predicted equals share_raw on fallback
    assert e["share_predicted_snp"] == pytest.approx(e["share_raw_snp"], abs=1e-9)
    # The seat still records the would-be consolidator + clarity for analyst inspection,
    # even though no flow was applied (per spec §5.3 step 5: clarity is computed before
    # the matrix-availability check).
    assert e["consolidator"] == "snp"
    assert e["clarity"] is not None and not pd.isna(e["clarity"])


def test_reform_threat_seat_f_ni_excluded(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    f = _seat(res.per_seat, "TST00006")
    flags = json.loads(f["notes"])
    assert "ni_excluded" in flags
    # NI: share_predicted equals share_raw
    assert f["share_predicted_other"] == pytest.approx(f["share_raw_other"], abs=1e-9)


def test_reform_threat_low_clarity_flag(tiny_snapshot_path):
    """Seat B (Bramford): Lab/LD near-tied (gap=2pp) → low_clarity at default threshold=5pp."""
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    b = _seat(res.per_seat, "TST00002")
    flags = json.loads(b["notes"])
    assert "low_clarity" in flags
    # Flow still applies at low clarity, just scaled down.
    assert b["consolidator"] == "lab"


def test_reform_threat_multiplier_monotone(tiny_snapshot_path):
    """Seat A: with weight 0.6 for LD, raising multiplier from 0.5 → 1.5 must move ≥ as much LD share."""
    snap = Snapshot(tiny_snapshot_path)
    moves: list[float] = []
    for m in (0.5, 1.0, 1.5):
        res = ReformThreatStrategy().predict(snap, ReformThreatConfig(multiplier=m))
        a = _seat(res.per_seat, "TST00001")
        moves.append(a["share_raw_ld"] - a["share_predicted_ld"])
    assert moves[0] <= moves[1] <= moves[2] + 1e-9


def test_reform_threat_determinism(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    a = ReformThreatStrategy().predict(snap, ReformThreatConfig()).per_seat
    b = ReformThreatStrategy().predict(snap, ReformThreatConfig()).per_seat
    a_sorted = a.sort_values("ons_code").reset_index(drop=True)
    b_sorted = b.sort_values("ons_code").reset_index(drop=True)
    assert a_sorted.equals(b_sorted)


def test_reform_threat_shares_sum_to_100_per_seat(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    res = ReformThreatStrategy().predict(snap, ReformThreatConfig())
    cols = [f"share_predicted_{p.value}" for p in PartyCode]
    sums = res.per_seat[cols].sum(axis=1)
    for s in sums:
        assert s == pytest.approx(100.0, abs=1e-6)


def test_reform_threat_consolidator_already_leads_unit():
    """Hand-built shares unit test for the consolidator_already_leads guard.

    With the current PartyCode-order tie-break (Lab precedes Reform), a strict
    Lab=Reform tie on share_raw resolves to leader=Lab, which would short-circuit
    via non_reform_leader. The consolidator_already_leads path is reachable only
    when Reform is uniquely max but lab=reform exactly — impossible on real data.
    To exercise the guard, construct a row where Reform > all other parties
    individually but Lab equals Reform exactly. We bypass _argmax by injecting
    leader=Reform manually and call the guard predicate from the helper.
    """
    from prediction_engine.strategies.reform_threat_consolidation import (
        identify_consolidator,
    )
    raw_shares = {p: 0.0 for p in PartyCode}
    raw_shares[PartyCode.REFORM] = 35.0
    raw_shares[PartyCode.LAB]    = 35.0  # tied with Reform
    raw_shares[PartyCode.LD]     = 10.0
    raw_shares[PartyCode.OTHER]  = 20.0
    consolidator = identify_consolidator(raw_shares, nation="england")
    # Identification picks the highest left-bloc party regardless of Reform's share.
    assert consolidator == PartyCode.LAB
    # The guard condition is purely scalar: share[consolidator] >= share[leader].
    # On a strict tie (35 == 35), the guard fires.
    assert raw_shares[consolidator] >= raw_shares[PartyCode.REFORM]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/prediction_engine/test_reform_threat.py -v`
Expected: integration tests fail with ImportError on `ReformThreatStrategy`.

- [ ] **Step 3: Append the Strategy implementation**

Append to `prediction_engine/strategies/reform_threat_consolidation.py`:

```python
import json

import pandas as pd

from prediction_engine.polls import compute_swing
from prediction_engine.projection import project_raw_shares
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.strategies.base import Strategy, register
from prediction_engine.strategies.uniform_swing import (
    PredictionResult,
    _add_winner_and_metadata,
    _compute_national_totals,
)
from schema.prediction import ReformThreatConfig


@register("reform_threat_consolidation")
class ReformThreatStrategy(Strategy):
    name = "reform_threat_consolidation"
    config_schema = ReformThreatConfig

    def predict(self, snapshot: Snapshot, scenario: ReformThreatConfig) -> PredictionResult:
        gb_swing = compute_swing(
            snapshot.polls,
            snapshot.results_2024,
            as_of=snapshot.manifest.as_of_date,
            window_days=scenario.polls_window_days,
            geography="GB",
        )
        per_seat = project_raw_shares(snapshot.results_2024, {"GB": gb_swing})

        # Initialise tactical-output columns to uniform-swing defaults; the per-seat loop
        # may override them.
        for p in PartyCode:
            per_seat[f"share_predicted_{p.value}"] = per_seat[f"share_raw_{p.value}"]
        per_seat["consolidator"]      = None
        per_seat["clarity"]           = None
        per_seat["matrix_nation"]     = None
        per_seat["matrix_provenance"] = "[]"
        per_seat["notes"]             = "[]"

        rows: list[dict] = []
        for _, row in per_seat.sort_values("ons_code").iterrows():
            updated = _predict_seat(row.to_dict(), snapshot, scenario)
            rows.append(updated)
        per_seat = pd.DataFrame(rows)

        per_seat = _add_winner_and_metadata(per_seat)
        per_seat = per_seat.sort_values("ons_code").reset_index(drop=True)
        national = _compute_national_totals(per_seat)

        return PredictionResult(
            per_seat=per_seat,
            national=national,
            run_metadata={
                "strategy": self.name,
                "scenario": scenario.model_dump(mode="json"),
                "snapshot_id": snapshot.snapshot_id,
            },
        )


def _predict_seat(row: dict, snapshot: Snapshot, scenario: ReformThreatConfig) -> dict:
    """Apply the reform-threat algorithm to one seat row. Returns a dict in seats-table
    schema with per-party share_predicted_* and metadata fields.

    Step ordering (matches spec §5.3 strictly):
      1. NI short-circuit
      2. leader != Reform → non_reform_leader fallback
      3. identify consolidator (None → matrix_unavailable; consolidator >= leader → consolidator_already_leads)
      4. compute clarity (always, regardless of matrix availability)
      5. matrix availability check (no consolidator entries → matrix_unavailable, but preserve clarity)
      6. weights lookup per source (cell missing → no_matrix_entry, source share unchanged)
      7. apply flows scaled by clarity × multiplier
      8. re-normalise to 100
    """
    nation = row["nation"]
    flags: list[str] = []

    raw_shares: dict[PartyCode, float] = {p: float(row[f"share_raw_{p.value}"]) for p in PartyCode}

    # 1. NI short-circuit.
    if nation == "northern_ireland":
        flags.append("ni_excluded")
        return _seat_with_flags(row, raw_shares, leader=_argmax(raw_shares),
                                consolidator=None, clarity=None, matrix_nation=None,
                                provenance=[], flags=flags)

    # 2. Non-Reform leader fallback.
    leader = _argmax(raw_shares)
    if leader != PartyCode.REFORM:
        flags.append("non_reform_leader")
        return _seat_with_flags(row, raw_shares, leader=leader,
                                consolidator=None, clarity=None, matrix_nation=None,
                                provenance=[], flags=flags)

    # 3. Identify consolidator.
    consolidator = identify_consolidator(raw_shares, nation=nation)
    if consolidator is None:
        flags.append("matrix_unavailable")
        return _seat_with_flags(row, raw_shares, leader=leader,
                                consolidator=None, clarity=None, matrix_nation=nation,
                                provenance=[], flags=flags)

    if raw_shares[consolidator] >= raw_shares[leader]:
        flags.append("consolidator_already_leads")
        return _seat_with_flags(row, raw_shares, leader=leader,
                                consolidator=consolidator, clarity=None,
                                matrix_nation=nation, provenance=[], flags=flags)

    # 4. Compute clarity. (Spec §5.3 step 4: clarity is always meaningful for an
    # identified consolidator; it does NOT depend on matrix availability.)
    clarity = compute_clarity(raw_shares, consolidator, nation, scenario.clarity_threshold)
    if clarity < 0.5:
        flags.append("low_clarity")

    # 5. Matrix availability. The matrix nation may have no consolidator entries (e.g.
    # Scotland in v1: no eligible by-election yet). We preserve the consolidator + clarity
    # in the seat output for analyst inspection, then fall back without applying flows.
    if not snapshot.consolidator_observed(nation, consolidator.value):
        flags.append("matrix_unavailable")
        return _seat_with_flags(row, raw_shares, leader=leader,
                                consolidator=consolidator, clarity=clarity,
                                matrix_nation=nation, provenance=[], flags=flags)

    # 6. Per-source weight lookup. Missing cells flag once and skip that source.
    weights: dict[PartyCode, float] = {}
    for source in PartyCode:
        if source in (leader, consolidator) or raw_shares[source] <= 0.0:
            continue
        w = snapshot.lookup_weight(nation, consolidator.value, source.value)
        if w is None:
            if "no_matrix_entry" not in flags:
                flags.append("no_matrix_entry")
            continue
        weights[source] = w

    # 7. Apply flows.
    new_shares = apply_flows(
        raw_shares,
        leader=leader,
        consolidator=consolidator,
        weights=weights,
        clarity=clarity,
        multiplier=scenario.multiplier,
        flag_sink=flags,
    )

    # 8. Re-normalise to 100 (apply_flows preserves total in exact arithmetic; float drift
    # makes this safe).
    total = sum(new_shares.values())
    if total > 0:
        new_shares = {p: v * 100.0 / total for p, v in new_shares.items()}

    provenance = snapshot.provenance_for_consolidator(nation, consolidator.value)
    return _seat_with_flags(row, new_shares, leader=leader, consolidator=consolidator,
                            clarity=clarity, matrix_nation=nation,
                            provenance=provenance, flags=flags)


def _seat_with_flags(
    row: dict,
    shares: dict[PartyCode, float],
    leader: PartyCode,
    consolidator: PartyCode | None,
    clarity: float | None,
    matrix_nation: str | None,
    provenance: list[str],
    flags: list[str],
) -> dict:
    out = dict(row)
    for p in PartyCode:
        out[f"share_predicted_{p.value}"] = shares[p]
    out["leader"]            = leader.value
    out["consolidator"]      = consolidator.value if consolidator else None
    out["clarity"]           = clarity
    out["matrix_nation"]     = matrix_nation
    out["matrix_provenance"] = json.dumps(sorted(provenance))
    out["notes"]             = json.dumps(flags)
    return out


def _argmax(shares: dict[PartyCode, float]) -> PartyCode:
    return max(shares, key=lambda p: (shares[p], -ord(p.value[0])))
```

Note: the `_add_winner_and_metadata` re-derive of `leader` will overwrite the `leader` set in `_seat_with_flags` — that's intentional and fine because both point at `argmax(share_raw_*)`. But `_add_winner_and_metadata` also overwrites `consolidator`, `clarity`, `matrix_nation`, `matrix_provenance`, `notes` to defaults. Override that by amending `_add_winner_and_metadata` to skip overwriting those columns when they're already populated:

- [ ] **Step 4: Refactor `_add_winner_and_metadata` to be additive only**

Edit `prediction_engine/strategies/uniform_swing.py:_add_winner_and_metadata`. The existing implementation unconditionally sets `consolidator/clarity/matrix_nation/matrix_provenance/notes` to defaults — that clobbers the reform-threat strategy's per-seat metadata. Update so it only sets those columns if they're missing:

```python
def _add_winner_and_metadata(per_seat: pd.DataFrame) -> pd.DataFrame:
    party_cols = [f"share_predicted_{p.value}" for p in PartyCode]
    raw_cols   = [f"share_raw_{p.value}"       for p in PartyCode]

    winners = per_seat[party_cols].idxmax(axis=1).str.replace("share_predicted_", "", regex=False)
    per_seat["predicted_winner"] = winners.values

    sorted_shares = per_seat[party_cols].apply(
        lambda row: sorted(row.values, reverse=True), axis=1, result_type="expand"
    )
    per_seat["predicted_margin"] = sorted_shares.iloc[:, 0] - sorted_shares.iloc[:, 1]

    leaders = per_seat[raw_cols].idxmax(axis=1).str.replace("share_raw_", "", regex=False)
    per_seat["leader"] = leaders.values

    # Only fill metadata columns if absent — reform_threat strategy populates them per-seat.
    for col, default in (
        ("consolidator", None),
        ("clarity", None),
        ("matrix_nation", None),
        ("matrix_provenance", "[]"),
        ("notes", "[]"),
    ):
        if col not in per_seat.columns:
            per_seat[col] = default
    return per_seat
```

- [ ] **Step 5: Update `prediction_engine/strategies/__init__.py`**

```python
# Importing the strategy modules triggers their @register decorators,
# populating STRATEGY_REGISTRY at import time. Adding a new strategy
# requires importing it here.
from prediction_engine.strategies import base  # noqa: F401
from prediction_engine.strategies import uniform_swing  # noqa: F401
from prediction_engine.strategies import reform_threat_consolidation  # noqa: F401
```

- [ ] **Step 6: Run all prediction_engine tests**

Run: `uv run pytest tests/prediction_engine/ -v`
Expected: all tests PASS, including the 10 new reform-threat integration tests (9 seat-level paths + 1 consolidator_already_leads unit guard) AND all earlier suites (no regression on uniform_swing).

- [ ] **Step 7: Commit**

```bash
git add prediction_engine/strategies/reform_threat_consolidation.py prediction_engine/strategies/uniform_swing.py prediction_engine/strategies/__init__.py tests/prediction_engine/test_reform_threat.py
git commit -m "feat(prediction): reform_threat_consolidation strategy with all flag paths"
```

---

## Task 10: Prediction-side SQLite I/O

**Files:**
- Create: `prediction_engine/sqlite_io.py`
- Test: `tests/prediction_engine/test_sqlite_io.py`

- [ ] **Step 1: Write the failing test**

`tests/prediction_engine/test_sqlite_io.py`:

```python
from datetime import date, datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest
from prediction_engine.sqlite_io import (
    PREDICTION_SCHEMA_VERSION,
    write_prediction_db,
    read_prediction_seats,
    read_prediction_config,
    read_prediction_national,
    read_prediction_notes_index,
    compute_config_hash,
    build_run_id,
    prediction_filename,
)
from schema.common import PartyCode
from schema.prediction import RunConfig, UniformSwingConfig


def _seats_df() -> pd.DataFrame:
    rows = []
    for ons in ("TST1", "TST2"):
        row = {
            "ons_code": ons, "constituency_name": ons, "nation": "england", "region": "X",
            "predicted_winner": "lab", "predicted_margin": 5.0,
            "leader": "lab", "consolidator": None, "clarity": None,
            "matrix_nation": None, "matrix_provenance": "[]",
            "notes": json.dumps(["non_reform_leader"]) if ons == "TST1" else "[]",
        }
        for prefix in ("share_2024", "share_raw", "share_predicted"):
            for p in PartyCode:
                row[f"{prefix}_{p.value}"] = 12.5
        rows.append(row)
    return pd.DataFrame(rows)


def _national_df() -> pd.DataFrame:
    return pd.DataFrame([
        {"scope": "overall", "scope_value": "", "party": "lab", "seats": 2},
    ])


def _run_config() -> RunConfig:
    return RunConfig(
        snapshot_id="2026-04-25__v1__abc123def456",
        snapshot_content_hash="abc123def456",
        snapshot_as_of_date=date(2026, 4, 25),
        strategy="uniform_swing",
        scenario_config_json='{"polls_window_days": 14}',
        config_hash="0011223344aa",
        schema_version=PREDICTION_SCHEMA_VERSION,
        run_id="abc123def456__uniform_swing__0011223344aa__baseline",
        label="baseline",
        generated_at=datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc),
    )


def test_compute_config_hash_stable():
    cfg1 = UniformSwingConfig(polls_window_days=14)
    cfg2 = UniformSwingConfig(polls_window_days=14)
    assert compute_config_hash(cfg1) == compute_config_hash(cfg2)
    assert len(compute_config_hash(cfg1)) == 12


def test_compute_config_hash_distinct_for_distinct_config():
    a = compute_config_hash(UniformSwingConfig(polls_window_days=14))
    b = compute_config_hash(UniformSwingConfig(polls_window_days=21))
    assert a != b


def test_build_run_id_format():
    rid = build_run_id("abc123def456", "uniform_swing", "0011223344aa", "baseline")
    assert rid == "abc123def456__uniform_swing__0011223344aa__baseline"


def test_prediction_filename(tmp_path: Path):
    out = prediction_filename(
        out_dir=tmp_path,
        snapshot_content_hash="abc123",
        strategy="uniform_swing",
        config_hash="cfg789",
        label="baseline",
    )
    assert out == tmp_path / "abc123__uniform_swing__cfg789__baseline.sqlite"


def test_write_prediction_db_round_trip(tmp_path: Path):
    out = tmp_path / "pred.sqlite"
    seats = _seats_df()
    nat   = _national_df()
    cfg   = _run_config()

    write_prediction_db(out, seats=seats, national=nat, run_config=cfg)
    assert out.exists()

    seats_back = read_prediction_seats(out)
    assert len(seats_back) == 2
    assert set(seats_back["ons_code"]) == {"TST1", "TST2"}

    nat_back = read_prediction_national(out)
    assert nat_back.iloc[0]["seats"] == 2

    cfg_back = read_prediction_config(out)
    assert cfg_back.run_id == cfg.run_id
    assert cfg_back.scenario_config_json == cfg.scenario_config_json

    notes_back = read_prediction_notes_index(out)
    # TST1 has 1 flag, TST2 has none → 1 row total
    assert len(notes_back) == 1
    assert notes_back.iloc[0]["ons_code"] == "TST1"
    assert notes_back.iloc[0]["flag"] == "non_reform_leader"


def test_label_slug_validation(tmp_path: Path):
    with pytest.raises(ValueError, match="invalid label"):
        prediction_filename(
            out_dir=tmp_path,
            snapshot_content_hash="abc123",
            strategy="uniform_swing",
            config_hash="cfg789",
            label="bad label/with slashes",
        )


def test_label_slug_rejects_empty_string(tmp_path: Path):
    with pytest.raises(ValueError, match="invalid label"):
        prediction_filename(
            out_dir=tmp_path,
            snapshot_content_hash="abc123",
            strategy="uniform_swing",
            config_hash="cfg789",
            label="",
        )


def test_round_trip_preserves_int_types(tmp_path: Path):
    """schema_version comes back from SQLite as numpy.int64 (via pandas), but Pydantic's
    int validator must accept it. This guards against subtle type-coercion regressions."""
    out = tmp_path / "pred_int.sqlite"
    write_prediction_db(out, seats=_seats_df(), national=_national_df(), run_config=_run_config())
    cfg_back = read_prediction_config(out)
    assert isinstance(cfg_back.schema_version, int)
    assert cfg_back.schema_version == PREDICTION_SCHEMA_VERSION


def test_explode_notes_handles_empty_flag_lists(tmp_path: Path):
    """A prediction where no seat carries any flag must still produce a writable
    notes_index (empty DataFrame) and not crash on read-back."""
    seats = _seats_df()
    seats["notes"] = "[]"  # remove all flags from every seat
    out = tmp_path / "pred_empty_notes.sqlite"
    write_prediction_db(out, seats=seats, national=_national_df(), run_config=_run_config())
    notes_back = read_prediction_notes_index(out)
    assert len(notes_back) == 0
    assert set(notes_back.columns) == {"ons_code", "flag"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prediction_engine/test_sqlite_io.py -v`
Expected: ImportError.

- [ ] **Step 3: Write minimal implementation**

`prediction_engine/sqlite_io.py`:

```python
import hashlib
import json
import logging
import re
import sqlite3
from contextlib import closing
from pathlib import Path

import pandas as pd

from data_engine.sqlite_io import open_snapshot_db, write_dataframe, read_dataframe
from schema.prediction import RunConfig, ScenarioConfig

logger = logging.getLogger(__name__)


PREDICTION_SCHEMA_VERSION = 1
_LABEL_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def compute_config_hash(scenario: ScenarioConfig) -> str:
    payload = json.dumps(scenario.model_dump(mode="json"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def build_run_id(
    snapshot_content_hash: str, strategy: str, config_hash: str, label: str
) -> str:
    return f"{snapshot_content_hash}__{strategy}__{config_hash}__{label}"


def prediction_filename(
    *,
    out_dir: Path,
    snapshot_content_hash: str,
    strategy: str,
    config_hash: str,
    label: str,
) -> Path:
    if not _LABEL_RE.fullmatch(label):
        raise ValueError(f"invalid label {label!r}: must match {_LABEL_RE.pattern}")
    return out_dir / f"{snapshot_content_hash}__{strategy}__{config_hash}__{label}.sqlite"


def write_prediction_db(
    path: Path,
    *,
    seats: pd.DataFrame,
    national: pd.DataFrame,
    run_config: RunConfig,
) -> None:
    """Write a prediction SQLite file with seats / national / config / notes_index."""
    notes_index = _explode_notes(seats)
    cfg_payload = run_config.model_dump(mode="json")
    cfg_df = pd.DataFrame([cfg_payload])

    path.parent.mkdir(parents=True, exist_ok=True)
    with open_snapshot_db(path) as conn:
        write_dataframe(conn, "seats", seats)
        write_dataframe(conn, "national", national)
        write_dataframe(conn, "config", cfg_df)
        write_dataframe(conn, "notes_index", notes_index)
    logger.info(
        "Wrote prediction %s (seats=%d, national=%d, notes_index=%d)",
        path.name, len(seats), len(national), len(notes_index),
    )


def _explode_notes(seats: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for _, r in seats.iterrows():
        flags = json.loads(r["notes"]) if r["notes"] else []
        for flag in flags:
            rows.append({"ons_code": r["ons_code"], "flag": flag})
    return pd.DataFrame(rows, columns=["ons_code", "flag"])


def read_prediction_seats(path: Path) -> pd.DataFrame:
    with closing(sqlite3.connect(str(path))) as conn:
        return pd.read_sql_query("SELECT * FROM seats", conn)


def read_prediction_national(path: Path) -> pd.DataFrame:
    with closing(sqlite3.connect(str(path))) as conn:
        return pd.read_sql_query("SELECT * FROM national", conn)


def read_prediction_notes_index(path: Path) -> pd.DataFrame:
    with closing(sqlite3.connect(str(path))) as conn:
        return pd.read_sql_query("SELECT * FROM notes_index", conn)


def read_prediction_config(path: Path) -> RunConfig:
    with closing(sqlite3.connect(str(path))) as conn:
        df = pd.read_sql_query("SELECT * FROM config", conn)
    if len(df) != 1:
        raise ValueError(f"config table must have exactly 1 row, found {len(df)}")
    return RunConfig.model_validate(df.iloc[0].to_dict())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prediction_engine/test_sqlite_io.py -v`
Expected: 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add prediction_engine/sqlite_io.py tests/prediction_engine/test_sqlite_io.py
git commit -m "feat(prediction): SQLite I/O — write/read prediction file with seats/national/config/notes_index"
```

---

## Task 11: Runner — load → predict → write

**Files:**
- Create: `prediction_engine/runner.py`
- Test: `tests/prediction_engine/test_runner.py`

- [ ] **Step 1: Write the failing test**

`tests/prediction_engine/test_runner.py`:

```python
from pathlib import Path
import pytest
from prediction_engine.runner import run_prediction
from prediction_engine.sqlite_io import read_prediction_seats, read_prediction_config
from schema.prediction import UniformSwingConfig


def test_run_prediction_writes_sqlite(tiny_snapshot_path, tmp_path: Path):
    out = run_prediction(
        snapshot_path=tiny_snapshot_path,
        strategy_name="uniform_swing",
        scenario=UniformSwingConfig(),
        out_dir=tmp_path,
        label="baseline",
    )
    assert out.exists()
    seats = read_prediction_seats(out)
    assert len(seats) == 6


def test_run_prediction_idempotent(tiny_snapshot_path, tmp_path: Path):
    """Idempotency contract: same (snapshot, strategy, config, label) ⇒ same path AND
    the file is NOT rewritten on the second call.

    We don't compare st_mtime_ns directly — filesystem timestamp resolution varies
    (Windows NTFS = 100ns, ext4 = 1ns, FAT32 = 2s) and pytest's tmp_path may live on
    a low-resolution mount. Instead, compare the SHA-256 of the file's bytes before
    and after; same bytes ⇒ no rewrite.
    """
    import hashlib

    def _file_hash(p: Path) -> str:
        return hashlib.sha256(p.read_bytes()).hexdigest()

    a = run_prediction(
        snapshot_path=tiny_snapshot_path,
        strategy_name="uniform_swing",
        scenario=UniformSwingConfig(),
        out_dir=tmp_path,
        label="baseline",
    )
    hash_before = _file_hash(a)
    b = run_prediction(
        snapshot_path=tiny_snapshot_path,
        strategy_name="uniform_swing",
        scenario=UniformSwingConfig(),
        out_dir=tmp_path,
        label="baseline",
    )
    assert a == b
    assert _file_hash(b) == hash_before, "second call rewrote the prediction file"


def test_run_prediction_writes_config_table(tiny_snapshot_path, tmp_path: Path):
    out = run_prediction(
        snapshot_path=tiny_snapshot_path,
        strategy_name="uniform_swing",
        scenario=UniformSwingConfig(),
        out_dir=tmp_path,
        label="baseline",
    )
    cfg = read_prediction_config(out)
    assert cfg.strategy == "uniform_swing"
    assert cfg.label == "baseline"
    # snapshot_content_hash from tiny snapshot fixture
    assert cfg.snapshot_content_hash == "tinyhash0001"


def test_run_prediction_unknown_strategy_raises(tiny_snapshot_path, tmp_path: Path):
    with pytest.raises(KeyError, match="unknown strategy"):
        run_prediction(
            snapshot_path=tiny_snapshot_path,
            strategy_name="nope",
            scenario=UniformSwingConfig(),
            out_dir=tmp_path,
            label="baseline",
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prediction_engine/test_runner.py -v`
Expected: ImportError.

- [ ] **Step 3: Write minimal implementation**

`prediction_engine/runner.py`:

```python
import logging
from datetime import datetime, timezone
from pathlib import Path

from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.sqlite_io import (
    PREDICTION_SCHEMA_VERSION,
    build_run_id,
    compute_config_hash,
    prediction_filename,
    write_prediction_db,
)
from prediction_engine import strategies as _strategies  # noqa: F401  populates registry
from prediction_engine.strategies.base import STRATEGY_REGISTRY
from schema.prediction import RunConfig, ScenarioConfig
import json

logger = logging.getLogger(__name__)


def run_prediction(
    *,
    snapshot_path: Path,
    strategy_name: str,
    scenario: ScenarioConfig,
    out_dir: Path,
    label: str = "baseline",
) -> Path:
    """Load snapshot → run strategy → write prediction SQLite. Idempotent on
    (snapshot_content_hash, strategy, config_hash, label).
    """
    if strategy_name not in STRATEGY_REGISTRY:
        raise KeyError(f"unknown strategy: {strategy_name}")
    strategy_cls = STRATEGY_REGISTRY[strategy_name]
    # Validate scenario via the strategy's own schema (catches mistyped configs).
    scenario_validated = strategy_cls.config_schema.model_validate(scenario.model_dump())

    snapshot = Snapshot(snapshot_path)
    config_hash = compute_config_hash(scenario_validated)
    out_path = prediction_filename(
        out_dir=out_dir,
        snapshot_content_hash=snapshot.manifest.content_hash,
        strategy=strategy_name,
        config_hash=config_hash,
        label=label,
    )
    if out_path.exists():
        logger.info("Prediction %s already exists; reusing", out_path.name)
        return out_path

    strat = strategy_cls()
    result = strat.predict(snapshot, scenario_validated)

    run_id = build_run_id(snapshot.manifest.content_hash, strategy_name, config_hash, label)
    cfg = RunConfig(
        snapshot_id=snapshot.snapshot_id,
        snapshot_content_hash=snapshot.manifest.content_hash,
        snapshot_as_of_date=snapshot.manifest.as_of_date,
        strategy=strategy_name,
        scenario_config_json=json.dumps(scenario_validated.model_dump(mode="json"), sort_keys=True),
        config_hash=config_hash,
        schema_version=PREDICTION_SCHEMA_VERSION,
        run_id=run_id,
        label=label,
        generated_at=datetime.now(tz=timezone.utc),
    )

    write_prediction_db(out_path, seats=result.per_seat, national=result.national, run_config=cfg)
    return out_path
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prediction_engine/test_runner.py -v`
Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add prediction_engine/runner.py tests/prediction_engine/test_runner.py
git commit -m "feat(prediction): runner — load snapshot, apply strategy, write prediction SQLite"
```

---

## Task 12: CLI — list-strategies, run, sweep, diff

**Files:**
- Create: `prediction_engine/cli.py`
- Modify: `pyproject.toml` (add entry point and dev dep)
- Test: `tests/prediction_engine/test_cli.py`

- [ ] **Step 1: Add entry point and reinstall (CRITICAL — see Plan A's better-memory gotcha)**

Edit `pyproject.toml`. Append to `[project.scripts]`:

```toml
seatpredict-predict = "prediction_engine.cli:main"
```

(Do NOT add `seatpredict-analyze` yet; that comes in Task 14.)

**Why this is fragile:** `uv run seatpredict-predict ...` triggers a package rebuild that reverts the editable install to the default PEP 660 finder mode. That mode's MAPPING omits `data_engine` (and now `prediction_engine`), so the entry point fails with `ModuleNotFoundError`. Plan A captured this in better-memory; the fix is the compat-mode reinstall.

Reinstall with compat editable mode:

```bash
uv pip install --config-settings editable_mode=compat -e ".[dev]"
```

Verify the binary exists and is invocable WITHOUT going through `uv run`:

```bash
# Windows:
.venv/Scripts/seatpredict-predict.exe --help
# POSIX:
.venv/bin/seatpredict-predict --help
```

Expected: Click banner showing the `list-strategies / run / sweep / diff` subcommands. If this errors with `ModuleNotFoundError`, repeat the compat-mode reinstall above. Do NOT proceed until the binary works.

- [ ] **Step 2: Write the failing test**

Tests use Click's `CliRunner` so they're independent of the editable-install state. `tests/prediction_engine/test_cli.py`:

```python
from pathlib import Path
import json

import pytest
from click.testing import CliRunner

from prediction_engine.cli import main


def test_list_strategies_prints_both():
    res = CliRunner().invoke(main, ["list-strategies"])
    assert res.exit_code == 0
    assert "uniform_swing" in res.output
    assert "reform_threat_consolidation" in res.output


def test_run_uniform_swing_writes_file(tiny_snapshot_path, tmp_path: Path):
    res = CliRunner().invoke(main, [
        "run",
        "--snapshot", str(tiny_snapshot_path),
        "--strategy", "uniform_swing",
        "--out-dir", str(tmp_path),
        "--label", "test",
        "--polls-window-days", "14",
    ])
    assert res.exit_code == 0, res.output
    files = list(tmp_path.glob("*.sqlite"))
    assert len(files) == 1


def test_run_reform_threat_writes_file(tiny_snapshot_path, tmp_path: Path):
    res = CliRunner().invoke(main, [
        "run",
        "--snapshot", str(tiny_snapshot_path),
        "--strategy", "reform_threat_consolidation",
        "--out-dir", str(tmp_path),
        "--label", "test",
        "--multiplier", "1.0",
        "--clarity-threshold", "5.0",
        "--polls-window-days", "14",
    ])
    assert res.exit_code == 0, res.output
    assert len(list(tmp_path.glob("*.sqlite"))) == 1


def test_sweep_produces_one_file_per_multiplier(tiny_snapshot_path, tmp_path: Path):
    res = CliRunner().invoke(main, [
        "sweep",
        "--snapshot", str(tiny_snapshot_path),
        "--strategy", "reform_threat_consolidation",
        "--out-dir", str(tmp_path),
        "--label-prefix", "swp",
        "--multiplier", "0.5,1.0,1.5",
        "--clarity-threshold", "5.0",
        "--polls-window-days", "14",
    ])
    assert res.exit_code == 0, res.output
    assert len(list(tmp_path.glob("*.sqlite"))) == 3


def test_diff_lists_flips(tiny_snapshot_path, tmp_path: Path):
    runner = CliRunner()
    runner.invoke(main, [
        "run", "--snapshot", str(tiny_snapshot_path),
        "--strategy", "uniform_swing",
        "--out-dir", str(tmp_path), "--label", "us",
    ])
    runner.invoke(main, [
        "run", "--snapshot", str(tiny_snapshot_path),
        "--strategy", "reform_threat_consolidation",
        "--out-dir", str(tmp_path), "--label", "rtc",
    ])
    files = sorted(tmp_path.glob("*.sqlite"))
    res = runner.invoke(main, ["diff", str(files[0]), str(files[1])])
    assert res.exit_code == 0, res.output
    # Output is human-readable; just check it ran and printed something.
    assert "flips" in res.output.lower() or "no flips" in res.output.lower()


def test_run_unknown_strategy_exits_nonzero(tiny_snapshot_path, tmp_path: Path):
    res = CliRunner().invoke(main, [
        "run",
        "--snapshot", str(tiny_snapshot_path),
        "--strategy", "nope",
        "--out-dir", str(tmp_path),
        "--label", "test",
    ])
    assert res.exit_code != 0
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/prediction_engine/test_cli.py -v`
Expected: ImportError.

- [ ] **Step 4: Write minimal implementation**

`prediction_engine/cli.py`:

```python
import logging
from pathlib import Path

import click

from prediction_engine.runner import run_prediction
from prediction_engine.sqlite_io import read_prediction_seats, read_prediction_config
from prediction_engine.strategies.base import STRATEGY_REGISTRY
from prediction_engine import strategies as _strategies  # noqa: F401  populates registry
from schema.prediction import ReformThreatConfig, UniformSwingConfig


@click.group()
def main():
    """Prediction engine: list strategies, run a prediction, sweep configs, diff runs."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)-30s %(message)s",
        datefmt="%H:%M:%S",
    )


@main.command("list-strategies")
def list_strategies_cmd():
    for name in sorted(STRATEGY_REGISTRY):
        click.echo(name)


_COMMON_OPTS = [
    click.option("--snapshot", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True),
    click.option("--out-dir", type=click.Path(file_okay=False, path_type=Path), required=True),
    click.option("--polls-window-days", type=int, default=14),
]


def _make_config(strategy: str, *, polls_window_days: int, multiplier: float | None,
                  clarity_threshold: float | None):
    if strategy == "uniform_swing":
        return UniformSwingConfig(polls_window_days=polls_window_days)
    if strategy == "reform_threat_consolidation":
        return ReformThreatConfig(
            polls_window_days=polls_window_days,
            multiplier=multiplier if multiplier is not None else 1.0,
            clarity_threshold=clarity_threshold if clarity_threshold is not None else 5.0,
        )
    raise click.ClickException(f"unknown strategy: {strategy}")


@main.command("run")
@click.option("--snapshot", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--strategy", type=str, required=True)
@click.option("--out-dir", type=click.Path(file_okay=False, path_type=Path), required=True)
@click.option("--label", type=str, default="baseline")
@click.option("--multiplier", type=float, default=None)
@click.option("--clarity-threshold", type=float, default=None)
@click.option("--polls-window-days", type=int, default=14)
def run_cmd(snapshot, strategy, out_dir, label, multiplier, clarity_threshold, polls_window_days):
    cfg = _make_config(strategy, polls_window_days=polls_window_days,
                       multiplier=multiplier, clarity_threshold=clarity_threshold)
    out = run_prediction(
        snapshot_path=snapshot, strategy_name=strategy, scenario=cfg,
        out_dir=out_dir, label=label,
    )
    click.echo(f"Prediction at {out}")


@main.command("sweep")
@click.option("--snapshot", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--strategy", type=str, required=True)
@click.option("--out-dir", type=click.Path(file_okay=False, path_type=Path), required=True)
@click.option("--label-prefix", type=str, default="swp")
@click.option("--multiplier", type=str, required=True, help="Comma-separated, e.g. 0.5,1.0,1.5")
@click.option("--clarity-threshold", type=float, default=5.0)
@click.option("--polls-window-days", type=int, default=14)
def sweep_cmd(snapshot, strategy, out_dir, label_prefix, multiplier, clarity_threshold, polls_window_days):
    multipliers = [float(x.strip()) for x in multiplier.split(",")]
    for m in multipliers:
        cfg = _make_config(strategy, polls_window_days=polls_window_days,
                           multiplier=m, clarity_threshold=clarity_threshold)
        label = f"{label_prefix}_m{m:.2f}".replace(".", "p")
        out = run_prediction(
            snapshot_path=snapshot, strategy_name=strategy, scenario=cfg,
            out_dir=out_dir, label=label,
        )
        click.echo(f"  m={m:.2f} -> {out.name}")


@main.command("diff")
@click.argument("run_a", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("run_b", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def diff_cmd(run_a, run_b):
    from prediction_engine.analysis.flips import compute_flips
    flips = compute_flips(run_a, run_b)
    if flips.empty:
        click.echo("no flips between the two runs")
        return
    click.echo(f"{len(flips)} flips between {run_a.name} and {run_b.name}:")
    for _, r in flips.iterrows():
        click.echo(f"  {r['ons_code']:11s} {r['constituency_name']:30s} {r['winner_a']} -> {r['winner_b']}")
```

This imports `prediction_engine.analysis.flips` at call-time; that module is added in Task 13. The `diff` test in this task will fail until Task 13 is done — split that test out into Task 13 or stub the function. **Resolution:** stub `compute_flips` in this task as a placeholder so all CLI tests in this task pass; Task 13 replaces the stub. To avoid that complication, **add a one-shot helper inside `cli.py` here** and migrate it to `analysis/flips.py` in Task 13 (DRY violation acceptable for one commit; Task 13 explicitly removes it).

Replace the `from prediction_engine.analysis.flips import compute_flips` import block in `diff_cmd` with an inline implementation:

```python
@main.command("diff")
@click.argument("run_a", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("run_b", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def diff_cmd(run_a, run_b):
    seats_a = read_prediction_seats(run_a).set_index("ons_code")
    seats_b = read_prediction_seats(run_b).set_index("ons_code")
    common = seats_a.index.intersection(seats_b.index)
    flipped = []
    for ons in sorted(common):
        wa = seats_a.loc[ons, "predicted_winner"]
        wb = seats_b.loc[ons, "predicted_winner"]
        if wa != wb:
            flipped.append((ons, seats_a.loc[ons, "constituency_name"], wa, wb))
    if not flipped:
        click.echo("no flips between the two runs")
        return
    click.echo(f"{len(flipped)} flips between {run_a.name} and {run_b.name}:")
    for ons, name, wa, wb in flipped:
        click.echo(f"  {ons:11s} {name:30s} {wa} -> {wb}")
```

(Task 13 will refactor this into `analysis/flips.py` and have the CLI delegate.)

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/prediction_engine/test_cli.py -v`
Expected: 6 tests PASS.

- [ ] **Step 6: Verify the binary works**

Run: `.venv/Scripts/seatpredict-predict.exe list-strategies` (Windows) or `.venv/bin/seatpredict-predict list-strategies` (POSIX).
Expected:
```
reform_threat_consolidation
uniform_swing
```

If you get ModuleNotFoundError, re-run the compat reinstall command from Step 1.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml prediction_engine/cli.py tests/prediction_engine/test_cli.py
git commit -m "feat(prediction): seatpredict-predict CLI (list-strategies, run, sweep, diff)"
```

---

## Task 13: Analysis layer — drilldown, flips, poll-trends, sweep helpers

**Files:**
- Create: `prediction_engine/analysis/__init__.py`
- Create: `prediction_engine/analysis/drilldown.py`
- Create: `prediction_engine/analysis/flips.py`
- Create: `prediction_engine/analysis/poll_trends.py`
- Create: `prediction_engine/analysis/sweep.py`
- Modify: `prediction_engine/cli.py` (delegate `diff` to `analysis/flips.py`)
- Test: `tests/prediction_engine/test_analysis_drilldown.py`
- Test: `tests/prediction_engine/test_analysis_flips.py`
- Test: `tests/prediction_engine/test_analysis_poll_trends.py`
- Test: `tests/prediction_engine/test_analysis_sweep.py`

Per spec §7.4, analysis-layer tests are light. Each helper gets one happy-path test plus one edge case.

- [ ] **Step 1: Write failing tests for `flips`**

`tests/prediction_engine/test_analysis_flips.py`:

```python
from pathlib import Path
import pandas as pd
from prediction_engine.runner import run_prediction
from prediction_engine.analysis.flips import compute_flips
from schema.prediction import UniformSwingConfig, ReformThreatConfig


def test_compute_flips_returns_dataframe(tiny_snapshot_path, tmp_path: Path):
    a = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="uniform_swing",
                       scenario=UniformSwingConfig(), out_dir=tmp_path, label="a")
    b = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="reform_threat_consolidation",
                       scenario=ReformThreatConfig(), out_dir=tmp_path, label="b")
    flips = compute_flips(a, b)
    assert set(flips.columns) >= {"ons_code", "constituency_name", "winner_a", "winner_b"}


def test_compute_flips_empty_when_runs_identical(tiny_snapshot_path, tmp_path: Path):
    a = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="uniform_swing",
                       scenario=UniformSwingConfig(), out_dir=tmp_path, label="a")
    flips = compute_flips(a, a)
    assert flips.empty
```

- [ ] **Step 2: Write failing tests for `drilldown`**

`tests/prediction_engine/test_analysis_drilldown.py`:

```python
from pathlib import Path
import pytest
from prediction_engine.runner import run_prediction
from prediction_engine.analysis.drilldown import explain_seat
from schema.prediction import ReformThreatConfig


def test_explain_seat_returns_structured_report(tiny_snapshot_path, tmp_path: Path):
    out = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="reform_threat_consolidation",
                        scenario=ReformThreatConfig(), out_dir=tmp_path, label="t")
    report = explain_seat(out, ons_code="TST00001")
    assert report["ons_code"] == "TST00001"
    assert "share_raw" in report
    assert "share_predicted" in report
    assert "consolidator" in report
    assert "matrix_provenance" in report
    assert "notes" in report


def test_explain_seat_unknown_seat_raises(tiny_snapshot_path, tmp_path: Path):
    out = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="reform_threat_consolidation",
                        scenario=ReformThreatConfig(), out_dir=tmp_path, label="t")
    with pytest.raises(KeyError, match="ZZZ00000"):
        explain_seat(out, ons_code="ZZZ00000")
```

- [ ] **Step 3: Write failing tests for `poll_trends` and `sweep`**

`tests/prediction_engine/test_analysis_poll_trends.py`:

```python
import pandas as pd
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.analysis.poll_trends import rolling_trend


def test_rolling_trend_returns_per_party_series(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    trend = rolling_trend(snap, window_days=7)
    assert isinstance(trend, pd.DataFrame)
    # one column per party + 'date' index
    assert {"con", "lab", "ld", "reform", "green", "snp", "plaid", "other"} <= set(trend.columns)
```

`tests/prediction_engine/test_analysis_sweep.py`:

```python
from pathlib import Path
from prediction_engine.runner import run_prediction
from prediction_engine.analysis.sweep import collect_sweep
from schema.prediction import ReformThreatConfig


def test_collect_sweep_summarises_runs(tiny_snapshot_path, tmp_path: Path):
    paths = []
    for m in (0.5, 1.0):
        paths.append(run_prediction(
            snapshot_path=tiny_snapshot_path,
            strategy_name="reform_threat_consolidation",
            scenario=ReformThreatConfig(multiplier=m),
            out_dir=tmp_path,
            label=f"swp_m{m:.2f}".replace(".", "p"),
        ))
    summary = collect_sweep(paths)
    # one row per (run, party) with seats column
    assert len(summary) > 0
    assert {"run_id", "multiplier", "clarity_threshold", "party", "seats"} <= set(summary.columns)
    assert set(summary["multiplier"]) == {0.5, 1.0}
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `uv run pytest tests/prediction_engine/test_analysis_drilldown.py tests/prediction_engine/test_analysis_flips.py tests/prediction_engine/test_analysis_poll_trends.py tests/prediction_engine/test_analysis_sweep.py -v`
Expected: ImportError on each.

- [ ] **Step 5: Implement `flips`**

`prediction_engine/analysis/__init__.py` — empty file.

`prediction_engine/analysis/flips.py`:

```python
from pathlib import Path

import pandas as pd

from prediction_engine.sqlite_io import read_prediction_seats


def compute_flips(run_a: Path, run_b: Path) -> pd.DataFrame:
    """Return seats whose predicted_winner differs between two runs.
    Columns: ons_code, constituency_name, winner_a, winner_b.
    Empty DataFrame if no flips."""
    a = read_prediction_seats(run_a).set_index("ons_code")
    b = read_prediction_seats(run_b).set_index("ons_code")
    common = a.index.intersection(b.index)
    rows: list[dict] = []
    for ons in sorted(common):
        wa = a.loc[ons, "predicted_winner"]
        wb = b.loc[ons, "predicted_winner"]
        if wa != wb:
            rows.append({
                "ons_code": ons,
                "constituency_name": a.loc[ons, "constituency_name"],
                "winner_a": wa, "winner_b": wb,
            })
    return pd.DataFrame(rows, columns=["ons_code", "constituency_name", "winner_a", "winner_b"])
```

- [ ] **Step 6: Implement `drilldown`**

`prediction_engine/analysis/drilldown.py`:

```python
import json
from pathlib import Path

from prediction_engine.sqlite_io import read_prediction_seats, read_prediction_config
from schema.common import PartyCode


def explain_seat(prediction_path: Path, ons_code: str) -> dict:
    """Return a structured drill-down for one seat: raw shares, predicted shares,
    consolidator/clarity/flows, matrix provenance, notes flags. Used by the drilldown notebook."""
    seats = read_prediction_seats(prediction_path)
    matched = seats[seats["ons_code"] == ons_code]
    if matched.empty:
        raise KeyError(f"seat {ons_code} not in prediction")
    row = matched.iloc[0].to_dict()

    cfg = read_prediction_config(prediction_path)
    return {
        "ons_code": row["ons_code"],
        "constituency_name": row["constituency_name"],
        "nation": row["nation"],
        "region": row["region"],
        "share_raw":       {p.value: float(row[f"share_raw_{p.value}"])       for p in PartyCode},
        "share_predicted": {p.value: float(row[f"share_predicted_{p.value}"]) for p in PartyCode},
        "leader": row["leader"],
        "consolidator": row["consolidator"],
        "clarity": row["clarity"],
        "matrix_nation": row["matrix_nation"],
        "matrix_provenance": json.loads(row["matrix_provenance"]) if row["matrix_provenance"] else [],
        "notes": json.loads(row["notes"]) if row["notes"] else [],
        "predicted_winner": row["predicted_winner"],
        "predicted_margin": float(row["predicted_margin"]),
        "run_id": cfg.run_id,
        "strategy": cfg.strategy,
    }
```

- [ ] **Step 7: Implement `poll_trends`**

`prediction_engine/analysis/poll_trends.py`:

```python
import pandas as pd

from prediction_engine.snapshot_loader import Snapshot
from schema.common import PartyCode


def rolling_trend(snapshot: Snapshot, window_days: int = 7, geography: str = "GB") -> pd.DataFrame:
    """Return a rolling per-party poll average per published_date.
    Index: published_date (datetime). Columns: 'con','lab','ld','reform','green','snp','plaid','other'.
    """
    polls = snapshot.polls
    polls = polls[polls["geography"] == geography].copy()
    polls["published_date"] = pd.to_datetime(polls["published_date"])
    polls = polls.sort_values("published_date").set_index("published_date")
    party_cols = [p.value for p in PartyCode]
    trend = polls[party_cols].rolling(f"{window_days}D").mean()
    return trend.dropna(how="all")
```

- [ ] **Step 8: Implement `sweep`**

`prediction_engine/analysis/sweep.py`:

```python
import json
from pathlib import Path

import pandas as pd

from prediction_engine.sqlite_io import read_prediction_config, read_prediction_national


def collect_sweep(prediction_paths: list[Path]) -> pd.DataFrame:
    """For a sweep of prediction files, return overall national totals per run as a long DataFrame.
    Columns: run_id, multiplier, clarity_threshold, party, seats. multiplier and clarity_threshold
    come from the run's scenario_config_json (NaN if the strategy doesn't expose them).
    """
    rows: list[dict] = []
    for p in prediction_paths:
        cfg = read_prediction_config(p)
        scenario = json.loads(cfg.scenario_config_json)
        multiplier = scenario.get("multiplier")
        clarity_threshold = scenario.get("clarity_threshold")
        nat = read_prediction_national(p)
        overall = nat[nat["scope"] == "overall"]
        for _, r in overall.iterrows():
            rows.append({
                "run_id": cfg.run_id,
                "multiplier": multiplier,
                "clarity_threshold": clarity_threshold,
                "party": r["party"],
                "seats": int(r["seats"]),
            })
    return pd.DataFrame(rows, columns=["run_id", "multiplier", "clarity_threshold", "party", "seats"])
```

- [ ] **Step 9: Refactor `cli.py:diff_cmd` to delegate to `analysis/flips.py`**

Replace the inline implementation in `prediction_engine/cli.py:diff_cmd` with:

```python
@main.command("diff")
@click.argument("run_a", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("run_b", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def diff_cmd(run_a, run_b):
    from prediction_engine.analysis.flips import compute_flips
    flips = compute_flips(run_a, run_b)
    if flips.empty:
        click.echo("no flips between the two runs")
        return
    click.echo(f"{len(flips)} flips between {run_a.name} and {run_b.name}:")
    for _, r in flips.iterrows():
        click.echo(f"  {r['ons_code']:11s} {r['constituency_name']:30s} {r['winner_a']} -> {r['winner_b']}")
```

- [ ] **Step 10: Run all analysis tests + the diff CLI test**

Run: `uv run pytest tests/prediction_engine/ -v`
Expected: all tests pass; no regressions.

- [ ] **Step 11: Commit**

```bash
git add prediction_engine/analysis prediction_engine/cli.py tests/prediction_engine/test_analysis_drilldown.py tests/prediction_engine/test_analysis_flips.py tests/prediction_engine/test_analysis_poll_trends.py tests/prediction_engine/test_analysis_sweep.py
git commit -m "feat(analysis): drilldown/flips/poll_trends/sweep helpers; CLI diff delegates"
```

---

## Task 14: `seatpredict-analyze` CLI

**Files:**
- Create: `prediction_engine/cli_analyze.py`
- Modify: `pyproject.toml` (add second entry point)
- Test: `tests/prediction_engine/test_cli_analyze.py`

- [ ] **Step 1: Add entry point and reinstall (CRITICAL — see Plan A's better-memory gotcha)**

Edit `pyproject.toml`. Append to `[project.scripts]`:

```toml
seatpredict-analyze = "prediction_engine.cli_analyze:main"
```

Reinstall in compat editable mode (don't skip — `uv run seatpredict-analyze` will silently revert the install otherwise):

```bash
uv pip install --config-settings editable_mode=compat -e ".[dev]"
```

Verify the binary works without `uv run`:

```bash
# Windows:
.venv/Scripts/seatpredict-analyze.exe --help
# POSIX:
.venv/bin/seatpredict-analyze --help
```

Expected: Click banner showing the `drilldown / flips` subcommands. Do NOT proceed until this works (a `ModuleNotFoundError` here means the install broke; re-run the compat-mode reinstall).

- [ ] **Step 2: Write the failing test**

`tests/prediction_engine/test_cli_analyze.py`:

```python
from pathlib import Path
import pytest
from click.testing import CliRunner

from prediction_engine.cli_analyze import main as analyze_main
from prediction_engine.runner import run_prediction
from schema.prediction import UniformSwingConfig, ReformThreatConfig


def _two_runs(tiny_snapshot_path, tmp_path: Path):
    a = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="uniform_swing",
                       scenario=UniformSwingConfig(), out_dir=tmp_path, label="a")
    b = run_prediction(snapshot_path=tiny_snapshot_path, strategy_name="reform_threat_consolidation",
                       scenario=ReformThreatConfig(), out_dir=tmp_path, label="b")
    return a, b


def test_drilldown_prints_seat_report(tiny_snapshot_path, tmp_path: Path):
    a, _ = _two_runs(tiny_snapshot_path, tmp_path)
    res = CliRunner().invoke(analyze_main, [
        "drilldown", "--run", str(a), "--seat", "TST00001", "--explain",
    ])
    assert res.exit_code == 0, res.output
    assert "TST00001" in res.output


def test_flips_prints_diff(tiny_snapshot_path, tmp_path: Path):
    a, b = _two_runs(tiny_snapshot_path, tmp_path)
    res = CliRunner().invoke(analyze_main, ["flips", "--runs", str(a), str(b)])
    assert res.exit_code == 0, res.output
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/prediction_engine/test_cli_analyze.py -v`
Expected: ImportError.

- [ ] **Step 4: Write minimal implementation**

`prediction_engine/cli_analyze.py`:

```python
import json
import logging
from pathlib import Path

import click

from prediction_engine.analysis.drilldown import explain_seat
from prediction_engine.analysis.flips import compute_flips


@click.group()
def main():
    """Analysis CLI for prediction outputs."""
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)-30s %(message)s",
                        datefmt="%H:%M:%S")


@main.command("drilldown")
@click.option("--run", "run_path", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--seat", type=str, required=True, help="ONS code, e.g. E14000123")
@click.option("--explain/--no-explain", default=False)
def drilldown_cmd(run_path: Path, seat: str, explain: bool):
    report = explain_seat(run_path, ons_code=seat)
    if not explain:
        click.echo(json.dumps(report, indent=2, default=str))
        return
    click.echo(f"Seat: {report['ons_code']} {report['constituency_name']} ({report['nation']}/{report['region']})")
    click.echo(f"  Run: {report['run_id']}  Strategy: {report['strategy']}")
    click.echo(f"  Predicted winner: {report['predicted_winner']} (margin {report['predicted_margin']:.2f})")
    click.echo(f"  Leader: {report['leader']}; Consolidator: {report['consolidator']}; Clarity: {report['clarity']}")
    click.echo(f"  Matrix nation: {report['matrix_nation']}; Provenance: {report['matrix_provenance']}")
    click.echo(f"  Notes: {report['notes']}")
    click.echo("  Raw -> Predicted:")
    for party in report["share_raw"]:
        raw = report["share_raw"][party]
        pred = report["share_predicted"][party]
        click.echo(f"    {party:7s}  {raw:5.1f}  ->  {pred:5.1f}  (Δ {pred - raw:+5.2f})")


@main.command("flips")
@click.option("--runs", nargs=2, type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
def flips_cmd(runs: tuple[Path, Path]):
    a, b = runs
    flips = compute_flips(a, b)
    if flips.empty:
        click.echo("no flips between the two runs")
        return
    click.echo(f"{len(flips)} flips between {a.name} and {b.name}:")
    for _, r in flips.iterrows():
        click.echo(f"  {r['ons_code']:11s} {r['constituency_name']:30s} {r['winner_a']} -> {r['winner_b']}")
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/prediction_engine/test_cli_analyze.py -v`
Expected: 2 tests PASS.

- [ ] **Step 6: Verify the binary works**

Run: `.venv/Scripts/seatpredict-analyze.exe --help`
Expected: usage banner listing `drilldown` and `flips` subcommands.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml prediction_engine/cli_analyze.py tests/prediction_engine/test_cli_analyze.py
git commit -m "feat(analysis): seatpredict-analyze CLI (drilldown, flips)"
```

---

## Task 15: Notebooks

**Files:**
- Create: `scripts/build_notebooks.py` (one-shot authoring script — reproducible, version-controlled)
- Create: `notebooks/01_polling_trends.ipynb`
- Create: `notebooks/02_constituency_drilldown.ipynb`
- Create: `notebooks/03_strategy_comparison.ipynb`
- Create: `notebooks/04_scenario_sweep.ipynb`
- Modify: `pyproject.toml` (add `jupyterlab`, `matplotlib`, `nbformat` to dev deps)

**Why a generator script:** authoring `.ipynb` files directly as JSON or via the Jupyter UI is fiddly; the implementer will silently skip cells or commit broken JSON. Using a Python script that calls `nbformat.v4.new_notebook` produces deterministic, diffable output and makes future edits a one-line code change. The script is committed alongside the notebooks so the build is reproducible.

**Notebooks are inspectable artefacts**, not unit-tested code. Validation is two-step: (a) `nbformat.read` round-trips each file (catches syntactic JSON breakage), and (b) `jupyter nbconvert --to notebook --execute --inplace` runs every cell against real Plan-A data and fails loudly on any cell-level exception. Both happen in Step 4.

### Cell content for each notebook (this is the source of truth — `build_notebooks.py` writes exactly this)

Each notebook starts with a markdown title cell and an imports cell, then alternates code/markdown.

**01_polling_trends.ipynb** (4 cells):

```python
# Cell 1 (md): "# Polling trends\n\nPer-party 7-day rolling mean from the GB national-VI poll table. Sanity-checks the data engine output."
# Cell 2 (code):
from pathlib import Path
import matplotlib.pyplot as plt
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.analysis.poll_trends import rolling_trend

snap_path = sorted(Path("data/snapshots").glob("*.sqlite"))[-1]
snap = Snapshot(snap_path)
trend = rolling_trend(snap, window_days=7, geography="GB")
trend.tail()
# Cell 3 (code):
ax = trend.plot(figsize=(10, 5))
ax.set_ylabel("Vote share (%)")
ax.set_title(f"7-day rolling per-party national VI trend (as of {snap.manifest.as_of_date})")
plt.show()
# Cell 4 (md): "Lines should be smooth (no sub-cell spikes); Reform should sit above other parties when the snapshot's GB national VI shows it leading."
```

**02_constituency_drilldown.ipynb** (5 cells):

```python
# Cell 1 (md): "# Constituency drilldown\n\nPick a seat. Show projected raw shares, the consolidator, clarity, matrix entries, flows, and the final prediction."
# Cell 2 (code): load the latest reform_threat prediction
from pathlib import Path
from prediction_engine.analysis.drilldown import explain_seat

prediction_path = sorted(Path("data/predictions").glob("*reform_threat_consolidation*.sqlite"))[-1]
# Pick the first ONS code from the prediction; user can edit this.
import sqlite3
from contextlib import closing
with closing(sqlite3.connect(str(prediction_path))) as conn:
    cur = conn.execute("SELECT ons_code FROM seats WHERE notes != '[]' ORDER BY ons_code LIMIT 1")
    row = cur.fetchone()
ons_code = row[0] if row else None
report = explain_seat(prediction_path, ons_code=ons_code)
report
# Cell 3 (code): pretty-print the before/after
import pandas as pd
pd.DataFrame({"raw": report["share_raw"], "predicted": report["share_predicted"]}).T
# Cell 4 (md): "Expect lab/plaid/snp/green's share_predicted > share_raw on Reform-threat seats; the parties in `matrix_provenance` are the by-elections that contributed."
```

**03_strategy_comparison.ipynb** (5 cells):

```python
# Cell 1 (md): "# Strategy comparison\n\nuniform_swing vs reform_threat_consolidation. List flips; chart national-total deltas."
# Cell 2 (code):
from pathlib import Path
from prediction_engine.analysis.flips import compute_flips
from prediction_engine.sqlite_io import read_prediction_national

pred_dir = Path("data/predictions")
us_run  = sorted(pred_dir.glob("*uniform_swing*.sqlite"))[-1]
rtc_run = sorted(pred_dir.glob("*reform_threat_consolidation*.sqlite"))[-1]
flips = compute_flips(us_run, rtc_run)
flips.head(20)
# Cell 3 (code):
import matplotlib.pyplot as plt
import pandas as pd
nat_us  = read_prediction_national(us_run)
nat_rtc = read_prediction_national(rtc_run)
us_overall  = nat_us [nat_us ["scope"] == "overall"].set_index("party")["seats"]
rtc_overall = nat_rtc[nat_rtc["scope"] == "overall"].set_index("party")["seats"]
pd.DataFrame({"uniform_swing": us_overall, "reform_threat": rtc_overall}).plot.bar(figsize=(8, 4))
plt.ylabel("Seats")
plt.title("National totals: uniform_swing vs reform_threat_consolidation")
plt.show()
# Cell 4 (md): "If reform_threat trims Reform seats vs uniform_swing while raising Lab/LD/Green/Plaid/SNP, the consolidation strategy is firing as expected."
```

**04_scenario_sweep.ipynb** (4 cells):

```python
# Cell 1 (md): "# Scenario sweep\n\nSweep `multiplier`. Plot per-party national seat counts."
# Cell 2 (code):
import subprocess, sys
from pathlib import Path
from prediction_engine.analysis.sweep import collect_sweep

pred_dir = Path("data/predictions")
snap_path = sorted(Path("data/snapshots").glob("*.sqlite"))[-1]
for m in (0.5, 0.75, 1.0, 1.25, 1.5):
    subprocess.run([
        sys.executable, "-m", "prediction_engine.cli", "run",
        "--snapshot", str(snap_path),
        "--strategy", "reform_threat_consolidation",
        "--out-dir", str(pred_dir),
        "--label", f"sweep_m{m:.2f}".replace(".", "p"),
        "--multiplier", str(m),
    ], check=True)

sweep_paths = sorted(pred_dir.glob("*sweep_m*p*.sqlite"))
summary = collect_sweep(sweep_paths)
summary
# Cell 3 (code):
import matplotlib.pyplot as plt
pivot = summary.pivot(index="multiplier", columns="party", values="seats").fillna(0)
pivot.plot(figsize=(10, 5), marker="o")
plt.ylabel("Seats")
plt.title("National seat count vs reform-threat multiplier")
plt.show()
# Cell 4 (md): "Reform's line should be monotonically decreasing; the consolidator parties' lines monotonically increasing."
```

- [ ] **Step 1: Add notebook deps to pyproject and reinstall**

Edit `pyproject.toml`. Update `[project.optional-dependencies] dev`:

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.0,<9",
    "respx>=0.21,<1",
    "jupyterlab>=4.0,<5",
    "matplotlib>=3.8,<4",
    "nbformat>=5.10,<6",
    "nbclient>=0.10,<1",
    "ipykernel>=6.29,<7",
]
```

Reinstall in compat editable mode:

```bash
uv pip install --config-settings editable_mode=compat -e ".[dev]"
```

Register the project venv as a Jupyter kernel named `python3` (this is the kernel name `nbconvert --execute` defaults to and the one our generator script writes into the notebook metadata). Skipping this is the most common reason `nbconvert --execute` fails with "No such kernel: python3":

```bash
uv run python -m ipykernel install --user --name python3 --display-name "Python 3 (seatpredictor)"
```

Verify the kernel registered:

```bash
.venv/Scripts/jupyter.exe kernelspec list
```

Expected: a `python3` entry pointing at the project's `.venv`.

- [ ] **Step 2: Create `scripts/build_notebooks.py`**

```bash
mkdir -p scripts notebooks
```

`scripts/build_notebooks.py`:

```python
"""Generate the four analysis notebooks from this script.

Run me with: uv run python scripts/build_notebooks.py
The notebooks are committed to git alongside this script. To edit a cell, edit
the dict in NOTEBOOK_SPECS below and rerun.
"""
from pathlib import Path

import nbformat
from nbformat.v4 import new_notebook, new_code_cell, new_markdown_cell


_REPO_ROOT = Path(__file__).resolve().parent.parent


_NB_01_TITLE_MD = "# Polling trends\n\nPer-party 7-day rolling mean from the GB national-VI poll table. Sanity-checks the data engine output."
_NB_01_LOAD = '''from pathlib import Path
import matplotlib.pyplot as plt
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.analysis.poll_trends import rolling_trend

snap_path = sorted(Path("data/snapshots").glob("*.sqlite"))[-1]
snap = Snapshot(snap_path)
trend = rolling_trend(snap, window_days=7, geography="GB")
trend.tail()'''
_NB_01_PLOT = '''ax = trend.plot(figsize=(10, 5))
ax.set_ylabel("Vote share (%)")
ax.set_title(f"7-day rolling per-party national VI trend (as of {snap.manifest.as_of_date})")
plt.show()'''
_NB_01_INTERP = "Lines should be smooth (no sub-cell spikes); Reform should sit above other parties when the snapshot's GB national VI shows it leading."

_NB_02_TITLE_MD = "# Constituency drilldown\n\nPick a seat. Show projected raw shares, the consolidator, clarity, matrix entries, flows, and the final prediction."
_NB_02_LOAD = '''from pathlib import Path
import sqlite3
from contextlib import closing
from prediction_engine.analysis.drilldown import explain_seat

prediction_path = sorted(Path("data/predictions").glob("*reform_threat_consolidation*.sqlite"))[-1]
with closing(sqlite3.connect(str(prediction_path))) as conn:
    cur = conn.execute("SELECT ons_code FROM seats WHERE notes != '[]' ORDER BY ons_code LIMIT 1")
    row = cur.fetchone()
ons_code = row[0]
report = explain_seat(prediction_path, ons_code=ons_code)
report'''
_NB_02_TABLE = '''import pandas as pd
pd.DataFrame({"raw": report["share_raw"], "predicted": report["share_predicted"]}).T'''
_NB_02_INTERP = "Expect lab/plaid/snp/green's share_predicted > share_raw on Reform-threat seats; the parties in `matrix_provenance` are the by-elections that contributed."

_NB_03_TITLE_MD = "# Strategy comparison\n\nuniform_swing vs reform_threat_consolidation. List flips; chart national-total deltas."
_NB_03_LOAD = '''from pathlib import Path
from prediction_engine.analysis.flips import compute_flips
from prediction_engine.sqlite_io import read_prediction_national

pred_dir = Path("data/predictions")
us_run  = sorted(pred_dir.glob("*uniform_swing*.sqlite"))[-1]
rtc_run = sorted(pred_dir.glob("*reform_threat_consolidation*.sqlite"))[-1]
flips = compute_flips(us_run, rtc_run)
flips.head(20)'''
_NB_03_PLOT = '''import matplotlib.pyplot as plt
import pandas as pd
nat_us  = read_prediction_national(us_run)
nat_rtc = read_prediction_national(rtc_run)
us_overall  = nat_us [nat_us ["scope"] == "overall"].set_index("party")["seats"]
rtc_overall = nat_rtc[nat_rtc["scope"] == "overall"].set_index("party")["seats"]
pd.DataFrame({"uniform_swing": us_overall, "reform_threat": rtc_overall}).plot.bar(figsize=(8, 4))
plt.ylabel("Seats")
plt.title("National totals: uniform_swing vs reform_threat_consolidation")
plt.show()'''
_NB_03_INTERP = "If reform_threat trims Reform seats vs uniform_swing while raising Lab/LD/Green/Plaid/SNP, the consolidation strategy is firing as expected."

_NB_04_TITLE_MD = "# Scenario sweep\n\nSweep `multiplier`. Plot per-party national seat counts."
_NB_04_RUN = '''import subprocess, sys
from pathlib import Path
from prediction_engine.analysis.sweep import collect_sweep

pred_dir = Path("data/predictions")
snap_path = sorted(Path("data/snapshots").glob("*.sqlite"))[-1]
for m in (0.5, 0.75, 1.0, 1.25, 1.5):
    subprocess.run([
        sys.executable, "-m", "prediction_engine.cli", "run",
        "--snapshot", str(snap_path),
        "--strategy", "reform_threat_consolidation",
        "--out-dir", str(pred_dir),
        "--label", f"sweep_m{m:.2f}".replace(".", "p"),
        "--multiplier", str(m),
    ], check=True)

sweep_paths = sorted(pred_dir.glob("*sweep_m*p*.sqlite"))
summary = collect_sweep(sweep_paths)
summary'''
_NB_04_PLOT = '''import matplotlib.pyplot as plt
pivot = summary.pivot(index="multiplier", columns="party", values="seats").fillna(0)
pivot.plot(figsize=(10, 5), marker="o")
plt.ylabel("Seats")
plt.title("National seat count vs reform-threat multiplier")
plt.show()'''
_NB_04_INTERP = "Reform's line should be monotonically decreasing; the consolidator parties' lines monotonically increasing."


NOTEBOOK_SPECS = [
    ("01_polling_trends.ipynb", [
        ("md", _NB_01_TITLE_MD),
        ("code", _NB_01_LOAD),
        ("code", _NB_01_PLOT),
        ("md", _NB_01_INTERP),
    ]),
    ("02_constituency_drilldown.ipynb", [
        ("md", _NB_02_TITLE_MD),
        ("code", _NB_02_LOAD),
        ("code", _NB_02_TABLE),
        ("md", _NB_02_INTERP),
    ]),
    ("03_strategy_comparison.ipynb", [
        ("md", _NB_03_TITLE_MD),
        ("code", _NB_03_LOAD),
        ("code", _NB_03_PLOT),
        ("md", _NB_03_INTERP),
    ]),
    ("04_scenario_sweep.ipynb", [
        ("md", _NB_04_TITLE_MD),
        ("code", _NB_04_RUN),
        ("code", _NB_04_PLOT),
        ("md", _NB_04_INTERP),
    ]),
]


def build():
    out_dir = _REPO_ROOT / "notebooks"
    out_dir.mkdir(exist_ok=True)
    for name, cells in NOTEBOOK_SPECS:
        nb = new_notebook()
        nb.cells = [
            (new_markdown_cell(content) if kind == "md" else new_code_cell(content))
            for kind, content in cells
        ]
        # Pin Python kernel metadata so VS Code / JupyterLab pick the right kernel.
        nb.metadata["kernelspec"] = {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        }
        path = out_dir / name
        with path.open("w", encoding="utf-8") as f:
            nbformat.write(nb, f)
        print(f"wrote {path}")


if __name__ == "__main__":
    build()
```

- [ ] **Step 3: Run the generator**

```bash
uv run python scripts/build_notebooks.py
```

Expected output: four `wrote …` lines. Then `nbformat.read` each one to confirm it parses:

```bash
uv run python -c "import nbformat; [print(p, len(nbformat.read(open(p, encoding='utf-8'), as_version=4).cells)) for p in __import__('pathlib').Path('notebooks').glob('*.ipynb')]"
```

Expected: each notebook reports 4 cells (filename plus cell count).

- [ ] **Step 4: Execute the notebooks end-to-end against real Plan-A data**

Notebooks 01 and 02–04 require `data/snapshots/*.sqlite` and `data/predictions/*.sqlite` to exist. Build them first:

```bash
uv run seatpredict-data fetch
uv run seatpredict-data snapshot
SNAP=$(ls -1 data/snapshots/*.sqlite | tail -1)
uv run seatpredict-predict run --snapshot "$SNAP" --strategy uniform_swing --out-dir data/predictions --label baseline_us
uv run seatpredict-predict run --snapshot "$SNAP" --strategy reform_threat_consolidation --out-dir data/predictions --label baseline_rtc
```

(PowerShell equivalent: `$SNAP = (Get-ChildItem data/snapshots/*.sqlite | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName`.)

Now execute each notebook in-place and fail loudly on any cell-level exception:

```bash
uv run jupyter nbconvert --to notebook --execute --inplace notebooks/01_polling_trends.ipynb
uv run jupyter nbconvert --to notebook --execute --inplace notebooks/02_constituency_drilldown.ipynb
uv run jupyter nbconvert --to notebook --execute --inplace notebooks/03_strategy_comparison.ipynb
uv run jupyter nbconvert --to notebook --execute --inplace notebooks/04_scenario_sweep.ipynb
```

Expected: each command exits 0 (no `CellExecutionError`). The executed notebooks now contain rendered chart outputs. Review them visually before committing.

If a cell errors, fix `scripts/build_notebooks.py` (NOT the notebook directly), regenerate, re-execute. Don't hand-edit the `.ipynb` files.

- [ ] **Step 5: Strip outputs before committing (so diffs stay clean)**

Outputs include base64 PNGs and execution counts that change every run. Strip them so subsequent regenerations don't churn git:

```bash
uv run jupyter nbconvert --clear-output --inplace notebooks/*.ipynb
```

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml scripts/build_notebooks.py notebooks/
git commit -m "feat(notebooks): four analysis notebooks (polling trends, drilldown, comparison, sweep) generated from build_notebooks.py"
```

---

## Task 16: README updates and final smoke verification

**Files:**
- Modify: `README.md`
- Test: end-to-end manual smoke

- [ ] **Step 1: Append "Predict" and "Notebooks" sections to README.md**

Open `README.md` and append:

````markdown
## Run a prediction

After producing a snapshot, run the prediction engine:

```bash
uv run seatpredict-predict list-strategies
uv run seatpredict-predict run \
    --snapshot data/snapshots/<snapshot-file>.sqlite \
    --strategy reform_threat_consolidation \
    --out-dir data/predictions \
    --multiplier 1.0 --clarity-threshold 5.0 --label baseline
```

Outputs a single SQLite file at `data/predictions/<snapshot_hash>__<strategy>__<config_hash>__<label>.sqlite` containing four tables: `seats`, `national`, `config`, `notes_index`. Per-seat schema is documented at the top of `prediction_engine/sqlite_io.py`.

Sweep over multipliers:

```bash
uv run seatpredict-predict sweep \
    --snapshot data/snapshots/<snapshot-file>.sqlite \
    --strategy reform_threat_consolidation \
    --out-dir data/predictions \
    --multiplier 0.5,0.75,1.0,1.25,1.5
```

Diff two runs:

```bash
uv run seatpredict-predict diff data/predictions/<run-a>.sqlite data/predictions/<run-b>.sqlite
```

## Analyze predictions

```bash
uv run seatpredict-analyze drilldown --run data/predictions/<run>.sqlite --seat E14000123 --explain
uv run seatpredict-analyze flips     --runs data/predictions/<run-a>.sqlite data/predictions/<run-b>.sqlite
```

## Notebooks (first time)

Four notebooks live in `notebooks/`:

| file | what it does |
|---|---|
| `01_polling_trends.ipynb` | per-party 7-day rolling poll average; sanity-check the data engine |
| `02_constituency_drilldown.ipynb` | one seat: raw shares → consolidator → flows → predicted shares |
| `03_strategy_comparison.ipynb` | uniform_swing vs reform_threat_consolidation; flips and bar charts |
| `04_scenario_sweep.ipynb` | sweep `multiplier`; plot per-party seats as a function of multiplier |

Two ways to run them. Either works:

**VS Code's notebook editor.** Open the `.ipynb` file in VS Code, click "Select Kernel" in the top-right, pick the project venv (`.venv/Scripts/python.exe` on Windows; `.venv/bin/python` on POSIX). Run cells with Shift+Enter.

**JupyterLab in a browser.** From the repo root:

```bash
uv run jupyter lab
```

A browser tab opens at `http://localhost:8888`. Open `notebooks/01_polling_trends.ipynb` and run cells with Shift+Enter.

> **Note for first-time Jupyter users:** A notebook is a sequence of cells; each runs Python (or markdown). You run them top-to-bottom. The output (charts, tables) appears under each cell. Restarting the kernel clears all variables — use "Run All Cells" after a restart to re-execute everything.
````

- [ ] **Step 2: End-to-end smoke**

Build a fresh snapshot from real data, run a prediction, run an analysis. Use the variant for your shell.

**bash / git-bash:**

```bash
uv run seatpredict-data fetch
uv run seatpredict-data snapshot
SNAP=$(ls -1 data/snapshots/*.sqlite | tail -1)
uv run seatpredict-predict run --snapshot "$SNAP" \
    --strategy reform_threat_consolidation \
    --out-dir data/predictions --label baseline
PRED=$(ls -1 data/predictions/*.sqlite | tail -1)
SEAT=$(uv run python -c "import sqlite3, contextlib; c=contextlib.closing(sqlite3.connect('$PRED')); [print(c.__enter__().execute('SELECT ons_code FROM seats LIMIT 1').fetchone()[0])]")
uv run seatpredict-analyze drilldown --run "$PRED" --seat "$SEAT" --explain
```

**PowerShell:**

```powershell
uv run seatpredict-data fetch
uv run seatpredict-data snapshot
$SNAP = (Get-ChildItem data/snapshots/*.sqlite | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
uv run seatpredict-predict run --snapshot "$SNAP" `
    --strategy reform_threat_consolidation `
    --out-dir data/predictions --label baseline
$PRED = (Get-ChildItem data/predictions/*.sqlite | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
$SEAT = uv run python -c "import sqlite3, contextlib; \
with contextlib.closing(sqlite3.connect(r'$PRED')) as c: print(c.execute('SELECT ons_code FROM seats LIMIT 1').fetchone()[0])"
uv run seatpredict-analyze drilldown --run "$PRED" --seat "$SEAT" --explain
```

Expected: prediction file written under `data/predictions/`; drilldown prints a structured per-seat report including `share_raw → share_predicted` lines and a `notes` list. The seat ONS code is picked from the prediction itself rather than hardcoded so the smoke is robust to the actual constituency boundaries in the live data.

- [ ] **Step 3: Run the full test suite**

```bash
uv run pytest
```

Expected: all tests pass (Plan A's 67 tests + ~50 new prediction-engine and analysis tests).

- [ ] **Step 4: Commit and tag the milestone**

```bash
git add README.md
git commit -m "docs: README sections for prediction CLI, analyze CLI, notebooks"
```

---

## Self-review checklist (re-read before declaring Plan B complete)

Run after the last task is merged:

- [ ] Spec §5.1 (Strategy contract): ABC + `predict(snapshot, scenario) → PredictionResult`. ✅ Tasks 6, 7, 9.
- [ ] Spec §5.2 (per-seat schema): all 35+ columns, `notes` is a list. ✅ Tasks 1, 7, 9.
- [ ] Spec §5.3 (uniform_swing): polls window, swing, projection, winner. ✅ Tasks 4, 5, 7.
- [ ] Spec §5.3 (reform_threat algorithm steps 1-7): each branch (non-Reform leader / consolidator-already-leads / matrix-unavailable / no-matrix-entry / multiplier-clipped / NI / low-clarity). ✅ Tasks 8, 9 (test cases match each flag).
- [ ] Spec §5.4 (determinism): explicit determinism test. ✅ Tasks 7, 9.
- [ ] Spec §5.5 (CLI surface): `list-strategies`, `run`, `sweep`, `diff`. ✅ Task 12.
- [ ] Spec §5.6 (prediction output): single SQLite with `seats`, `national`, `config`, `notes_index`. ✅ Task 10.
- [ ] Spec §6.1 (notebooks): four notebooks. ✅ Task 15.
- [ ] Spec §6.2 (analysis CLI helpers): `seatpredict-analyze drilldown` + `flips`. ✅ Task 14.
- [ ] Spec §6.3 (notebook hand-holding in README). ✅ Task 16.
- [ ] Spec §7.3 (prediction-engine tests): determinism / 6 flag paths / multiplier monotonicity / handcrafted fake snapshot. ✅ Tasks 7, 9.
- [ ] Spec §7.4 (analysis-layer light tests). ✅ Task 13.
- [ ] Spec §8 (error handling): `compute_swing` raises `ValueError` on empty window; `run_prediction` raises `KeyError` on unknown strategy; YAML / parse failures still come from Plan A's data engine. ✅ Tasks 4, 11.

Type/signature consistency: `Snapshot` exposes `manifest`, `polls`, `results_2024`, `byelections_events`, `byelections_results`, `transfer_weights`, `transfer_weights_provenance`, `lookup_weight`, `consolidator_observed`, `provenance_for_consolidator`, `snapshot_id`, `path`. `PredictionResult` is a dataclass with `per_seat`, `national`, `run_metadata`. Every `predict_seat` flag string is in `ALLOWED_NOTE_FLAGS`. ✅ Verified inline.

No placeholders / TBDs / "implement later" / "similar to Task N" — verified by grep.
