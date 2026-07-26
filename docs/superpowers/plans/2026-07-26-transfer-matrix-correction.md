# Transfer Matrix Correction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct the by-election transfer-weight derivation so a weight represents transfer to the consolidator rather than total vote loss, and add a hand-curated override file that can replace or create matrix cells.

**Architecture:** All changes live in `data_engine/` and `schema/`. `_compute_flows` gains a `scale` factor derived from the consolidator's own gain. A new YAML loader feeds `derive_transfer_matrix` an optional overrides DataFrame, applied after the existing groupby. No file under `prediction_engine/` is modified — `Snapshot.lookup_weight` and friends are generic queries that pick up the new cells unchanged.

**Tech Stack:** Python 3.11+, pandas, pydantic v2, PyYAML, pytest, click.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-26-transfer-matrix-correction-design.md`.
- Run the binary directly, never `uv run` — `uv run` reverts the editable install. Use `.venv/Scripts/seatpredict-data.exe` and `.venv/Scripts/seatpredict-predict.exe`.
- Run tests with `.venv/Scripts/python.exe -m pytest`.
- `PRIOR_SHARE_THRESHOLD = 2.0` is unchanged throughout.
- Reform and Restore are never flow *sources*, but their losses ARE counted in the `total_loss` denominator.
- Only the `england / lab` block is hand-curated. `england / green`, `wales / plaid`, `england / ld` and `scotland / green` stay derived.
- Every commit message ends with `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.
- The Bash tool here is Git Bash. For multi-line commit messages use a heredoc (`git commit -F - <<'EOF'`), never PowerShell here-string syntax.

---

## File Structure

**Modified:**
- `data_engine/transforms/transfer_matrix.py` — `_compute_flows` rewritten; `derive_transfer_matrix` gains an `overrides` parameter.
- `schema/transfer_weights.py` — new `TransferWeightOverride` model; `TransferWeightCell.n` constraint relaxed.
- `data_engine/snapshot.py` — new config field, overrides loaded and passed through, `SCHEMA_VERSION` bumped, override file hashed into `source_versions`.
- `data_engine/cli.py` — pass the override path in the `snapshot` and `backfill` commands.

**Created:**
- `data_engine/sources/transfer_overrides.py` — YAML loader.
- `data/hand_curated/transfer_overrides.yaml` — the four curated weights.
- `tests/data_engine/test_transfer_overrides.py` — loader tests.

**Test files updated:**
- `tests/data_engine/test_transfer_matrix.py` — formula change + override application.
- `tests/schema/test_transfer_weights.py` — `n = 0` now valid; new model tests.
- `tests/prediction_engine/test_fixture_sanity.py` — hard-coded expected weights.
- `tests/data_engine/test_snapshot_orchestrator.py` — `source_versions` key list.

---

### Task 1: Corrected derivation

**Files:**
- Modify: `data_engine/transforms/transfer_matrix.py:95-113` (`_compute_flows`)
- Modify: `data_engine/snapshot.py:25` (`SCHEMA_VERSION`)
- Test: `tests/data_engine/test_transfer_matrix.py`
- Test: `tests/prediction_engine/test_fixture_sanity.py:45-56`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `_compute_flows(ev_results: pd.DataFrame, consolidator: PartyCode) -> dict[PartyCode, float]` — signature unchanged, values now scaled. `SCHEMA_VERSION = 4`.

**Background.** The current formula, `(prior - actual) / prior`, measures a source party's total vote loss, not its transfer to the consolidator. It can attribute more vote to the consolidator than the consolidator actually gained. The fix scales every flow by `consolidator_gain / total_loss`, so the transferred total equals the consolidator's real gain.

`SCHEMA_VERSION` is bumped in this task because the module docstring at `data_engine/snapshot.py:146-148` requires it whenever parser semantics change without inputs changing. One bump covers this task and Task 5.

- [ ] **Step 1: Write the failing tests**

Replace the body of `test_lab_to_plaid_flow_rate` in `tests/data_engine/test_transfer_matrix.py` and add four new tests. The existing `_fake_byelections()` helper is unchanged.

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/data_engine/test_transfer_matrix.py -v`

Expected: `test_lab_to_plaid_flow_rate`, `test_every_source_scaled_by_the_same_factor`, `test_transferred_total_cannot_exceed_consolidator_gain` and `test_shrinking_reform_counted_in_total_loss` FAIL on the assertion comparing weights (current code returns the unscaled 0.7608696 for lab). `test_scale_clipped_to_one_when_gain_exceeds_loss` and `test_no_flows_when_nothing_shrank` may already pass — that is expected, they guard the new guards.

- [ ] **Step 3: Rewrite `_compute_flows`**

Replace the whole function in `data_engine/transforms/transfer_matrix.py`:

```python
def _compute_flows(
    ev_results: pd.DataFrame,
    consolidator: PartyCode,
) -> dict[PartyCode, float]:
    """For each eligible source party, the fraction of its vote that moved to the
    consolidator.

    The consolidator's own gain is the transfer budget. A source party's raw
    shrinkage, (prior - actual) / prior, measures how much of its vote disappeared —
    not how much reached the consolidator. Scaling every raw shrinkage by
    consolidator_gain / total_loss makes the transferred total equal the
    consolidator's actual gain: an accounting identity, since shares sum to 100
    before and after, so total gains equal total losses. Whatever the consolidator
    did not capture was absorbed by the threat party, without needing to be
    modelled explicitly.

    Restore sits right of Reform: like Reform it is never a flow *source*, but both
    are counted in total_loss when they shrink, because they compete for the same
    freed vote.
    """
    cons_row = ev_results[ev_results["party"] == consolidator.value]
    if cons_row.empty:
        return {}
    consolidator_gain = float(cons_row.iloc[0]["actual_share"]) - float(
        cons_row.iloc[0]["prior_share"]
    )
    if consolidator_gain <= 0:
        return {}

    total_loss = 0.0
    for _, r in ev_results.iterrows():
        if PartyCode(r["party"]) == consolidator:
            continue
        total_loss += max(0.0, float(r["prior_share"]) - float(r["actual_share"]))
    if total_loss <= 0:
        return {}

    scale = max(0.0, min(1.0, consolidator_gain / total_loss))

    flows: dict[PartyCode, float] = {}
    for _, r in ev_results.iterrows():
        party = PartyCode(r["party"])
        if party in (PartyCode.REFORM, PartyCode.RESTORE) or party == consolidator:
            continue
        prior = float(r["prior_share"])
        actual = float(r["actual_share"])
        if prior <= PRIOR_SHARE_THRESHOLD:
            continue
        raw_flow = (prior - actual) / prior
        flows[party] = max(0.0, min(1.0, raw_flow * scale))
    return flows
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/data_engine/test_transfer_matrix.py -v`

Expected: all PASS.

- [ ] **Step 5: Update the downstream fixture assertions**

The tiny snapshot fixture derives its matrix through `derive_transfer_matrix`, so its expected weights shift. Replace `test_fixture_matrix_weights_are_correct` in `tests/prediction_engine/test_fixture_sanity.py`:

```python
def test_fixture_matrix_weights_are_correct(tiny_snapshot_path):
    """Verify the derived matrix matches the hand-computed flows.

    tst_eng_2025: lab gains 20.0; losses are ld 6.0 + green 5.0 + con 2.0 +
    reform 8.0 = 21.0, so scale = 20/21.
    tst_wal_2025: plaid gains 25.0; losses are lab 30.0 + ld 2.0 + green 5.0 +
    con 3.0 = 40.0, so scale = 25/40 = 0.625.
    """
    tw = _read(tiny_snapshot_path, "transfer_weights").set_index(
        ["nation", "consolidator", "source"]
    )
    eng = 20.0 / 21.0
    assert tw.loc[("england", "lab", "ld"),    "weight"] == pytest.approx(0.6 * eng, abs=1e-6)
    assert tw.loc[("england", "lab", "green"), "weight"] == pytest.approx(0.5 * eng, abs=1e-6)
    assert tw.loc[("england", "lab", "con"),   "weight"] == pytest.approx(0.4 * eng, abs=1e-6)
    assert tw.loc[("wales", "plaid", "lab"),   "weight"] == pytest.approx(0.375,     abs=1e-6)
    assert tw.loc[("wales", "plaid", "green"), "weight"] == pytest.approx(0.3125,    abs=1e-6)
    assert tw.loc[("wales", "plaid", "con"),   "weight"] == pytest.approx(0.375,     abs=1e-6)
    assert tw.loc[("wales", "plaid", "ld"),    "weight"] == pytest.approx(2/3 * 0.625, abs=1e-6)
```

- [ ] **Step 6: Bump `SCHEMA_VERSION`**

In `data_engine/snapshot.py`, change line 25:

```python
SCHEMA_VERSION = 4
```

The input hash captures inputs but not parser code, so a semantics change requires this bump to invalidate old snapshot caches. All tests reference the constant rather than a literal, so nothing else needs editing.

- [ ] **Step 7: Run the full suite**

Run: `.venv/Scripts/python.exe -m pytest -q`

Expected: all PASS. If `tests/prediction_engine/test_reform_threat.py` fails, its fixtures pass `weights` directly into `apply_flows` and are unaffected by this change — investigate rather than adjusting the expected values.

- [ ] **Step 8: Commit**

```bash
git add data_engine/transforms/transfer_matrix.py data_engine/snapshot.py \
        tests/data_engine/test_transfer_matrix.py \
        tests/prediction_engine/test_fixture_sanity.py
git commit -F - <<'EOF'
fix: scale transfer weights by the consolidator's actual gain

(prior - actual) / prior measures how much of a source party's vote
disappeared, not how much reached the consolidator. In a Reform-threat
by-election most of the collapsing Conservative vote goes to Reform, but the
old formula credited all of it to the consolidator, letting the transferred
total exceed what the consolidator actually gained.

Every raw shrinkage is now scaled by consolidator_gain / total_loss, so the
transferred total equals the consolidator's real gain. Reform and Restore
remain excluded as flow sources but their losses count toward the denominator.

SCHEMA_VERSION bumped to 4: the input hash captures inputs but not parser
code, so old snapshot caches must be invalidated.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
```

---

### Task 2: Override schema

**Files:**
- Modify: `schema/transfer_weights.py`
- Test: `tests/schema/test_transfer_weights.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `TransferWeightOverride(nation: Nation, consolidator: PartyCode, source: PartyCode, weight: float, rationale: str)`. `TransferWeightCell.n` accepts 0.

- [ ] **Step 1: Write the failing tests**

In `tests/schema/test_transfer_weights.py`, replace `test_cell_n_must_be_positive` and append the new model's tests. Add `TransferWeightOverride` to the import at the top of the file.

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/schema/test_transfer_weights.py -v`

Expected: FAIL with `ImportError: cannot import name 'TransferWeightOverride'`.

- [ ] **Step 3: Update the schema**

In `schema/transfer_weights.py`, change `TransferWeightCell.n` and append the new model:

```python
class TransferWeightCell(BaseModel):
    nation: Nation
    consolidator: PartyCode
    source: PartyCode
    weight: float = Field(ge=0.0, le=1.0)
    n: int = Field(ge=0)  # 0 == hand-curated, no supporting by-election events


class TransferWeightOverride(BaseModel):
    """A hand-curated matrix cell from data/hand_curated/transfer_overrides.yaml.

    Replaces a derived cell with the same key, or creates the cell if no
    by-election produced one. `rationale` is required: an override is a judgement
    call and the reasoning must travel with the number.
    """
    nation: Nation
    consolidator: PartyCode
    source: PartyCode
    weight: float = Field(ge=0.0, le=1.0)
    rationale: str = Field(min_length=1)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/schema/test_transfer_weights.py -v`

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add schema/transfer_weights.py tests/schema/test_transfer_weights.py
git commit -F - <<'EOF'
feat: add TransferWeightOverride schema

Hand-curated matrix cells need a model with a required rationale field -- an
override is a judgement call and the reasoning must travel with the number.

TransferWeightCell.n relaxed from gt=0 to ge=0 so a cell with no supporting
by-election events can be represented.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
```

---

### Task 3: Override loader

**Files:**
- Create: `data_engine/sources/transfer_overrides.py`
- Test: `tests/data_engine/test_transfer_overrides.py`

**Interfaces:**
- Consumes: `TransferWeightOverride` from Task 2.
- Produces: `load_transfer_overrides(yaml_path: Path) -> pd.DataFrame` with columns `nation, consolidator, source, weight, rationale`, all string-valued except `weight` (float). An absent file returns an empty DataFrame with those columns.

- [ ] **Step 1: Write the failing tests**

Create `tests/data_engine/test_transfer_overrides.py`:

```python
from pathlib import Path

import pytest
from pydantic import ValidationError

from data_engine.sources.transfer_overrides import load_transfer_overrides


_VALID = """
overrides:
  - nation: england
    consolidator: lab
    source: con
    weight: 0.2
    rationale: Most of the collapsing Conservative vote goes to Reform.
  - nation: england
    consolidator: lab
    source: green
    weight: 0.7
    rationale: Green voters consolidate readily but not universally.
"""


def _write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "transfer_overrides.yaml"
    p.write_text(text, encoding="utf-8")
    return p


def test_loads_valid_file(tmp_path):
    df = load_transfer_overrides(_write(tmp_path, _VALID))
    assert list(df.columns) == ["nation", "consolidator", "source", "weight", "rationale"]
    assert len(df) == 2
    con = df[df["source"] == "con"].iloc[0]
    assert con["nation"] == "england"
    assert con["consolidator"] == "lab"
    assert con["weight"] == pytest.approx(0.2)
    assert "Reform" in con["rationale"]


def test_missing_file_returns_empty_frame(tmp_path):
    df = load_transfer_overrides(tmp_path / "does_not_exist.yaml")
    assert df.empty
    assert list(df.columns) == ["nation", "consolidator", "source", "weight", "rationale"]


def test_empty_overrides_list_returns_empty_frame(tmp_path):
    df = load_transfer_overrides(_write(tmp_path, "overrides: []\n"))
    assert df.empty
    assert list(df.columns) == ["nation", "consolidator", "source", "weight", "rationale"]


def test_duplicate_key_raises(tmp_path):
    text = _VALID + """
  - nation: england
    consolidator: lab
    source: con
    weight: 0.9
    rationale: Contradicts the first entry.
"""
    with pytest.raises(ValueError, match="duplicate"):
        load_transfer_overrides(_write(tmp_path, text))


def test_missing_rationale_rejected(tmp_path):
    text = """
overrides:
  - nation: england
    consolidator: lab
    source: con
    weight: 0.2
"""
    with pytest.raises(ValidationError):
        load_transfer_overrides(_write(tmp_path, text))


def test_weight_out_of_range_rejected(tmp_path):
    text = """
overrides:
  - nation: england
    consolidator: lab
    source: con
    weight: 1.4
    rationale: Too big.
"""
    with pytest.raises(ValidationError):
        load_transfer_overrides(_write(tmp_path, text))


def test_unknown_nation_rejected(tmp_path):
    text = """
overrides:
  - nation: mercia
    consolidator: lab
    source: con
    weight: 0.2
    rationale: Not a nation.
"""
    with pytest.raises(ValidationError):
        load_transfer_overrides(_write(tmp_path, text))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/data_engine/test_transfer_overrides.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'data_engine.sources.transfer_overrides'`.

- [ ] **Step 3: Write the loader**

Create `data_engine/sources/transfer_overrides.py`:

```python
import logging
from pathlib import Path

import pandas as pd
import yaml
from schema.transfer_weights import TransferWeightOverride

logger = logging.getLogger(__name__)


COLUMNS = ["nation", "consolidator", "source", "weight", "rationale"]


def load_transfer_overrides(yaml_path: Path) -> pd.DataFrame:
    """Load transfer_overrides.yaml into a DataFrame with columns
    nation, consolidator, source, weight, rationale.

    Overrides are optional: a missing file returns an empty DataFrame with those
    columns rather than raising, so a checkout without the file still builds.

    A duplicate (nation, consolidator, source) key raises ValueError. Silent
    last-wins would hide a curation mistake, and these numbers are judgement
    calls that someone has to defend.
    """
    if not yaml_path.exists():
        logger.info("No transfer overrides at %s; using derived weights only", yaml_path)
        return pd.DataFrame(columns=COLUMNS)

    with yaml_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    rows: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in raw.get("overrides") or []:
        o = TransferWeightOverride.model_validate(entry)
        key = (o.nation.value, o.consolidator.value, o.source.value)
        if key in seen:
            raise ValueError(
                f"duplicate transfer override for {key} in {yaml_path}"
            )
        seen.add(key)
        rows.append({
            "nation": o.nation.value,
            "consolidator": o.consolidator.value,
            "source": o.source.value,
            "weight": o.weight,
            "rationale": o.rationale,
        })

    if not rows:
        return pd.DataFrame(columns=COLUMNS)

    logger.info("Loaded %d hand-curated transfer overrides", len(rows))
    return pd.DataFrame(rows, columns=COLUMNS)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/data_engine/test_transfer_overrides.py -v`

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add data_engine/sources/transfer_overrides.py tests/data_engine/test_transfer_overrides.py
git commit -F - <<'EOF'
feat: add transfer overrides YAML loader

Optional file -- a missing path returns an empty frame so a checkout without
it still builds. Duplicate (nation, consolidator, source) keys raise rather
than silently taking the last value, since a contradiction in curated
judgement calls should surface loudly.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
```

---

### Task 4: Apply overrides in `derive_transfer_matrix`

**Files:**
- Modify: `data_engine/transforms/transfer_matrix.py:12-66` (`derive_transfer_matrix`)
- Test: `tests/data_engine/test_transfer_matrix.py`

**Interfaces:**
- Consumes: the loader's DataFrame shape from Task 3 (columns `nation, consolidator, source, weight, rationale`).
- Produces: `derive_transfer_matrix(events, results, overrides: pd.DataFrame | None = None) -> tuple[pd.DataFrame, pd.DataFrame]`. Cells keep columns `nation, consolidator, source, weight, n`; provenance keeps `nation, consolidator, event_id`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/data_engine/test_transfer_matrix.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/data_engine/test_transfer_matrix.py -v`

Expected: the seven new tests FAIL with `TypeError: derive_transfer_matrix() got an unexpected keyword argument 'overrides'`.

- [ ] **Step 3: Add override application**

In `data_engine/transforms/transfer_matrix.py`, change the signature and the early-return, then apply overrides before returning. The `_identify_consolidator` and `_compute_flows` helpers are untouched.

```python
def derive_transfer_matrix(
    events: pd.DataFrame,
    results: pd.DataFrame,
    overrides: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Derive the reform_threat transfer matrix from by-election data.

    overrides, when given, is the frame returned by
    data_engine.sources.transfer_overrides.load_transfer_overrides. Each row
    replaces the derived cell with the same (nation, consolidator, source) key,
    or creates that cell if the data produced none. Overridden cells carry n = 0.

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

    n_events = len(eligible)
    if cell_records:
        raw = pd.DataFrame(cell_records)
        cells = (
            raw.groupby(["nation", "consolidator", "source"], as_index=False)
            .agg(weight=("weight", "mean"), n=("event_id", "nunique"))
        )
        provenance = pd.DataFrame(prov_records)
        logger.info("Derived %d matrix cells from %d eligible events", len(cells), n_events)
    else:
        cells = pd.DataFrame(columns=["nation", "consolidator", "source", "weight", "n"])
        provenance = pd.DataFrame(columns=["nation", "consolidator", "event_id"])

    cells, provenance = _apply_overrides(cells, provenance, overrides)
    return cells, provenance


def _apply_overrides(
    cells: pd.DataFrame,
    provenance: pd.DataFrame,
    overrides: pd.DataFrame | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Replace or create cells from hand-curated overrides.

    Overridden and created cells carry n = 0 to mark them as unsupported by any
    by-election. Each distinct (nation, consolidator) touched gains a single
    'hand_curated' provenance row, so a seat's matrix_provenance shows at a glance
    that curated numbers were involved.
    """
    if overrides is None or overrides.empty:
        return cells, provenance

    cells = cells.copy()
    for _, o in overrides.iterrows():
        key = (
            (cells["nation"] == o["nation"])
            & (cells["consolidator"] == o["consolidator"])
            & (cells["source"] == o["source"])
        )
        if key.any():
            cells.loc[key, "weight"] = float(o["weight"])
            cells.loc[key, "n"] = 0
        else:
            cells = pd.concat([cells, pd.DataFrame([{
                "nation": o["nation"],
                "consolidator": o["consolidator"],
                "source": o["source"],
                "weight": float(o["weight"]),
                "n": 0,
            }])], ignore_index=True)

    prov_rows = [
        {"nation": nation, "consolidator": consolidator, "event_id": "hand_curated"}
        for nation, consolidator in (
            overrides[["nation", "consolidator"]].drop_duplicates().itertuples(index=False)
        )
    ]
    provenance = pd.concat(
        [provenance, pd.DataFrame(prov_rows)], ignore_index=True
    )

    cells["n"] = cells["n"].astype(int)
    logger.info("Applied %d hand-curated overrides", len(overrides))
    return cells, provenance
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/data_engine/test_transfer_matrix.py -v`

Expected: all PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python.exe -m pytest -q`

Expected: all PASS — no caller passes `overrides` yet, so behaviour is unchanged everywhere else.

- [ ] **Step 6: Commit**

```bash
git add data_engine/transforms/transfer_matrix.py tests/data_engine/test_transfer_matrix.py
git commit -F - <<'EOF'
feat: apply hand-curated overrides to the transfer matrix

derive_transfer_matrix takes an optional overrides frame. Each row replaces the
derived cell with the same (nation, consolidator, source) key or creates it if
the data produced none, marking it n = 0. Each distinct (nation, consolidator)
touched gains one hand_curated provenance row so per-seat matrix_provenance
shows when curated numbers were involved.

The empty-derivation path now flows through the same override application
instead of returning early, so overrides work even when no event is eligible.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
```

---

### Task 5: Wire overrides into the snapshot build

**Files:**
- Modify: `data_engine/snapshot.py:28-42` (`BuildSnapshotConfig`), `:71-75` (parse block), `:145-156` (`_source_versions`)
- Modify: `data_engine/cli.py:90-96` and `:110-118`
- Test: `tests/data_engine/test_snapshot_orchestrator.py:181-183` plus two appended tests

**Interfaces:**
- Consumes: `load_transfer_overrides` from Task 3, `derive_transfer_matrix(..., overrides=...)` from Task 4.
- Produces: `BuildSnapshotConfig.transfer_overrides_yaml: Path | None = None`. `_source_versions` gains a `transfer_overrides_yaml` key.

**Background.** The override file must be hashed into `source_versions`, otherwise editing a weight would not change the input hash and `build_snapshot` would hand back the stale cached snapshot. The field defaults to `None` so existing call sites — including every test — keep working untouched.

- [ ] **Step 1: Write the failing tests**

Two edits to `tests/data_engine/test_snapshot_orchestrator.py`.

First, add the new key to the existing `source_versions` assertion (currently at lines 181-183, inside the test that builds a snapshot and reads its manifest back):

```python
    assert set(manifest.source_versions.keys()) == {
        "wikipedia_polls", "hoc_results", "byelections_yaml",
        "polls_geographies", "transfer_overrides_yaml",
    }
```

Second, append two new tests at the end of the file. They use the module's existing `primed_cache` fixture and the `_REPO_ROOT` constant defined at line 11.

```python
def test_overrides_absent_hashes_to_none(tmp_path: Path, primed_cache: RawCache):
    from data_engine.snapshot import _source_versions
    cfg = BuildSnapshotConfig(
        as_of_date=date(2026, 4, 25),
        raw_cache=primed_cache,
        out_dir=tmp_path,
        byelections_yaml=_REPO_ROOT / "data" / "hand_curated" / "by_elections.yaml",
        transfer_overrides_yaml=None,
    )
    assert _source_versions(cfg)["transfer_overrides_yaml"] == "none"


def test_editing_overrides_changes_the_input_hash(tmp_path: Path, primed_cache: RawCache):
    """A curated weight change must invalidate the cached snapshot."""
    from data_engine.snapshot import _source_versions
    ovr = tmp_path / "transfer_overrides.yaml"

    def versions(weight: float) -> dict:
        ovr.write_text(
            "overrides:\n"
            "  - nation: england\n"
            "    consolidator: lab\n"
            "    source: con\n"
            f"    weight: {weight}\n"
            "    rationale: test\n",
            encoding="utf-8",
        )
        return _source_versions(BuildSnapshotConfig(
            as_of_date=date(2026, 4, 25),
            raw_cache=primed_cache,
            out_dir=tmp_path,
            byelections_yaml=_REPO_ROOT / "data" / "hand_curated" / "by_elections.yaml",
            transfer_overrides_yaml=ovr,
        ))

    assert versions(0.2)["transfer_overrides_yaml"] != versions(0.3)["transfer_overrides_yaml"]
```

Every other test in this file constructs `BuildSnapshotConfig` without the new field; leave them alone — the default proves backward compatibility.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/data_engine/test_snapshot_orchestrator.py -v`

Expected: FAIL — the key-set assertion is missing `transfer_overrides_yaml`, and the two new tests raise `TypeError: BuildSnapshotConfig.__init__() got an unexpected keyword argument 'transfer_overrides_yaml'`.

- [ ] **Step 3: Add the config field and thread it through**

In `data_engine/snapshot.py`, add the import next to the other source imports:

```python
from data_engine.sources.transfer_overrides import load_transfer_overrides
```

Add the field to `BuildSnapshotConfig`, after `byelections_yaml`:

```python
    byelections_yaml: Path
    transfer_overrides_yaml: Path | None = None
    polls_geographies: tuple[str, ...] = ("GB",)
```

Replace the derivation call (currently line 75):

```python
    events_df, ev_results_df = load_byelections(cfg.byelections_yaml, as_of=cfg.as_of_date)
    overrides_df = (
        load_transfer_overrides(cfg.transfer_overrides_yaml)
        if cfg.transfer_overrides_yaml is not None
        else None
    )
    cells_df, provenance_df = derive_transfer_matrix(
        events_df, ev_results_df, overrides=overrides_df
    )
```

Replace `_source_versions`:

```python
def _source_versions(cfg: BuildSnapshotConfig) -> dict[str, str]:
    # NOTE: this hash captures inputs but NOT parser code. If parser semantics
    # change in a way that affects output, bump SCHEMA_VERSION to invalidate
    # all old snapshot caches.
    yaml_bytes = cfg.byelections_yaml.read_bytes()
    yaml_hash = hashlib.sha256(yaml_bytes).hexdigest()[:12]
    # The override file is an input like any other: editing a curated weight has
    # to change the hash, or build_snapshot hands back the stale cached snapshot.
    if cfg.transfer_overrides_yaml is not None and cfg.transfer_overrides_yaml.exists():
        ovr_bytes = cfg.transfer_overrides_yaml.read_bytes()
        ovr_hash = hashlib.sha256(ovr_bytes).hexdigest()[:12]
    else:
        ovr_hash = "none"
    return {
        "wikipedia_polls": cfg.as_of_date.isoformat(),
        "hoc_results": "ge_2024",
        "byelections_yaml": yaml_hash,
        "transfer_overrides_yaml": ovr_hash,
        "polls_geographies": ",".join(cfg.polls_geographies),
    }
```

- [ ] **Step 4: Point the CLI at the override file**

In `data_engine/cli.py`, in the `snapshot` command (around line 92):

```python
    cfg = BuildSnapshotConfig(
        as_of_date=as_of_date,
        raw_cache=_raw_cache(),
        out_dir=_project_root() / "data" / "snapshots",
        byelections_yaml=_project_root() / "data" / "hand_curated" / "by_elections.yaml",
        transfer_overrides_yaml=_project_root() / "data" / "hand_curated" / "transfer_overrides.yaml",
    )
```

And in `backfill` (around line 112), add the path next to `yaml_path` and pass it in the loop:

```python
    yaml_path = _project_root() / "data" / "hand_curated" / "by_elections.yaml"
    overrides_path = _project_root() / "data" / "hand_curated" / "transfer_overrides.yaml"
    while cur <= today:
        cfg = BuildSnapshotConfig(
            as_of_date=cur,
            raw_cache=cache,
            out_dir=out_dir,
            byelections_yaml=yaml_path,
            transfer_overrides_yaml=overrides_path,
        )
```

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python.exe -m pytest -q`

Expected: all PASS. `data/hand_curated/transfer_overrides.yaml` does not exist yet, so the loader returns an empty frame and the CLI's behaviour is unchanged — that is the point of this task.

- [ ] **Step 6: Commit**

```bash
git add data_engine/snapshot.py data_engine/cli.py tests/data_engine/test_snapshot_orchestrator.py
git commit -F - <<'EOF'
feat: wire transfer overrides into the snapshot build

BuildSnapshotConfig gains transfer_overrides_yaml, defaulting to None so every
existing call site keeps working. The file is hashed into source_versions --
without that, editing a curated weight would leave the input hash unchanged and
build_snapshot would hand back the stale cached snapshot.

No behaviour change yet: the override file does not exist, so the loader
returns an empty frame.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
```

---

### Task 6: The curated weights

**Files:**
- Create: `data/hand_curated/transfer_overrides.yaml`
- Test: `tests/data_engine/test_transfer_overrides.py`

**Interfaces:**
- Consumes: `load_transfer_overrides` from Task 3.
- Produces: the real override data the CLI path from Task 5 reads.

**Background.** These four weights are the analyst's judgement, not derived values. Against the corrected derivation they move `con` down (0.401 → 0.20) and `green` and `ld` up (0.423 → 0.70, 0.473 → 0.65). The `england lab ← other` cell has no derived counterpart at all — no by-election produced one — so this creates it and closes the `no_matrix_entry` gap in 143 seats.

- [ ] **Step 1: Write the failing test**

Append to `tests/data_engine/test_transfer_overrides.py`:

```python
_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_real_override_file_is_valid_and_complete():
    """The shipped file parses and covers exactly the england/lab block."""
    df = load_transfer_overrides(
        _REPO_ROOT / "data" / "hand_curated" / "transfer_overrides.yaml"
    )
    assert len(df) == 4
    assert set(df["nation"]) == {"england"}
    assert set(df["consolidator"]) == {"lab"}
    weights = dict(zip(df["source"], df["weight"]))
    assert weights == pytest.approx({"con": 0.2, "green": 0.7, "ld": 0.65, "other": 0.2})
    assert all(len(r) > 40 for r in df["rationale"])
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/data_engine/test_transfer_overrides.py::test_real_override_file_is_valid_and_complete -v`

Expected: FAIL on `assert len(df) == 4` — the file does not exist, so the loader returns an empty frame.

- [ ] **Step 3: Create the override file**

Create `data/hand_curated/transfer_overrides.yaml`:

```yaml
# Hand-curated transfer-matrix cells. Each entry replaces the weight derived from
# by-election data for that (nation, consolidator, source), or creates the cell if
# no by-election produced one. Overridden cells carry n = 0 in the snapshot and add
# a "hand_curated" row to transfer_weights_provenance.
#
# These are judgement calls, not measurements. The project has no voter-movement
# data of any kind -- every derived weight is inferred from aggregate share deltas
# across 5 by-elections and cannot separate switching from differential turnout.
# The rationale field is mandatory so the reasoning travels with the number.
#
# Scope: england/lab only. That block decides 141 of the 153 Reform-led seats and
# previously rested on a single event (makerfield_2026, n=1). Every other
# consolidator block stays derived.

overrides:
  - nation: england
    consolidator: lab
    source: con
    weight: 0.2
    rationale: >
      The derived weight conflated total Conservative vote loss with transfer to
      Labour; most of that vote goes to Reform. Conservative-to-Labour tactical
      switching to stop Reform is real but small.

  - nation: england
    consolidator: lab
    source: green
    weight: 0.7
    rationale: >
      Green voters facing a Reform-leading seat consolidate behind Labour readily,
      but less than universally.

  - nation: england
    consolidator: lab
    source: ld
    weight: 0.65
    rationale: >
      Liberal Democrat tactical switching to Labour is well attested, but the
      derived 0.94 implied near-total collapse of the LD vote into Labour.

  - nation: england
    consolidator: lab
    source: other
    weight: 0.2
    rationale: >
      "Other" is dominated by fringe parties and independents whose votes are
      belief-driven or personal-relationship-driven, and therefore move less
      readily than mainstream tactical votes. No derived cell exists for this
      pair, so this override creates it.
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/data_engine/test_transfer_overrides.py -v`

Expected: all PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python.exe -m pytest -q`

Expected: all PASS. Existing tests build their configs without `transfer_overrides_yaml`, so none of them read this file.

- [ ] **Step 6: Commit**

```bash
git add data/hand_curated/transfer_overrides.yaml tests/data_engine/test_transfer_overrides.py
git commit -F - <<'EOF'
feat: add curated england/lab transfer weights

Four judgement calls replacing the england/lab block: con 0.2, green 0.7,
ld 0.65, other 0.2. The other cell has no derived counterpart, so this creates
it and closes the no_matrix_entry gap in 143 seats.

That block decides 141 of the 153 Reform-led seats and previously rested on a
single by-election. Scope is england/lab only; every other consolidator block
stays derived.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
```

---

### Task 7: Rebuild and verify end to end

**Files:**
- No source changes. Produces `data/snapshots/*.sqlite` and `data/predictions/*.sqlite`.

**Interfaces:**
- Consumes: everything from Tasks 1-6.
- Produces: a v4 snapshot and two rebuilt predictions.

**Background.** The `SCHEMA_VERSION` bump and the new override hash both change the snapshot filename, so this produces a fresh snapshot rather than reusing `2026-07-26__v3__f83714f07627.sqlite`. Reform's tactical seat count should land well above the old 22 and below the uniform-swing 153.

- [ ] **Step 1: Run the full suite one final time**

Run: `.venv/Scripts/python.exe -m pytest -q`

Expected: all PASS. Do not proceed on a red suite.

- [ ] **Step 2: Build the new snapshot**

```bash
.venv/Scripts/seatpredict-data.exe snapshot
```

Expected: a path ending `__v4__<hash>.sqlite`, different from `2026-07-26__v3__f83714f07627.sqlite`. If it prints a `v3` name, the `SCHEMA_VERSION` bump from Task 1 was lost.

- [ ] **Step 3: Verify the matrix landed as specified**

```bash
.venv/Scripts/python.exe - <<'PY'
import sqlite3, glob, os
snap = max(glob.glob('data/snapshots/*.sqlite'), key=os.path.getmtime)
print(snap)
c = sqlite3.connect(snap)
for r in c.execute('select nation, consolidator, source, round(weight,4), n '
                   'from transfer_weights order by nation, consolidator, source'):
    print(r)
print('--- provenance ---')
for r in c.execute('select * from transfer_weights_provenance order by nation, consolidator, event_id'):
    print(r)
PY
```

Expected: `england/lab` shows con 0.2 / green 0.7 / ld 0.65 / other 0.2, all with `n = 0`. `england/green` shows con 0.497 / lab 0.296 / ld 0.359 / other 0.485 with `n = 1`. `wales/plaid` shows con 0.305 / lab 0.271 / ld 0.178 / other 0.246. `england/ld` and `scotland/green` are all below 0.02. Provenance includes `('england', 'lab', 'hand_curated')` alongside `('england', 'lab', 'makerfield_2026')`.

- [ ] **Step 4: Rebuild both predictions**

```bash
SNAP=$(ls -1t data/snapshots/*.sqlite | head -1)
.venv/Scripts/seatpredict-predict.exe run --snapshot "$SNAP" --strategy uniform_swing \
    --out-dir data/predictions --label baseline_us
.venv/Scripts/seatpredict-predict.exe run --snapshot "$SNAP" --strategy reform_threat_consolidation \
    --out-dir data/predictions --label baseline_rtc
```

- [ ] **Step 5: Report the seat totals and flag counts**

```bash
.venv/Scripts/python.exe - <<'PY'
import sqlite3, glob, os, json, collections
for tag in ('uniform_swing', 'reform_threat_consolidation'):
    f = max(glob.glob(f'data/predictions/*__{tag}__*.sqlite'), key=os.path.getmtime)
    c = sqlite3.connect(f)
    print('==', tag, os.path.basename(f))
    for r in c.execute("select party, seats from national where scope='overall' order by seats desc"):
        print('   %-7s %d' % r)
    if tag == 'reform_threat_consolidation':
        notes = collections.Counter()
        for (n,) in c.execute('select notes from seats'):
            for flag in json.loads(n):
                notes[flag] += 1
        print('   flags:', dict(notes))
PY
```

Expected: `uniform_swing` totals are unchanged from before this work (lab 233, reform 153, con 118, ld 77, other 35, snp 24, green 6, plaid 4) — the strategy never touches the matrix. `reform_threat_consolidation` shows Reform well above 22. `no_matrix_entry` should have dropped sharply or vanished, since the `england lab ← other` cell now exists.

If `uniform_swing` totals differ from those figures, something outside the matrix changed — stop and investigate before committing.

- [ ] **Step 6: Commit the rebuilt data**

```bash
git add data/snapshots data/predictions
git commit -F - <<'EOF'
data: rebuild snapshot and predictions on the corrected matrix

First v4 snapshot. Carries the gain-budget-corrected derived weights and the
four curated england/lab overrides.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
```

---

## Self-Review

**Spec coverage.**

| Spec section | Task |
|---|---|
| §1 Corrected derivation | Task 1 |
| §1 Effect on every cell (expected values) | Task 7 step 3 |
| §1 Near-zero consolidators (accepted, no code) | Task 7 step 3 verifies they stay below 0.02 |
| §1 Noise-level consolidators (recorded, out of scope) | no task — deliberate |
| §2 Override file | Task 6 |
| §3 Schema and loader | Tasks 2 and 3 |
| §4 Application and provenance | Task 4 |
| §4 `snapshot.py` wiring | Task 5 |
| §5 Downstream effects | Task 7 steps 4-5 |
| Testing — `_compute_flows` | Task 1 step 1 |
| Testing — loader | Task 3 step 1 |
| Testing — `derive_transfer_matrix` | Task 4 step 1 |
| Testing — existing test updates | Tasks 1, 2, 5 |

**Two additions the spec did not name**, both required for correctness:

1. **`SCHEMA_VERSION` bump (Task 1 step 6).** The input hash covers inputs but not parser code, so without the bump the corrected formula would silently reuse cached v3 snapshots. The requirement is documented at `data_engine/snapshot.py:146-148`.
2. **Hashing the override file into `source_versions` (Task 5 step 3).** Without it, editing a curated weight leaves the input hash unchanged and `build_snapshot` returns the stale cached snapshot.

**One spec detail corrected.** The spec says the empty-cell path "returns an empty DataFrame" early. Task 4 routes that path through `_apply_overrides` instead, so an override can stand up a block when no event is eligible — otherwise the create-mode decision would silently not apply in that case. `test_overrides_apply_even_when_no_events_are_eligible` covers it.

**Type consistency.** `derive_transfer_matrix(events, results, overrides=None)` in Task 4 matches the call in Task 5. `load_transfer_overrides(yaml_path) -> pd.DataFrame` in Task 3 matches its use in Task 5. Column names `nation, consolidator, source, weight, rationale` are identical across Tasks 3, 4 and 6. `TransferWeightOverride` field names in Task 2 match the YAML keys in Task 6.
