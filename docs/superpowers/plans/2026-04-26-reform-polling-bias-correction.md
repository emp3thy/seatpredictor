# Reform Polling Bias Correction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `reform_polling_correction_pp` parameter to both existing strategies (`uniform_swing`, `reform_threat_consolidation`) that shifts Reform's projected national share by a configurable number of percentage points. Compute the recommended value empirically from a `data/derived/reform_polling_bias.json` artifact derived from existing by-elections + a new hand-curated `local_elections.yaml`. Display the analysis in a new notebook 05 and document the local-elections curation cadence as a `/add-local-election` slash skill.

**Architecture:** Single field added to the existing `ScenarioConfig` base in `schema/prediction.py`; threaded through `compute_swing()` in `prediction_engine/polls.py` (already the single chokepoint where both strategies derive Reform's national swing); both strategies pass the field through unchanged. The bias artifact is computed by a new analysis module reading the snapshot's `polls` + `byelections_results` tables plus a new YAML, and written as JSON to `data/derived/`. No snapshot schema bump; no new strategy class; no per-seat logic changes.

**Tech Stack:** Python 3.11+, uv, Pydantic v2, pandas, Click, pytest, jupyterlab + nbformat (already dev deps), pyyaml (already a transitive dep). No new dependencies.

**Spec reference:** `docs/superpowers/specs/2026-04-26-reform-polling-bias-correction-design.md`.

**Predecessor branches & merge order:**
- This plan sits on top of `firstrunfixes` (which contains the notebook prelude required for notebook 05's CWD resilience and the deterministic `_pick_prediction` helper). `firstrunfixes` should be merged to `main` before plan execution begins.
- Once merged, branch fresh: `git checkout -b plan-c-reform-bias-correction main`.
- All work in this plan happens on `plan-c-reform-bias-correction`. Frequent commits per the per-task `git commit` steps.

**Successor plans:** None planned. Future polling-analytics work (transition matrices, drop-scenario stages, pipeline framework) was deliberately cut from this scope per the spec's §1 non-goals — those would require their own brainstorm + spec + plan if and when needed.

---

## Existing artefacts this plan consumes

These exist on `main` post-`firstrunfixes` merge and MUST NOT be modified except where listed in the per-task "Modify" lines:

| Path | Purpose |
|---|---|
| `schema/common.py` | `PartyCode`, `Nation`, `LEFT_BLOC` |
| `schema/prediction.py` | `ScenarioConfig` (base — extended), `UniformSwingConfig`, `ReformThreatConfig` (unchanged), `RunConfig` |
| `prediction_engine/polls.py` | `compute_swing` (extended with new kwarg), `ge2024_national_share` |
| `prediction_engine/projection.py` | `project_raw_shares` — unchanged; its existing renormalisation step handles the share-redistribution after Reform's swing is bumped |
| `prediction_engine/strategies/uniform_swing.py` | `UniformSwingStrategy` — adds one kwarg pass-through |
| `prediction_engine/strategies/reform_threat_consolidation.py` | `ReformThreatStrategy` — adds one kwarg pass-through |
| `prediction_engine/snapshot_loader.py` | `Snapshot` — read the polls + byelections_results tables for bias analysis |
| `prediction_engine/cli.py` | `seatpredict-predict run` and `sweep` — extended with new flag |
| `data_engine/sources/byelections.py` | Pattern for `local_elections.py` (load YAML → DataFrames) |
| `data/hand_curated/by_elections.yaml` | Existing data — used unchanged in bias analysis |
| `scripts/build_notebooks.py` | `_PRELUDE`, `_pick_prediction`, `NOTEBOOK_SPECS` — extended for notebook 05 |
| `data/snapshots/*.sqlite` | Read-only inputs at runtime |

---

## File structure produced by this plan

```
seatpredictor/
  schema/
    prediction.py                                # MODIFY: add reform_polling_correction_pp to ScenarioConfig

  data_engine/
    sources/
      local_elections.py                         # NEW: load_local_elections() loader

  prediction_engine/
    polls.py                                     # MODIFY: compute_swing accepts + applies reform_polling_correction_pp
    cli.py                                       # MODIFY: --reform-polling-correction-pp flag
    strategies/
      uniform_swing.py                           # MODIFY: pass field through to compute_swing
      reform_threat_consolidation.py             # MODIFY: pass field through to compute_swing
    analysis/
      poll_bias.py                               # NEW: compute_reform_bias(), write_bias_json()

  data/
    hand_curated/
      local_elections.yaml                       # NEW: hand-curated PNS per local-election event
    derived/                                      # NEW directory (gitkeep until populated)
      reform_polling_bias.json                   # NEW: regenerated by notebook 05 against latest snapshot

  notebooks/
    05_reform_polling_bias.ipynb                 # NEW: built by scripts/build_notebooks.py

  scripts/
    build_notebooks.py                           # MODIFY: add notebook 05 specs

  tests/
    schema/
      test_prediction.py                         # MODIFY: add tests for new field on ScenarioConfig
    data_engine/
      test_local_elections.py                    # NEW: load + validate tests
    prediction_engine/
      test_polls.py                              # MODIFY: add 2 tests for the new compute_swing kwarg
      test_uniform_swing.py                      # MODIFY: add 1 test that the correction lands in share_raw
      test_reform_threat.py                      # MODIFY: add 1 test that the correction lands in share_raw
      test_cli.py                                # MODIFY: add 1 test that --reform-polling-correction-pp is accepted
      test_poll_bias.py                          # NEW: synthetic-snapshot tests for compute_reform_bias
    fixtures/
      local_elections_sample.yaml                # NEW: tiny fixture for loader tests

  .claude/
    skills/
      add-local-election/
        SKILL.md                                 # NEW: walks me through sourcing + writing PNS each May
```

---

## Cross-task design notes (read these first)

These are referenced from multiple tasks; collected here to avoid duplication.

### N1. The new field's contract

```python
# In schema/prediction.py's ScenarioConfig (base class)
reform_polling_correction_pp: float = Field(default=0.0)
```

- **Default 0.0 is a no-op** — preserves all existing behaviour for callers that don't set it.
- **Inherited by both `UniformSwingConfig` and `ReformThreatConfig`** automatically (they already inherit from `ScenarioConfig`).
- **Backwards compatible with existing prediction files** — `RunConfig.scenario_config_json` JSONs from before this change lack the field; on re-load, the default kicks in. `extra='forbid'` rejects UNEXPECTED keys, not MISSING ones, so this is safe.
- **No range constraint.** A user might set negative (pollsters over-state Reform) or positive (under-state). A zero-magnitude check is the user's responsibility.

### N2. How the correction is applied in `compute_swing`

```python
# prediction_engine/polls.py compute_swing — added at the end of swing computation:
if reform_polling_correction_pp != 0.0:
    swing[PartyCode.REFORM] += reform_polling_correction_pp
```

This bumps Reform's swing-vs-GE2024 by `+correction_pp`. Per-seat projection then:
1. Adds Reform's swing to each seat's GE2024 Reform share.
2. Clips negative shares at 0.
3. Renormalises every seat's share to sum to 100.

The renormalisation step in `projection.py` is what redistributes the per-seat extra Reform points away from other parties proportionally. **Verify this is unchanged**: `project_raw_shares` already does `wide[f"share_raw_{p.value}"] = wide[f"_post_{p.value}"] * 100.0 / totals`.

### N3. Strategy plumbing pattern (identical for both strategies)

```python
# In each strategy's predict() method, the existing call:
gb_swing = compute_swing(
    snapshot.polls,
    snapshot.results_2024,
    as_of=snapshot.manifest.as_of_date,
    window_days=scenario.polls_window_days,
    geography="GB",
)
# Becomes:
gb_swing = compute_swing(
    snapshot.polls,
    snapshot.results_2024,
    as_of=snapshot.manifest.as_of_date,
    window_days=scenario.polls_window_days,
    geography="GB",
    reform_polling_correction_pp=scenario.reform_polling_correction_pp,
)
```

Nothing else in either strategy changes.

### N4. CLI plumbing pattern

`prediction_engine/cli.py`'s `_make_config()` already discovers fields via `STRATEGY_REGISTRY[strategy].config_schema.model_fields`. A new candidate kwarg added to its `candidates` dict is automatically picked up if the field is on the chosen config. Pattern matches the existing `multiplier`/`clarity_threshold` handling.

### N5. Bias-analysis weight conventions

Per spec §5.2 (post-revision):
```python
EVENT_WEIGHTS = {"ge": 1.0, "local_election": 1.0, "by_election": 1.0}
```
By-elections weight equally with local elections — they're treated as a behavioural turnout-validation signal.

### N6. Aggregate formula

```python
aggregate_bias_pp = sum(bias_pp * weight for events_with_polls) / sum(weight for events_with_polls)
```
Events without any pre-event polls in the 7-day window are kept in `per_event` (descriptive) but excluded from the aggregate denominator.

### N7. Pollster name normalisation

The polls table stores the pollster name as parsed from Wikipedia (e.g. `"YouGov"`, `"More in Common"`). For the per-pollster bias key, normalise via:
```python
def _normalise_pollster(name: str) -> str:
    return name.strip().lower().replace(" ", "_")
```
Documented in the `compute_reform_bias` docstring; called consistently in the JSON output.

### N8. Local-elections YAML schema

```yaml
events:
  - date: 2025-05-01
    name: "May 2025 county and unitary elections"
    pns:
      sources:
        - source: "BBC News"
          source_url: "https://www.bbc.co.uk/news/articles/...."
          shares:
            con: 15.0
            lab: 20.0
            ld: 17.0
            reform: 30.0
            green: 11.0
            other: 7.0
      consolidated:
        method: "median_across_sources"   # or "sole_source"
        shares:
          con: 15.0
          lab: 20.0
          ld: 17.0
          reform: 30.0
          green: 11.0
          other: 7.0
    notes: "Single-source seed; verify via /add-local-election skill."
```

The loader returns a list of `LocalElectionEvent` dataclasses. Plaid/SNP/Other are optional in `shares` — missing parties default to 0.0. `consolidated.shares` must sum to 100 ± 2 (loader emits warning if not).

### N9. Bias-analysis JSON output schema (canonical example)

```json
{
  "schema_version": 1,
  "generated_at_utc": "2026-04-26T15:45:00Z",
  "derived_from_snapshot_hash": "a80a24d29233",
  "derived_from_snapshot_as_of_date": "2026-04-26",
  "derived_from_local_elections_yaml_sha256": "ab12cd34efab",
  "method": {
    "description": "actual_minus_final_week_poll_mean, weighted",
    "final_week_window_days": 7,
    "geography": "GB",
    "weights": {"ge": 1.0, "local_election": 1.0, "by_election": 1.0}
  },
  "events_used": 5,
  "events_with_polls": 4,
  "aggregate": {
    "bias_pp": 1.4,
    "interpretation": "positive value: pollsters under-state Reform on average; recommended correction = +bias_pp",
    "recommended_reform_polling_correction_pp": 1.4
  },
  "per_pollster": {
    "yougov":         {"mean_bias_pp": 1.2, "n_events_with_polls": 5, "reliability": "high"},
    "more_in_common": {"mean_bias_pp": 0.4, "n_events_with_polls": 4, "reliability": "high"}
  },
  "per_event": [
    {"event_id": "may_2025_local_elections", "type": "local_election",
     "date": "2025-05-01", "actual_share_pp": 30.0, "actual_source": "median_across_sources",
     "poll_mean_share_pp": 14.0, "bias_pp": 16.0,
     "weight": 1.0, "n_polls_in_window": 12,
     "pollsters_in_window": ["yougov", "opinium", "techne", "more_in_common"]}
  ]
}
```

### N10. Notebook 05 — built via the existing pattern

`scripts/build_notebooks.py` already auto-prepends `_PRELUDE` (which `chdir`s to repo root) and provides `_pick_prediction()`. Notebook 05 follows the same `("md", _NB_05_TITLE_MD), ("code", _PRELUDE), ("code", _NB_05_LOAD), ...` pattern. Verification: `cd notebooks && uv run jupyter nbconvert --to notebook --execute 05_reform_polling_bias.ipynb --output _verify_05.ipynb` from inside `notebooks/` should succeed.

---

## Tasks

### Task 1: Add `reform_polling_correction_pp` to `ScenarioConfig`

**Files:**
- Modify: `schema/prediction.py` (add field on `ScenarioConfig` base)
- Modify: `tests/schema/test_prediction.py` (add 3 tests)

- [ ] **Step 1.1: Write the failing tests**

Append to `tests/schema/test_prediction.py`:

```python
def test_scenario_config_default_reform_polling_correction_is_zero():
    """The new field defaults to 0.0 — no-op when callers don't set it."""
    from schema.prediction import UniformSwingConfig, ReformThreatConfig
    assert UniformSwingConfig().reform_polling_correction_pp == 0.0
    assert ReformThreatConfig().reform_polling_correction_pp == 0.0


def test_scenario_config_reform_polling_correction_accepts_positive_and_negative():
    """Positive = pollsters under-state Reform; negative = pollsters over-state.
    No clamp — caller's choice."""
    from schema.prediction import UniformSwingConfig, ReformThreatConfig
    assert UniformSwingConfig(reform_polling_correction_pp=2.5).reform_polling_correction_pp == 2.5
    assert ReformThreatConfig(reform_polling_correction_pp=-1.0).reform_polling_correction_pp == -1.0


def test_scenario_config_reform_polling_correction_round_trips_through_model_dump():
    """The field appears in model_dump() so RunConfig.scenario_config_json captures it."""
    from schema.prediction import ReformThreatConfig
    cfg = ReformThreatConfig(reform_polling_correction_pp=1.5, multiplier=1.0)
    dumped = cfg.model_dump(mode="json")
    assert dumped["reform_polling_correction_pp"] == 1.5
    # Round-trip
    restored = ReformThreatConfig.model_validate(dumped)
    assert restored.reform_polling_correction_pp == 1.5
```

- [ ] **Step 1.2: Run tests to verify they fail**

```bash
.venv/Scripts/python.exe -m pytest tests/schema/test_prediction.py -v -k reform_polling_correction
```

Expected: 3 tests FAIL with `AttributeError: ... has no attribute 'reform_polling_correction_pp'` or similar.

- [ ] **Step 1.3: Implement — add the field to `ScenarioConfig`**

Edit `schema/prediction.py`. Replace this block:

```python
class ScenarioConfig(BaseModel):
    """Base for strategy-specific knobs. Subclasses add their own fields.

    extra='forbid' propagates to subclasses: if the runner re-validates a
    ReformThreatConfig against UniformSwingConfig (cross-strategy mismatch),
    the extra fields (multiplier, clarity_threshold) raise ValidationError
    instead of being silently dropped by Pydantic v2's default extra='ignore'.
    """
    model_config = ConfigDict(extra="forbid")
```

With:

```python
class ScenarioConfig(BaseModel):
    """Base for strategy-specific knobs. Subclasses add their own fields.

    extra='forbid' propagates to subclasses: if the runner re-validates a
    ReformThreatConfig against UniformSwingConfig (cross-strategy mismatch),
    the extra fields (multiplier, clarity_threshold) raise ValidationError
    instead of being silently dropped by Pydantic v2's default extra='ignore'.
    """
    model_config = ConfigDict(extra="forbid")

    # Pollster bias correction. Positive value bumps Reform's projected national
    # swing up by this many percentage points (interpretation: pollsters under-
    # state Reform). Negative value pulls Reform down. Default 0.0 is a no-op so
    # existing predictions and existing prediction files are unaffected. The
    # recommended value is computed by notebook 05 from real electoral events
    # and persisted to data/derived/reform_polling_bias.json.
    reform_polling_correction_pp: float = Field(default=0.0)
```

- [ ] **Step 1.4: Run tests to verify they pass**

```bash
.venv/Scripts/python.exe -m pytest tests/schema/test_prediction.py -v
```

Expected: ALL tests pass (existing + 3 new).

- [ ] **Step 1.5: Commit**

```bash
git add schema/prediction.py tests/schema/test_prediction.py
git commit -m "feat(schema): add reform_polling_correction_pp to ScenarioConfig

New optional field on the base ScenarioConfig (inherited by both
UniformSwingConfig and ReformThreatConfig). Default 0.0 is a no-op
preserving all existing behaviour. Positive value bumps Reform's
projected national swing; negative pulls it down. Plumbing through
compute_swing and the strategies follows in subsequent commits."
```

---

### Task 2: Wire the correction through `compute_swing()` and both strategies

**Files:**
- Modify: `prediction_engine/polls.py:57-96` (add kwarg, apply at end)
- Modify: `prediction_engine/strategies/uniform_swing.py:30-36` (pass kwarg)
- Modify: `prediction_engine/strategies/reform_threat_consolidation.py:103-109` (pass kwarg)
- Modify: `tests/prediction_engine/test_polls.py` (add 2 tests)
- Modify: `tests/prediction_engine/test_uniform_swing.py` (add 1 integration test)
- Modify: `tests/prediction_engine/test_reform_threat.py` (add 1 integration test)

- [ ] **Step 2.1: Write the failing `compute_swing` tests**

Append to `tests/prediction_engine/test_polls.py`:

```python
def test_compute_swing_default_correction_is_no_op():
    """Default reform_polling_correction_pp=0.0 produces the same swing as before."""
    polls = _polls_df_simple()
    results = _results_2024_df()
    swing_no_arg = compute_swing(polls, results, as_of=date(2026, 4, 25),
                                  window_days=14, geography="GB")
    swing_zero = compute_swing(polls, results, as_of=date(2026, 4, 25),
                                window_days=14, geography="GB",
                                reform_polling_correction_pp=0.0)
    for p in PartyCode:
        assert swing_no_arg[p] == pytest.approx(swing_zero[p])


def test_compute_swing_positive_correction_bumps_reform():
    """+2.5 correction adds 2.5 to Reform's swing; other parties unchanged."""
    polls = _polls_df_simple()
    results = _results_2024_df()
    swing_base = compute_swing(polls, results, as_of=date(2026, 4, 25),
                                window_days=14, geography="GB")
    swing_corr = compute_swing(polls, results, as_of=date(2026, 4, 25),
                                window_days=14, geography="GB",
                                reform_polling_correction_pp=2.5)
    assert swing_corr[PartyCode.REFORM] == pytest.approx(swing_base[PartyCode.REFORM] + 2.5)
    # All other parties' swings unchanged in compute_swing — the per-seat
    # renormalisation in project_raw_shares handles the redistribution.
    for p in PartyCode:
        if p is PartyCode.REFORM:
            continue
        assert swing_corr[p] == pytest.approx(swing_base[p])


def test_compute_swing_negative_correction_pulls_reform_down():
    """Negative correction subtracts from Reform's swing."""
    polls = _polls_df_simple()
    results = _results_2024_df()
    swing_base = compute_swing(polls, results, as_of=date(2026, 4, 25),
                                window_days=14, geography="GB")
    swing_corr = compute_swing(polls, results, as_of=date(2026, 4, 25),
                                window_days=14, geography="GB",
                                reform_polling_correction_pp=-1.5)
    assert swing_corr[PartyCode.REFORM] == pytest.approx(swing_base[PartyCode.REFORM] - 1.5)
```

- [ ] **Step 2.2: Run the new tests to verify they fail**

```bash
.venv/Scripts/python.exe -m pytest tests/prediction_engine/test_polls.py -v -k correction
```

Expected: 3 tests FAIL with `TypeError: compute_swing() got an unexpected keyword argument 'reform_polling_correction_pp'`.

- [ ] **Step 2.3: Implement — add the kwarg to `compute_swing`**

Edit `prediction_engine/polls.py`. Replace the function signature + body of `compute_swing` (lines 57-96) with:

```python
def compute_swing(
    polls: pd.DataFrame,
    results_2024: pd.DataFrame,
    as_of: date,
    window_days: int,
    geography: str,
    reform_polling_correction_pp: float = 0.0,
) -> dict[PartyCode, float]:
    """Average per-party poll share over the window, then subtract GE 2024 share.

    Window: published_date in (as_of - window_days, as_of].
    Failures (no polls match) raise ValueError per spec §8.
    The GE-2024 baseline is restricted to the nations that the poll geography covers
    — GB polls exclude Northern Ireland.

    reform_polling_correction_pp: optional correction added to Reform's swing AFTER
    the poll-derived swing is computed. Positive value: pollsters under-state Reform
    (so we bump the projected swing up by this many pp). Negative: pollsters
    over-state Reform. Default 0.0 is a no-op. The empirical recommended value is
    derived by notebook 05 from electoral events; users typically set it via the
    --reform-polling-correction-pp CLI flag on seatpredict-predict.
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
    window = polls.loc[filt]
    if window.empty:
        raise ValueError(
            f"no polls in window: geography={geography} as_of={as_of} window_days={window_days}"
        )

    poll_means = {p: float(window[p.value].mean()) for p in PartyCode}
    ge_shares = ge2024_national_share(results_2024, nations=_GEO_TO_NATIONS[geography])
    swing = {p: poll_means[p] - ge_shares[p] for p in PartyCode}

    if reform_polling_correction_pp != 0.0:
        swing[PartyCode.REFORM] += reform_polling_correction_pp

    logger.debug(
        "Swing computed: as_of=%s geography=%s n_polls=%d correction=%+.2f swings=%s",
        as_of, geography, len(window), reform_polling_correction_pp,
        {p.value: round(v, 2) for p, v in swing.items()},
    )
    return swing
```

- [ ] **Step 2.4: Run the polls tests to verify they pass**

```bash
.venv/Scripts/python.exe -m pytest tests/prediction_engine/test_polls.py -v
```

Expected: ALL tests pass (existing 6 + new 3 = 9 tests).

- [ ] **Step 2.5: Write the failing strategy integration tests**

Append to `tests/prediction_engine/test_uniform_swing.py`:

```python
def test_uniform_swing_honours_reform_polling_correction(tiny_snapshot_path):
    """+5pp correction lifts Reform's average share_raw across all seats by ~5pp
    (small deviation possible from per-seat clip+renormalise)."""
    from prediction_engine.snapshot_loader import Snapshot
    from prediction_engine.strategies.uniform_swing import UniformSwingStrategy
    from schema.prediction import UniformSwingConfig
    snap = Snapshot(tiny_snapshot_path)
    base = UniformSwingStrategy().predict(snap, UniformSwingConfig()).per_seat
    corr = UniformSwingStrategy().predict(
        snap, UniformSwingConfig(reform_polling_correction_pp=5.0)
    ).per_seat
    base_reform_mean = base["share_raw_reform"].mean()
    corr_reform_mean = corr["share_raw_reform"].mean()
    # +5pp correction; expect ~+4-5pp lift after renormalisation (slightly less
    # than the raw 5pp because other parties get scaled down proportionally).
    assert corr_reform_mean - base_reform_mean > 3.0
    assert corr_reform_mean - base_reform_mean < 5.5
```

Append to `tests/prediction_engine/test_reform_threat.py`:

```python
def test_reform_threat_honours_reform_polling_correction(tiny_snapshot_path):
    """+5pp correction lifts Reform's projected raw share before per-seat
    threat-consolidation logic runs. share_raw_reform mean should rise; the
    threat strategy may then flip MORE seats away from Reform (because Reform
    leads in more seats post-correction), but share_raw is what the test pins."""
    from prediction_engine.snapshot_loader import Snapshot
    from prediction_engine.strategies.reform_threat_consolidation import ReformThreatStrategy
    from schema.prediction import ReformThreatConfig
    snap = Snapshot(tiny_snapshot_path)
    base = ReformThreatStrategy().predict(snap, ReformThreatConfig()).per_seat
    corr = ReformThreatStrategy().predict(
        snap, ReformThreatConfig(reform_polling_correction_pp=5.0)
    ).per_seat
    base_reform_mean = base["share_raw_reform"].mean()
    corr_reform_mean = corr["share_raw_reform"].mean()
    assert corr_reform_mean - base_reform_mean > 3.0
    assert corr_reform_mean - base_reform_mean < 5.5
```

- [ ] **Step 2.6: Run strategy tests to verify they fail**

```bash
.venv/Scripts/python.exe -m pytest tests/prediction_engine/test_uniform_swing.py tests/prediction_engine/test_reform_threat.py -v -k correction
```

Expected: 2 tests FAIL — share_raw_reform won't change because the strategies don't pass the new field through yet.

- [ ] **Step 2.7: Implement — thread the field through both strategies**

Edit `prediction_engine/strategies/uniform_swing.py` lines 30-36. Replace:

```python
        gb_swing = compute_swing(
            snapshot.polls,
            snapshot.results_2024,
            as_of=snapshot.manifest.as_of_date,
            window_days=scenario.polls_window_days,
            geography="GB",
        )
```

With:

```python
        gb_swing = compute_swing(
            snapshot.polls,
            snapshot.results_2024,
            as_of=snapshot.manifest.as_of_date,
            window_days=scenario.polls_window_days,
            geography="GB",
            reform_polling_correction_pp=scenario.reform_polling_correction_pp,
        )
```

Edit `prediction_engine/strategies/reform_threat_consolidation.py` lines 103-109. Replace the same `compute_swing` call (same lines) with the same change — adding `reform_polling_correction_pp=scenario.reform_polling_correction_pp` as the last kwarg.

- [ ] **Step 2.8: Run all strategy tests to verify they pass**

```bash
.venv/Scripts/python.exe -m pytest tests/prediction_engine/test_uniform_swing.py tests/prediction_engine/test_reform_threat.py -v
```

Expected: all tests pass.

- [ ] **Step 2.9: Run the FULL test suite to verify nothing else regressed**

```bash
.venv/Scripts/python.exe -m pytest -v
```

Expected: all tests pass.

- [ ] **Step 2.10: Commit**

```bash
git add prediction_engine/polls.py \
        prediction_engine/strategies/uniform_swing.py \
        prediction_engine/strategies/reform_threat_consolidation.py \
        tests/prediction_engine/test_polls.py \
        tests/prediction_engine/test_uniform_swing.py \
        tests/prediction_engine/test_reform_threat.py
git commit -m "feat(prediction): apply reform_polling_correction_pp in compute_swing

compute_swing accepts a new kwarg that, when non-zero, adds the value
to Reform's swing-vs-GE2024 after the poll-derived swing is computed.
Both strategies thread scenario.reform_polling_correction_pp through.

Per-seat redistribution is handled implicitly by project_raw_shares'
existing renormalisation step: Reform's per-seat share rises, sum
exceeds 100, every party scales by 100/sum, so non-Reform parties
shed share proportionally."
```

---

### Task 3: Add `--reform-polling-correction-pp` CLI flag

**Files:**
- Modify: `prediction_engine/cli.py:27-53` (`_make_config`), :56-88 (`run_cmd`), :91-128 (`sweep_cmd`)
- Modify: `tests/prediction_engine/test_cli.py` (add 1 test)

- [ ] **Step 3.1: Write the failing test**

Append to `tests/prediction_engine/test_cli.py`:

```python
def test_run_cli_accepts_reform_polling_correction_pp(tmp_path, tiny_snapshot_path, monkeypatch):
    """`seatpredict-predict run --reform-polling-correction-pp 2.5` must:
    1. Accept the flag without erroring.
    2. Persist the value into the prediction file's scenario_config_json.
    """
    import json
    import sqlite3
    from contextlib import closing
    from click.testing import CliRunner
    from prediction_engine.cli import main
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    out_dir = tmp_path / "preds"
    out_dir.mkdir()
    result = runner.invoke(main, [
        "run",
        "--snapshot", str(tiny_snapshot_path),
        "--strategy", "uniform_swing",
        "--out-dir", str(out_dir),
        "--label", "test_corr",
        "--reform-polling-correction-pp", "2.5",
    ])
    assert result.exit_code == 0, result.output
    pred_files = list(out_dir.glob("*.sqlite"))
    assert len(pred_files) == 1
    with closing(sqlite3.connect(str(pred_files[0]))) as conn:
        row = conn.execute("SELECT scenario_config_json FROM config").fetchone()
    cfg = json.loads(row[0])
    assert cfg["reform_polling_correction_pp"] == 2.5
```

- [ ] **Step 3.2: Run the test to verify it fails**

```bash
.venv/Scripts/python.exe -m pytest tests/prediction_engine/test_cli.py -v -k reform_polling_correction
```

Expected: FAIL with `Error: No such option: --reform-polling-correction-pp` in the output.

- [ ] **Step 3.3: Implement — extend `_make_config` and both CLI commands**

Edit `prediction_engine/cli.py`. Replace `_make_config` (lines 27-53):

```python
def _make_config(
    strategy: str,
    *,
    polls_window_days: int | None,
    multiplier: float | None,
    clarity_threshold: float | None,
    reform_polling_correction_pp: float | None,
):
    """Build a ScenarioConfig for the named strategy.

    Discovers the config class via STRATEGY_REGISTRY rather than hard-coding strategy
    names. Any future strategy registered via @register is automatically supported,
    provided its config fields match a subset of the CLI's candidate kwargs (or the
    strategy's defaults cover them). The candidate kwargs map CLI flag names →
    ScenarioConfig field names; only those present on the chosen config_schema are
    forwarded, so passing --multiplier to uniform_swing won't error.
    """
    if strategy not in STRATEGY_REGISTRY:
        raise click.ClickException(f"unknown strategy: {strategy}")
    config_cls = STRATEGY_REGISTRY[strategy].config_schema
    candidates = {
        "polls_window_days": polls_window_days,
        "multiplier": multiplier,
        "clarity_threshold": clarity_threshold,
        "reform_polling_correction_pp": reform_polling_correction_pp,
    }
    fields = set(config_cls.model_fields)
    kwargs = {k: v for k, v in candidates.items() if k in fields and v is not None}
    return config_cls(**kwargs)
```

Replace `run_cmd` (lines 56-88) — add the flag declaration and the parameter:

```python
@main.command("run")
@click.option("--snapshot", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--strategy", type=str, required=True)
@click.option("--out-dir", type=click.Path(file_okay=False, path_type=Path), required=True)
@click.option("--label", type=str, default="baseline")
@click.option("--multiplier", type=float, default=None)
@click.option("--clarity-threshold", type=float, default=None)
# polls-window-days defaults to None so unspecified flag delegates to the strategy's
# config_schema default via _make_config's `v is not None` filter.
@click.option("--polls-window-days", type=int, default=None)
@click.option("--reform-polling-correction-pp", type=float, default=None,
              help="Pollster bias correction in pp (positive = pollsters under-state Reform). "
                   "Recommended value is in data/derived/reform_polling_bias.json.")
def run_cmd(
    snapshot: Path,
    strategy: str,
    out_dir: Path,
    label: str,
    multiplier: float | None,
    clarity_threshold: float | None,
    polls_window_days: int | None,
    reform_polling_correction_pp: float | None,
) -> None:
    cfg = _make_config(
        strategy,
        polls_window_days=polls_window_days,
        multiplier=multiplier,
        clarity_threshold=clarity_threshold,
        reform_polling_correction_pp=reform_polling_correction_pp,
    )
    out = run_prediction(
        snapshot_path=snapshot,
        strategy_name=strategy,
        scenario=cfg,
        out_dir=out_dir,
        label=label,
    )
    click.echo(f"Prediction at {out}")
```

Replace `sweep_cmd` (lines 91-128) — add the same flag:

```python
@main.command("sweep")
@click.option("--snapshot", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--strategy", type=str, required=True)
@click.option("--out-dir", type=click.Path(file_okay=False, path_type=Path), required=True)
@click.option("--label-prefix", type=str, default="swp")
@click.option("--multiplier", type=str, required=True, help="Comma-separated, e.g. 0.5,1.0,1.5")
# clarity-threshold and polls-window-days default to None so unspecified flags delegate
# to the strategy's config_schema defaults via _make_config's `v is not None` filter
# (matches run_cmd's pattern; future model-default changes flow through both commands
# without code changes).
@click.option("--clarity-threshold", type=float, default=None)
@click.option("--polls-window-days", type=int, default=None)
@click.option("--reform-polling-correction-pp", type=float, default=None,
              help="Constant pollster bias correction applied to every sweep point.")
def sweep_cmd(
    snapshot: Path,
    strategy: str,
    out_dir: Path,
    label_prefix: str,
    multiplier: str,
    clarity_threshold: float | None,
    polls_window_days: int | None,
    reform_polling_correction_pp: float | None,
) -> None:
    multipliers = [float(x.strip()) for x in multiplier.split(",")]
    for m in multipliers:
        cfg = _make_config(
            strategy,
            polls_window_days=polls_window_days,
            multiplier=m,
            clarity_threshold=clarity_threshold,
            reform_polling_correction_pp=reform_polling_correction_pp,
        )
        label = f"{label_prefix}_m{m:.2f}".replace(".", "p")
        out = run_prediction(
            snapshot_path=snapshot,
            strategy_name=strategy,
            scenario=cfg,
            out_dir=out_dir,
            label=label,
        )
        click.echo(f"  m={m:.2f} -> {out.name}")
```

- [ ] **Step 3.4: Run the CLI test to verify it passes**

```bash
.venv/Scripts/python.exe -m pytest tests/prediction_engine/test_cli.py -v
```

Expected: all CLI tests pass.

- [ ] **Step 3.5: Quick smoke check the binary**

```bash
.venv/Scripts/seatpredict-predict.exe run --help | grep reform-polling-correction-pp
```

Expected: line shows `--reform-polling-correction-pp FLOAT  Pollster bias correction in pp ...`.

- [ ] **Step 3.6: Commit**

```bash
git add prediction_engine/cli.py tests/prediction_engine/test_cli.py
git commit -m "feat(cli): add --reform-polling-correction-pp flag

Both 'run' and 'sweep' subcommands accept an optional pollster bias
correction in pp. Forwarded through _make_config which already filters
candidate kwargs against the chosen config_schema's fields, so the
flag is silently ignored if a future strategy doesn't declare the
field. Verified the value round-trips through the prediction file's
config.scenario_config_json."
```

---

### Task 4: Create the `/add-local-election` skill

**Files:**
- Create: `.claude/skills/add-local-election/SKILL.md`

There is no test for this task — it's a slash-skill instruction document. Verification is in Task 5 when the skill is invoked to seed the YAML.

- [ ] **Step 4.1: Create the skill directory and file**

```bash
mkdir -p .claude/skills/add-local-election
```

- [ ] **Step 4.2: Write `SKILL.md`**

Create `.claude/skills/add-local-election/SKILL.md`:

```markdown
---
description: Use when the user wants to add a new UK local-election Projected National Share (PNS) entry to data/hand_curated/local_elections.yaml. Triggers on phrases like "add local election", "new PNS entry", "May elections came in", "update local elections". Walks through sourcing PNS from BBC, Sky, Britain Elects, and Wikipedia, reconciling across sources, and appending a validated entry to the YAML.
---

# /add-local-election — append a new PNS entry to local_elections.yaml

Adds a new event to `data/hand_curated/local_elections.yaml` for the most-recent UK local elections.

## Steps

1. **Determine the date.** If the user named one (e.g. "May 2026"), use the first Thursday of that month. Otherwise ask. Confirm before proceeding.

2. **Look up PNS values from these sources, in order. Use WebFetch on each.** Record each source's per-party shares plus the URL where they were published.

   | Priority | Source | Where to look |
   |---|---|---|
   | 1 | BBC News | `https://www.bbc.co.uk/news/topics/cn4x6dw8430t` (local elections topic) — find the year's live results page; PNS is in the headline summary or "Projected national vote share" section. |
   | 2 | Sky News | `https://news.sky.com/topic/general-election-7457` (or local-election equivalent for the year) — Sky typically publishes a PNS chart on results day. |
   | 3 | Britain Elects | `https://britainelects.com/` — published spreadsheet linked from their results post; usually the most analytically careful PNS calculation. |
   | 4 | Wikipedia | `https://en.wikipedia.org/wiki/<YEAR>_United_Kingdom_local_elections` — PNS table at the top of the article. |

   For each source, extract per-party shares (con, lab, ld, reform, green, plaid, snp, other). Use 0.0 for parties not listed. Lower-case party keys per `PartyCode.value`.

3. **Reconcile across sources:**
   - If 2+ sources published, the consolidated `shares` is the per-party median across sources. Set `consolidated.method = "median_across_sources"`.
   - If only 1 source published (e.g. BBC alone), use it directly. Set `consolidated.method = "sole_source"`.
   - If sources disagree on Reform by more than 2pp, flag in the YAML's `notes` field and surface this to the user before writing — ask whether they want to proceed.

4. **Append the new event to `data/hand_curated/local_elections.yaml`.** Preserve YAML structure:

   ```yaml
   events:
     # ... existing events ...
     - date: <YYYY-MM-DD>
       name: <descriptive name, e.g. "May 2026 London borough and met district elections">
       pns:
         sources:
           - source: "BBC News"
             source_url: "<full URL where the BBC PNS was published>"
             shares: { con: ..., lab: ..., ld: ..., reform: ..., green: ..., plaid: ..., snp: ..., other: ... }
           - source: "Sky News"
             source_url: "<URL>"
             shares: { ... }
         consolidated:
           method: "median_across_sources"   # or "sole_source"
           shares: { con: ..., lab: ..., ld: ..., reform: ..., green: ..., plaid: ..., snp: ..., other: ... }
       notes: <one-line note, e.g. "BBC and Sky agree within 1pp; Reform PNS dominated by gains in mets.">
   ```

5. **Validate the YAML loads** by running:

   ```bash
   .venv/Scripts/python.exe -c "from data_engine.sources.local_elections import load_local_elections; from pathlib import Path; events = load_local_elections(Path('data/hand_curated/local_elections.yaml')); print(f'OK: {len(events)} events loaded')"
   ```

   Expected: `OK: <N> events loaded` with N being the new total. If any warning is emitted (e.g. shares don't sum to 100 ± 2), inspect the YAML and correct.

6. **Tell the user the entry was added.** Suggest re-running notebook 05 to refresh `data/derived/reform_polling_bias.json`:

   > Added <event name>. To refresh the bias artifact, re-run the last cell of notebooks/05_reform_polling_bias.ipynb (or run `nbconvert` on it from inside notebooks/).

## Pitfalls

- **PNS publication is not instant.** BBC usually publishes PNS the morning after polling day; Sky later that day; Britain Elects 1-3 days after; Wikipedia within a week. If you're invoking this on the day-of, only the BBC value may be available — use it as `sole_source` and add a note flagging that the Britain Elects value should be added later.
- **Reform's PNS may include "Reform UK" + "Reform UK aligned independents"** in some sources but not others. Accept the source's published value; do not adjust.
- **Northern Irish parties don't appear in PNS.** GB-only metric. Roll any NI residual into `other`.
- **Don't re-curate existing events without explicit user request.** This skill appends only.
```

- [ ] **Step 4.3: Verify the skill file exists**

```bash
ls -la .claude/skills/add-local-election/
```

Expected: `SKILL.md` present.

- [ ] **Step 4.4: Commit**

```bash
git add .claude/skills/add-local-election/SKILL.md
git commit -m "feat(skills): add /add-local-election for May-each-year PNS curation

Documents the source-reconciliation procedure for appending new
local-election Projected National Share entries to
data/hand_curated/local_elections.yaml. Sources in priority order:
BBC, Sky, Britain Elects, Wikipedia. Median across sources for the
consolidated value; flag if Reform sources disagree by >2pp."
```

---

### Task 5: Create `local_elections.yaml` and `data_engine/sources/local_elections.py`

**Files:**
- Create: `data/hand_curated/local_elections.yaml`
- Create: `data_engine/sources/local_elections.py`
- Create: `tests/data_engine/test_local_elections.py`
- Create: `tests/fixtures/local_elections_sample.yaml`

- [ ] **Step 5.1: Write the test fixture**

Create `tests/fixtures/local_elections_sample.yaml`:

```yaml
events:
  - date: 2025-05-01
    name: "Sample event A — two sources, median"
    pns:
      sources:
        - source: "BBC"
          source_url: "https://example.test/bbc"
          shares: { con: 16.0, lab: 22.0, ld: 18.0, reform: 28.0, green: 10.0, other: 6.0 }
        - source: "Sky"
          source_url: "https://example.test/sky"
          shares: { con: 14.0, lab: 18.0, ld: 16.0, reform: 32.0, green: 12.0, other: 8.0 }
      consolidated:
        method: "median_across_sources"
        shares: { con: 15.0, lab: 20.0, ld: 17.0, reform: 30.0, green: 11.0, other: 7.0 }
    notes: "Two-source fixture; consolidated values are the per-party median."

  - date: 2026-05-07
    name: "Sample event B — sole source"
    pns:
      sources:
        - source: "BBC"
          source_url: "https://example.test/bbc-2"
          shares: { con: 20.0, lab: 25.0, ld: 15.0, reform: 25.0, green: 8.0, other: 7.0 }
      consolidated:
        method: "sole_source"
        shares: { con: 20.0, lab: 25.0, ld: 15.0, reform: 25.0, green: 8.0, other: 7.0 }
    notes: "Sole-source fixture for the no-reconciliation path."
```

- [ ] **Step 5.2: Write the failing loader tests**

Create `tests/data_engine/test_local_elections.py`:

```python
import warnings
from datetime import date
from pathlib import Path

import pytest

from data_engine.sources.local_elections import load_local_elections, LocalElectionEvent


_FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_load_local_elections_returns_sorted_events():
    events = load_local_elections(_FIXTURES / "local_elections_sample.yaml")
    assert len(events) == 2
    assert events[0].date == date(2025, 5, 1)
    assert events[1].date == date(2026, 5, 7)
    assert all(isinstance(e, LocalElectionEvent) for e in events)


def test_load_local_elections_event_a_two_sources():
    events = load_local_elections(_FIXTURES / "local_elections_sample.yaml")
    a = events[0]
    assert a.name == "Sample event A — two sources, median"
    assert a.consolidated_method == "median_across_sources"
    assert a.consolidated_shares["reform"] == 30.0
    assert len(a.sources) == 2
    assert a.sources[0].source == "BBC"
    assert a.sources[1].shares["reform"] == 32.0


def test_load_local_elections_event_b_sole_source():
    events = load_local_elections(_FIXTURES / "local_elections_sample.yaml")
    b = events[1]
    assert b.consolidated_method == "sole_source"
    assert len(b.sources) == 1
    assert b.consolidated_shares == b.sources[0].shares


def test_load_local_elections_warns_on_bad_consolidated_sum(tmp_path):
    """Consolidated shares not summing to 100 ± 2 must emit UserWarning."""
    bad = tmp_path / "bad.yaml"
    bad.write_text("""
events:
  - date: 2025-05-01
    name: "Bad sum"
    pns:
      sources:
        - source: "X"
          source_url: "https://x.test"
          shares: { con: 30.0, lab: 30.0, reform: 30.0 }
      consolidated:
        method: "sole_source"
        shares: { con: 30.0, lab: 30.0, reform: 30.0 }
    notes: "sums to 90, not 100"
""", encoding="utf-8")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        load_local_elections(bad)
    msgs = [str(w.message) for w in caught]
    assert any("sums to 90.0" in m or "outside 98-102" in m for m in msgs), \
        f"expected sum-warning, got: {msgs}"


def test_load_local_elections_missing_file_returns_empty_list_with_warning(tmp_path):
    """Graceful: missing file -> [] + warning, NOT an exception."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = load_local_elections(tmp_path / "does_not_exist.yaml")
    assert result == []
    assert any("not found" in str(w.message).lower() for w in caught)


def test_load_local_elections_missing_optional_parties_default_to_zero():
    events = load_local_elections(_FIXTURES / "local_elections_sample.yaml")
    a = events[0]
    # snp and plaid not listed in fixture A — must default to 0.0.
    assert a.consolidated_shares.get("snp", 0.0) == 0.0
    assert a.consolidated_shares.get("plaid", 0.0) == 0.0
```

- [ ] **Step 5.3: Run loader tests to verify they fail**

```bash
.venv/Scripts/python.exe -m pytest tests/data_engine/test_local_elections.py -v
```

Expected: ALL fail with `ModuleNotFoundError: No module named 'data_engine.sources.local_elections'`.

- [ ] **Step 5.4: Implement the loader**

Create `data_engine/sources/local_elections.py`:

```python
import logging
import warnings
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LocalElectionPNSSource:
    """One published source's PNS values for a local-election event."""
    source: str
    source_url: str
    shares: dict[str, float]   # PartyCode.value -> percentage; missing parties default to 0.0


@dataclass(frozen=True)
class LocalElectionEvent:
    """One local-election event with PNS published by 1+ sources, plus a consolidated value."""
    date: date
    name: str
    sources: list[LocalElectionPNSSource]
    consolidated_shares: dict[str, float]    # PartyCode.value -> percentage
    consolidated_method: str                  # "median_across_sources" | "sole_source"
    notes: str | None


def load_local_elections(path: Path) -> list[LocalElectionEvent]:
    """Parse local_elections.yaml. Returns events sorted by date ascending.

    Behaviour for non-happy paths:
      - File missing: emit UserWarning and return []. Bias analysis is expected to
        run with by-elections only in this case.
      - Consolidated shares don't sum to 100 ± 2: emit UserWarning per event but
        continue loading (the user may have intentionally entered partial data).
      - Per-source shares missing optional parties (snp/plaid/etc.): default to 0.0.

    Hard failures (raise) are reserved for actual schema violations (missing
    'date'/'name'/'pns' keys, unparseable YAML).
    """
    if not path.exists():
        warnings.warn(
            f"local_elections.yaml not found at {path}; bias analysis will use by-elections only.",
            UserWarning,
            stacklevel=2,
        )
        return []

    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not raw or "events" not in raw:
        warnings.warn(
            f"local_elections.yaml at {path} has no 'events' key; treating as empty.",
            UserWarning,
            stacklevel=2,
        )
        return []

    events: list[LocalElectionEvent] = []
    for entry in raw["events"]:
        # Hard schema check
        for required in ("date", "name", "pns"):
            if required not in entry:
                raise ValueError(f"local-election entry missing required key '{required}': {entry}")
        if "consolidated" not in entry["pns"]:
            raise ValueError(f"local-election entry missing pns.consolidated: {entry['name']}")
        if "sources" not in entry["pns"] or not entry["pns"]["sources"]:
            raise ValueError(f"local-election entry must have at least one pns.source: {entry['name']}")

        sources = [
            LocalElectionPNSSource(
                source=s["source"],
                source_url=s["source_url"],
                shares={k: float(v) for k, v in s["shares"].items()},
            )
            for s in entry["pns"]["sources"]
        ]
        consolidated_shares = {k: float(v) for k, v in entry["pns"]["consolidated"]["shares"].items()}
        method = entry["pns"]["consolidated"]["method"]
        if method not in ("median_across_sources", "sole_source"):
            raise ValueError(f"unknown consolidated.method '{method}' in event {entry['name']}")

        # Soft check: consolidated shares should sum to 100 ± 2
        total = sum(consolidated_shares.values())
        if not (98.0 <= total <= 102.0):
            warnings.warn(
                f"event '{entry['name']}': consolidated shares sums to {total} "
                f"(outside 98-102 range); check the YAML.",
                UserWarning,
                stacklevel=2,
            )

        events.append(LocalElectionEvent(
            date=entry["date"],
            name=entry["name"],
            sources=sources,
            consolidated_shares=consolidated_shares,
            consolidated_method=method,
            notes=entry.get("notes"),
        ))

    events.sort(key=lambda e: e.date)
    logger.info("Loaded %d local-election events from %s", len(events), path)
    return events
```

- [ ] **Step 5.5: Run loader tests to verify they pass**

```bash
.venv/Scripts/python.exe -m pytest tests/data_engine/test_local_elections.py -v
```

Expected: ALL 6 tests pass.

- [ ] **Step 5.6: Use the `/add-local-election` skill (Task 4) to seed `data/hand_curated/local_elections.yaml` with the May 2025 entry**

The skill's procedure: WebFetch BBC, Sky, Britain Elects, and Wikipedia for May 2025 county/unitary elections; collate per-source shares; compute the median; write the YAML.

For reference, the May 2025 county/unitary elections were held on 2025-05-01. As a starting point if WebFetch sources are slow, here is one widely-reported headline PNS (BBC):

- Reform: 30%
- Lab: 20%
- Lib Dem: 17%
- Con: 15%
- Green: 11%
- Other: 7%

**The agent executing this step must run the skill and replace these placeholder values with whatever the live sources publish.** A single-source entry is acceptable if only one source is reachable. Final YAML structure (matching N8 in the cross-task notes):

```yaml
# UK local-election Projected National Share (PNS) per event.
# Each event records 1+ source-published PNS plus a consolidated value used by the
# bias analysis. Curate via the /add-local-election skill each May.

events:
  - date: 2025-05-01
    name: "May 2025 county and unitary elections"
    pns:
      sources:
        - source: "BBC News"
          source_url: "<URL the agent obtained via WebFetch>"
          shares: { con: 15.0, lab: 20.0, ld: 17.0, reform: 30.0, green: 11.0, other: 7.0 }
        # Add Sky, Britain Elects, Wikipedia entries here if reachable
      consolidated:
        method: "sole_source"   # or "median_across_sources" if 2+ sources are listed
        shares: { con: 15.0, lab: 20.0, ld: 17.0, reform: 30.0, green: 11.0, other: 7.0 }
    notes: "Seeded by plan-c bootstrap; verify and extend with additional sources via /add-local-election."
```

- [ ] **Step 5.7: Validate the seeded YAML loads cleanly**

```bash
.venv/Scripts/python.exe -c "from data_engine.sources.local_elections import load_local_elections; from pathlib import Path; events = load_local_elections(Path('data/hand_curated/local_elections.yaml')); print(f'OK: {len(events)} events loaded; first: {events[0].name}')"
```

Expected: `OK: 1 events loaded; first: May 2025 county and unitary elections`. No warnings emitted (consolidated shares sum to 100).

- [ ] **Step 5.8: Commit**

```bash
git add data_engine/sources/local_elections.py \
        data/hand_curated/local_elections.yaml \
        tests/data_engine/test_local_elections.py \
        tests/fixtures/local_elections_sample.yaml
git commit -m "feat(data_engine): load_local_elections + May 2025 PNS seed

Adds local_elections.yaml (mirroring by_elections.yaml's pattern)
and a frozen-dataclass loader. Schema records each PNS source with
URL plus a per-party median ('consolidated') used by downstream
bias analysis. Seeded with May 2025 county/unitary entry via the
/add-local-election skill."
```

---

### Task 6: Implement `prediction_engine/analysis/poll_bias.py`

**Files:**
- Create: `prediction_engine/analysis/poll_bias.py`
- Create: `tests/prediction_engine/test_poll_bias.py`

- [ ] **Step 6.1: Write the failing tests**

Create `tests/prediction_engine/test_poll_bias.py`:

```python
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from data_engine.sources.local_elections import LocalElectionEvent, LocalElectionPNSSource
from prediction_engine.analysis.poll_bias import (
    EVENT_WEIGHTS,
    BiasResult,
    compute_reform_bias,
    write_bias_json,
)


def _polls_df(rows):
    cols = ["pollster", "fieldwork_start", "fieldwork_end", "published_date",
            "sample_size", "geography", "con", "lab", "ld", "reform",
            "green", "snp", "plaid", "other"]
    return pd.DataFrame(rows, columns=cols)


def _byelections_results_df(rows):
    return pd.DataFrame(rows, columns=["event_id", "party", "votes", "actual_share", "prior_share"])


def _byelections_events_df(rows):
    return pd.DataFrame(rows, columns=["event_id", "name", "date", "event_type",
                                        "nation", "region", "threat_party",
                                        "exclude_from_matrix", "narrative_url"])


class _StubSnapshot:
    def __init__(self, polls, byelections_events, byelections_results):
        self.polls = polls
        self.byelections_events = byelections_events
        self.byelections_results = byelections_results

        class _M:
            content_hash = "deadbeef0123"
            as_of_date = date(2026, 4, 26)
        self.manifest = _M()

        self.snapshot_id = "test-snapshot"


def test_compute_reform_bias_single_byelection_no_local():
    """One by-election with one final-week poll: bias = actual - poll_mean."""
    snapshot = _StubSnapshot(
        polls=_polls_df([
            ("YouGov", "2025-04-25", "2025-04-27", "2025-04-28", 1500, "GB",
             20.0, 25.0, 12.0, 12.0, 8.0, 3.0, 1.0, 19.0),  # reform=12
        ]),
        byelections_events=_byelections_events_df([
            ("runcorn_helsby_2025", "Runcorn", "2025-05-01", "westminster_byelection",
             "england", "North West", "reform", False, ""),
        ]),
        byelections_results=_byelections_results_df([
            ("runcorn_helsby_2025", "reform", 12645, 38.72, 18.10),
        ]),
    )
    result = compute_reform_bias(snapshot, local_elections=None)
    assert result.n_events_used == 1
    assert result.n_events_with_polls == 1
    assert len(result.per_event) == 1
    e = result.per_event[0]
    assert e["event_id"] == "runcorn_helsby_2025"
    assert e["actual_share_pp"] == pytest.approx(38.72)
    assert e["poll_mean_share_pp"] == pytest.approx(12.0)
    assert e["bias_pp"] == pytest.approx(38.72 - 12.0)
    assert e["weight"] == 1.0
    assert result.aggregate_bias_pp == pytest.approx(38.72 - 12.0)
    assert result.recommended_reform_polling_correction_pp == pytest.approx(38.72 - 12.0)


def test_compute_reform_bias_excludes_events_without_polls_from_aggregate():
    """An event with zero polls in window stays in per_event (descriptive) but
    its bias_pp is None and it does NOT contribute to the aggregate."""
    snapshot = _StubSnapshot(
        polls=_polls_df([
            # Only event-1 has polls in window; event-2 has nothing.
            ("YouGov", "2025-04-25", "2025-04-27", "2025-04-28", 1500, "GB",
             20.0, 25.0, 12.0, 10.0, 8.0, 3.0, 1.0, 21.0),
        ]),
        byelections_events=_byelections_events_df([
            ("e1", "Event 1", "2025-05-01", "westminster_byelection",
             "england", "North West", "reform", False, ""),
            ("e2", "Event 2 (no polls)", "2024-09-15", "westminster_byelection",
             "england", "South East", "reform", False, ""),
        ]),
        byelections_results=_byelections_results_df([
            ("e1", "reform", 12645, 38.72, 18.10),
            ("e2", "reform", 5000, 22.00, 5.00),
        ]),
    )
    result = compute_reform_bias(snapshot, local_elections=None)
    assert result.n_events_used == 2
    assert result.n_events_with_polls == 1
    e2 = next(e for e in result.per_event if e["event_id"] == "e2")
    assert e2["bias_pp"] is None
    assert e2["n_polls_in_window"] == 0
    # Aggregate only uses e1
    assert result.aggregate_bias_pp == pytest.approx(38.72 - 10.0)


def test_compute_reform_bias_local_election_uses_consolidated_shares():
    """Local-election event uses pns.consolidated.shares['reform'] as actual."""
    snapshot = _StubSnapshot(
        polls=_polls_df([
            ("BBC", "2025-04-25", "2025-04-27", "2025-04-28", 1500, "GB",
             20.0, 25.0, 12.0, 14.0, 8.0, 3.0, 1.0, 17.0),
        ]),
        byelections_events=_byelections_events_df([]),
        byelections_results=_byelections_results_df([]),
    )
    local = [LocalElectionEvent(
        date=date(2025, 5, 1),
        name="May 2025",
        sources=[LocalElectionPNSSource(source="BBC", source_url="https://x", shares={"reform": 30.0})],
        consolidated_shares={"reform": 30.0},
        consolidated_method="sole_source",
        notes=None,
    )]
    result = compute_reform_bias(snapshot, local_elections=local)
    assert result.n_events_used == 1
    e = result.per_event[0]
    assert e["type"] == "local_election"
    assert e["actual_share_pp"] == 30.0
    assert e["bias_pp"] == pytest.approx(30.0 - 14.0)


def test_compute_reform_bias_per_pollster_decomposition():
    """Per-pollster bias: each pollster gets its own mean_bias_pp + n_events_with_polls."""
    snapshot = _StubSnapshot(
        polls=_polls_df([
            # Window for event on 2025-05-01: [2025-04-24, 2025-04-30]
            ("YouGov",        "2025-04-25", "2025-04-26", "2025-04-27", 1500, "GB",
             20.0, 25.0, 12.0, 11.0, 8.0, 3.0, 1.0, 20.0),
            ("More in Common","2025-04-25", "2025-04-26", "2025-04-28", 1500, "GB",
             20.0, 25.0, 12.0, 13.0, 8.0, 3.0, 1.0, 18.0),
        ]),
        byelections_events=_byelections_events_df([
            ("e1", "E1", "2025-05-01", "westminster_byelection",
             "england", "North West", "reform", False, ""),
        ]),
        byelections_results=_byelections_results_df([
            ("e1", "reform", 12000, 30.0, 18.0),
        ]),
    )
    result = compute_reform_bias(snapshot, local_elections=None)
    assert "yougov" in result.per_pollster
    assert "more_in_common" in result.per_pollster
    # YouGov polled reform=11, actual=30 → bias = +19
    assert result.per_pollster["yougov"]["mean_bias_pp"] == pytest.approx(19.0)
    # More in Common polled reform=13, actual=30 → bias = +17
    assert result.per_pollster["more_in_common"]["mean_bias_pp"] == pytest.approx(17.0)
    # Both saw 1 event → low reliability
    assert result.per_pollster["yougov"]["reliability"] == "low"
    assert result.per_pollster["yougov"]["n_events_with_polls"] == 1


def test_compute_reform_bias_returns_empty_aggregate_when_no_events_have_polls():
    """If no events have any polls in window, aggregate_bias_pp is 0.0 and
    recommended is 0.0 — explicit no-op rather than NaN."""
    snapshot = _StubSnapshot(
        polls=_polls_df([]),
        byelections_events=_byelections_events_df([
            ("e1", "E1", "2025-05-01", "westminster_byelection",
             "england", "North West", "reform", False, ""),
        ]),
        byelections_results=_byelections_results_df([
            ("e1", "reform", 12000, 30.0, 18.0),
        ]),
    )
    result = compute_reform_bias(snapshot, local_elections=None)
    assert result.n_events_used == 1
    assert result.n_events_with_polls == 0
    assert result.aggregate_bias_pp == 0.0
    assert result.recommended_reform_polling_correction_pp == 0.0


def test_write_bias_json_roundtrips(tmp_path):
    """write_bias_json produces a file that re-loads and matches the schema in N9."""
    snapshot = _StubSnapshot(
        polls=_polls_df([
            ("YouGov", "2025-04-25", "2025-04-26", "2025-04-28", 1500, "GB",
             20.0, 25.0, 12.0, 12.0, 8.0, 3.0, 1.0, 19.0),
        ]),
        byelections_events=_byelections_events_df([
            ("e1", "E1", "2025-05-01", "westminster_byelection",
             "england", "North West", "reform", False, ""),
        ]),
        byelections_results=_byelections_results_df([
            ("e1", "reform", 12000, 30.0, 18.0),
        ]),
    )
    result = compute_reform_bias(snapshot, local_elections=None)
    out = tmp_path / "bias.json"
    write_bias_json(result, snapshot, local_elections_yaml_path=None, out_path=out)
    assert out.exists()
    j = json.loads(out.read_text(encoding="utf-8"))
    assert j["schema_version"] == 1
    assert j["derived_from_snapshot_hash"] == "deadbeef0123"
    assert j["derived_from_snapshot_as_of_date"] == "2026-04-26"
    assert j["derived_from_local_elections_yaml_sha256"] is None
    assert j["method"]["weights"] == EVENT_WEIGHTS
    assert j["aggregate"]["bias_pp"] == pytest.approx(18.0)
    assert j["aggregate"]["recommended_reform_polling_correction_pp"] == pytest.approx(18.0)
    assert len(j["per_event"]) == 1
```

- [ ] **Step 6.2: Run tests to verify they fail**

```bash
.venv/Scripts/python.exe -m pytest tests/prediction_engine/test_poll_bias.py -v
```

Expected: ALL fail with `ModuleNotFoundError: No module named 'prediction_engine.analysis.poll_bias'`.

- [ ] **Step 6.3: Implement `poll_bias.py`**

Create `prediction_engine/analysis/poll_bias.py`:

```python
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from data_engine.sources.local_elections import LocalElectionEvent
from prediction_engine.snapshot_loader import Snapshot

logger = logging.getLogger(__name__)


# Per spec §5.2 (post-revision): all event types weight equally — by-elections
# are a behavioural turnout-validation signal, not noise to suppress.
EVENT_WEIGHTS: dict[str, float] = {"ge": 1.0, "local_election": 1.0, "by_election": 1.0}
FINAL_WEEK_WINDOW_DAYS: int = 7
SCHEMA_VERSION: int = 1


@dataclass
class BiasResult:
    """Output of compute_reform_bias."""
    aggregate_bias_pp: float
    recommended_reform_polling_correction_pp: float
    n_events_used: int
    n_events_with_polls: int
    per_event: list[dict] = field(default_factory=list)
    per_pollster: dict[str, dict] = field(default_factory=dict)
    method: dict = field(default_factory=dict)


def _normalise_pollster(name: str) -> str:
    """Canonical lowercase-snake-case key. Used for the per_pollster dict."""
    return name.strip().lower().replace(" ", "_")


def _final_week_polls(polls: pd.DataFrame, event_date: date,
                      window_days: int = FINAL_WEEK_WINDOW_DAYS) -> pd.DataFrame:
    """GB polls published in [event_date - window_days, event_date - 1]."""
    if polls.empty:
        return polls
    lo = (event_date - timedelta(days=window_days)).isoformat()
    hi = (event_date - timedelta(days=1)).isoformat()
    mask = (
        (polls["geography"] == "GB")
        & (polls["published_date"] >= lo)
        & (polls["published_date"] <= hi)
    )
    return polls.loc[mask]


def _event_actual_reform(by_event_id: str | None, by_results: pd.DataFrame,
                          local: LocalElectionEvent | None) -> tuple[float, str | None]:
    """Return (actual_reform_share_pp, source_descriptor) for one event."""
    if local is not None:
        share = float(local.consolidated_shares.get("reform", 0.0))
        return share, local.consolidated_method
    if by_event_id is None:
        raise ValueError("must pass either by_event_id or local")
    rows = by_results.loc[by_results["event_id"] == by_event_id]
    if rows.empty:
        raise ValueError(f"no by-election results for event_id={by_event_id}")
    reform_rows = rows.loc[rows["party"] == "reform"]
    if reform_rows.empty:
        return 0.0, None
    return float(reform_rows["actual_share"].iloc[0]), None


def compute_reform_bias(
    snapshot: Snapshot,
    local_elections: list[LocalElectionEvent] | None = None,
    *,
    weights: dict[str, float] = EVENT_WEIGHTS,
    final_week_window_days: int = FINAL_WEEK_WINDOW_DAYS,
) -> BiasResult:
    """Compute Reform polling bias by comparing pre-event national poll means
    against actual results across by-elections + local elections.

    Per-pollster keys are normalised to lowercase snake_case (e.g. "More in Common" -> "more_in_common").
    Events without any final-week polls are listed in per_event with bias_pp=None
    and excluded from the aggregate denominator. If no events have polls,
    aggregate_bias_pp = recommended = 0.0 (explicit no-op).

    Reliability for per-pollster: 'high' if n_events_with_polls >= 3 else 'low'.
    """
    polls = snapshot.polls
    by_events = snapshot.byelections_events
    by_results = snapshot.byelections_results

    per_event_rows: list[dict] = []
    per_pollster_collect: dict[str, list[float]] = {}

    # Walk by-elections
    for _, row in by_events.iterrows():
        event_id = str(row["event_id"])
        event_date = date.fromisoformat(str(row["date"]))
        actual_reform, _ = _event_actual_reform(event_id, by_results, None)
        window = _final_week_polls(polls, event_date, final_week_window_days)
        if window.empty:
            per_event_rows.append({
                "event_id": event_id, "type": "by_election",
                "date": event_date.isoformat(),
                "actual_share_pp": actual_reform, "actual_source": None,
                "poll_mean_share_pp": None, "bias_pp": None,
                "weight": weights["by_election"],
                "n_polls_in_window": 0, "pollsters_in_window": [],
            })
            continue
        poll_mean = float(window["reform"].mean())
        bias = actual_reform - poll_mean
        per_event_rows.append({
            "event_id": event_id, "type": "by_election",
            "date": event_date.isoformat(),
            "actual_share_pp": actual_reform, "actual_source": None,
            "poll_mean_share_pp": poll_mean, "bias_pp": bias,
            "weight": weights["by_election"],
            "n_polls_in_window": int(len(window)),
            "pollsters_in_window": sorted({_normalise_pollster(p) for p in window["pollster"]}),
        })
        for pollster_name in window["pollster"].unique():
            pollster_rows = window.loc[window["pollster"] == pollster_name]
            pollster_mean = float(pollster_rows["reform"].mean())
            per_pollster_collect.setdefault(_normalise_pollster(str(pollster_name)), []).append(
                actual_reform - pollster_mean
            )

    # Walk local elections
    for ev in (local_elections or []):
        event_id = f"{ev.date.isoformat().replace('-', '_')}_local"
        actual_reform, source_method = _event_actual_reform(None, by_results, ev)
        window = _final_week_polls(polls, ev.date, final_week_window_days)
        if window.empty:
            per_event_rows.append({
                "event_id": event_id, "type": "local_election",
                "date": ev.date.isoformat(),
                "actual_share_pp": actual_reform, "actual_source": source_method,
                "poll_mean_share_pp": None, "bias_pp": None,
                "weight": weights["local_election"],
                "n_polls_in_window": 0, "pollsters_in_window": [],
            })
            continue
        poll_mean = float(window["reform"].mean())
        bias = actual_reform - poll_mean
        per_event_rows.append({
            "event_id": event_id, "type": "local_election",
            "date": ev.date.isoformat(),
            "actual_share_pp": actual_reform, "actual_source": source_method,
            "poll_mean_share_pp": poll_mean, "bias_pp": bias,
            "weight": weights["local_election"],
            "n_polls_in_window": int(len(window)),
            "pollsters_in_window": sorted({_normalise_pollster(p) for p in window["pollster"]}),
        })
        for pollster_name in window["pollster"].unique():
            pollster_rows = window.loc[window["pollster"] == pollster_name]
            pollster_mean = float(pollster_rows["reform"].mean())
            per_pollster_collect.setdefault(_normalise_pollster(str(pollster_name)), []).append(
                actual_reform - pollster_mean
            )

    # Aggregate
    eligible = [(e["bias_pp"], e["weight"]) for e in per_event_rows if e["bias_pp"] is not None]
    if eligible:
        num = sum(b * w for b, w in eligible)
        den = sum(w for _, w in eligible)
        aggregate = num / den if den > 0 else 0.0
    else:
        aggregate = 0.0

    per_pollster: dict[str, dict] = {}
    for name, biases in per_pollster_collect.items():
        n = len(biases)
        per_pollster[name] = {
            "mean_bias_pp": float(sum(biases) / n),
            "n_events_with_polls": n,
            "reliability": "high" if n >= 3 else "low",
        }

    method = {
        "description": "actual_minus_final_week_poll_mean, weighted",
        "final_week_window_days": final_week_window_days,
        "geography": "GB",
        "weights": dict(weights),
    }

    n_events_used = len(per_event_rows)
    n_events_with_polls = sum(1 for e in per_event_rows if e["bias_pp"] is not None)

    logger.info(
        "Reform bias: aggregate=%.2fpp from %d/%d events; %d pollsters",
        aggregate, n_events_with_polls, n_events_used, len(per_pollster),
    )

    return BiasResult(
        aggregate_bias_pp=aggregate,
        recommended_reform_polling_correction_pp=aggregate,
        n_events_used=n_events_used,
        n_events_with_polls=n_events_with_polls,
        per_event=per_event_rows,
        per_pollster=per_pollster,
        method=method,
    )


def write_bias_json(
    result: BiasResult,
    snapshot: Snapshot,
    local_elections_yaml_path: Path | None,
    out_path: Path,
) -> Path:
    """Serialise BiasResult to JSON per spec §5.4 schema. Atomic write via tmp+rename.
    Returns out_path on success."""
    yaml_hash: str | None = None
    if local_elections_yaml_path is not None and local_elections_yaml_path.exists():
        yaml_hash = hashlib.sha256(local_elections_yaml_path.read_bytes()).hexdigest()[:12]

    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "derived_from_snapshot_hash": snapshot.manifest.content_hash,
        "derived_from_snapshot_as_of_date": snapshot.manifest.as_of_date.isoformat(),
        "derived_from_local_elections_yaml_sha256": yaml_hash,
        "method": {
            **result.method,
            "interpretation_note":
                "positive aggregate.bias_pp means pollsters under-state Reform; "
                "the recommended_reform_polling_correction_pp can be passed to "
                "seatpredict-predict via --reform-polling-correction-pp.",
        },
        "events_used": result.n_events_used,
        "events_with_polls": result.n_events_with_polls,
        "aggregate": {
            "bias_pp": result.aggregate_bias_pp,
            "interpretation": "positive value: pollsters under-state Reform on average; "
                              "recommended correction = +bias_pp",
            "recommended_reform_polling_correction_pp": result.recommended_reform_polling_correction_pp,
        },
        "per_pollster": result.per_pollster,
        "per_event": result.per_event,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(out_path)
    logger.info("Wrote bias JSON to %s (aggregate=%+.2fpp)", out_path, result.aggregate_bias_pp)
    return out_path
```

- [ ] **Step 6.4: Run tests to verify they pass**

```bash
.venv/Scripts/python.exe -m pytest tests/prediction_engine/test_poll_bias.py -v
```

Expected: ALL 6 tests pass.

- [ ] **Step 6.5: Commit**

```bash
git add prediction_engine/analysis/poll_bias.py tests/prediction_engine/test_poll_bias.py
git commit -m "feat(analysis): compute_reform_bias + write_bias_json

compute_reform_bias walks by-elections + local-election events and
compares pre-event final-week national poll means for Reform against
actual results, weighted equally. Per-pollster decomposition lower-
cases names. Events without polls are listed (descriptive) but
excluded from the aggregate. Empty input gives 0.0 (explicit no-op,
not NaN). write_bias_json atomically writes the JSON per spec §5.4."
```

---

### Task 7: Add notebook 05 to `scripts/build_notebooks.py` and regenerate

**Files:**
- Modify: `scripts/build_notebooks.py` (add `_NB_05_*` strings + entry in `NOTEBOOK_SPECS`)
- Create (via build): `notebooks/05_reform_polling_bias.ipynb`

- [ ] **Step 7.1: Edit `scripts/build_notebooks.py`**

Insert four new constants AFTER the existing `_NB_04_INTERP` line (around line 149) and BEFORE the `NOTEBOOK_SPECS` list:

```python
_NB_05_TITLE_MD = """# Reform polling bias

For every electoral event in our snapshot (by-elections + curated local-election PNS), \
compare the pre-event 7-day national poll mean for Reform against the actual Reform result. \
Aggregate to a single recommended `--reform-polling-correction-pp` value.

**Caveats:** by-elections and local elections are weighted equally (see spec §5.2). \
Per-pollster bias is shown but is descriptive — most pollsters appear in too few events for \
statistically powerful estimates."""

_NB_05_LOAD = '''from pathlib import Path
from data_engine.sources.local_elections import load_local_elections
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.analysis.poll_bias import compute_reform_bias, write_bias_json

snap_path = sorted(Path("data/snapshots").glob("*.sqlite"))[-1]
snap = Snapshot(snap_path)
local_yaml = Path("data/hand_curated/local_elections.yaml")
local_events = load_local_elections(local_yaml)
print(f"Snapshot: {snap_path.name}")
print(f"Local-election events loaded: {len(local_events)}")
print(f"By-election events in snapshot: {len(snap.byelections_events)}")'''

_NB_05_COMPUTE = '''import pandas as pd
result = compute_reform_bias(snap, local_elections=local_events)
per_event_df = pd.DataFrame(result.per_event)
per_event_df'''

_NB_05_PER_POLLSTER = '''per_pollster_df = pd.DataFrame.from_dict(result.per_pollster, orient="index")
per_pollster_df.sort_values("mean_bias_pp", ascending=False)'''

_NB_05_HEADLINE = '''print(f"Aggregate Reform polling bias: {result.aggregate_bias_pp:+.2f} pp")
print(f"Events used: {result.n_events_used} (with polls in window: {result.n_events_with_polls})")
print()
print(f"Recommended CLI flag:")
print(f"  --reform-polling-correction-pp {result.recommended_reform_polling_correction_pp:+.2f}")'''

_NB_05_WRITE = '''out_path = Path("data/derived/reform_polling_bias.json")
write_bias_json(result, snap, local_elections_yaml_path=local_yaml, out_path=out_path)
print(f"Wrote {out_path}")'''

_NB_05_INTERP = """A positive aggregate means pollsters under-state Reform; pass `+aggregate` to \
`seatpredict-predict --reform-polling-correction-pp`. Negative means pollsters over-state — pass the negative value. \
Per-pollster numbers below `n_events_with_polls = 3` are flagged `low` reliability and should be \
read as descriptive only."""
```

Then update `NOTEBOOK_SPECS` (replace the closing `]` of the list with the notebook 05 entry inserted before it):

Find the existing `NOTEBOOK_SPECS` list and replace it entirely with:

```python
NOTEBOOK_SPECS = [
    ("01_polling_trends.ipynb", [
        ("md", _NB_01_TITLE_MD),
        ("code", _PRELUDE),
        ("code", _NB_01_LOAD),
        ("code", _NB_01_PLOT),
        ("md", _NB_01_INTERP),
    ]),
    ("02_constituency_drilldown.ipynb", [
        ("md", _NB_02_TITLE_MD),
        ("code", _PRELUDE),
        ("code", _NB_02_LOAD),
        ("code", _NB_02_TABLE),
        ("md", _NB_02_INTERP),
    ]),
    ("03_strategy_comparison.ipynb", [
        ("md", _NB_03_TITLE_MD),
        ("code", _PRELUDE),
        ("code", _NB_03_LOAD),
        ("code", _NB_03_PLOT),
        ("md", _NB_03_INTERP),
    ]),
    ("04_scenario_sweep.ipynb", [
        ("md", _NB_04_TITLE_MD),
        ("code", _PRELUDE),
        ("code", _NB_04_RUN),
        ("code", _NB_04_PLOT),
        ("md", _NB_04_INTERP),
    ]),
    ("05_reform_polling_bias.ipynb", [
        ("md", _NB_05_TITLE_MD),
        ("code", _PRELUDE),
        ("code", _NB_05_LOAD),
        ("code", _NB_05_COMPUTE),
        ("code", _NB_05_PER_POLLSTER),
        ("code", _NB_05_HEADLINE),
        ("code", _NB_05_WRITE),
        ("md", _NB_05_INTERP),
    ]),
]
```

- [ ] **Step 7.2: Regenerate the notebooks**

```bash
.venv/Scripts/python.exe scripts/build_notebooks.py
```

Expected: writes 5 notebook files including `05_reform_polling_bias.ipynb`.

- [ ] **Step 7.3: Verify notebook 05 executes from inside `notebooks/` (worst-case CWD test)**

Run from the project root. The single-line `cd notebooks && ... && cd ..` keeps each step self-contained:

```bash
cd notebooks && uv run jupyter nbconvert --to notebook --execute 05_reform_polling_bias.ipynb --output _verify_05.ipynb 2>&1 | tail -3 && rm -f _verify_05.ipynb && cd ..
```

Expected: `[NbConvertApp] Writing N bytes to _verify_05.ipynb` then no further output. Any traceback = abort and inspect.

If `data/derived/reform_polling_bias.json` was written by the test execution, that's expected — Task 8 will assert it programmatically. Do NOT delete it.

- [ ] **Step 7.4: Verify all four prior notebooks STILL execute (no regression)**

Run from the project root. Each notebook is verified with a self-contained `cd / cd ..` so a failure mid-loop doesn't leave cwd in `notebooks/`:

```bash
for nb in 01_polling_trends 02_constituency_drilldown 03_strategy_comparison 04_scenario_sweep; do
  echo "--- $nb ---"
  cd notebooks && uv run jupyter nbconvert --to notebook --execute "$nb.ipynb" --output "_verify_${nb}.ipynb" 2>&1 | tail -2 && rm -f "_verify_${nb}.ipynb" && cd ..
done
```

Expected: each notebook reports `Writing N bytes`. Any failure = a regression in the build_notebooks.py edits — abort and inspect.

- [ ] **Step 7.5: Commit**

```bash
git add scripts/build_notebooks.py notebooks/05_reform_polling_bias.ipynb \
        notebooks/01_polling_trends.ipynb notebooks/02_constituency_drilldown.ipynb \
        notebooks/03_strategy_comparison.ipynb notebooks/04_scenario_sweep.ipynb
git commit -m "feat(notebooks): add 05_reform_polling_bias

Generated by scripts/build_notebooks.py. Loads the latest snapshot
+ data/hand_curated/local_elections.yaml; runs compute_reform_bias;
shows per-event diffs, per-pollster house effects, and the
recommended --reform-polling-correction-pp value; writes the JSON
artifact to data/derived/. Verified end-to-end via nbconvert from
inside notebooks/."
```

---

### Task 8: End-to-end integration check + better-memory record

**Files:**
- (No code changes; verification + observation recording only)

- [ ] **Step 8.1: Verify the JSON artifact exists and parses**

```bash
.venv/Scripts/python.exe -c "
import json
from pathlib import Path
p = Path('data/derived/reform_polling_bias.json')
assert p.exists(), 'reform_polling_bias.json missing — re-run notebook 05'
j = json.loads(p.read_text())
assert j['schema_version'] == 1
assert 'recommended_reform_polling_correction_pp' in j['aggregate']
print(f\"OK — recommended correction: {j['aggregate']['recommended_reform_polling_correction_pp']:+.2f} pp\")
print(f\"events_used: {j['events_used']}; events_with_polls: {j['events_with_polls']}\")
print(f\"derived_from_snapshot_hash: {j['derived_from_snapshot_hash']}\")
"
```

Expected: `OK — recommended correction: ±X.XX pp` plus event counts.

- [ ] **Step 8.2: Run a real prediction with the recommended correction**

Pick the recommended value from Step 8.1's output (call it `<REC>`). Then run BOTH strategies with the correction applied:

```bash
SNAP=$(ls -1t data/snapshots/*.sqlite | head -1)
.venv/Scripts/seatpredict-predict.exe run --snapshot "$SNAP" --strategy uniform_swing \
    --out-dir data/predictions --label baseline_us_corrected --reform-polling-correction-pp <REC>
.venv/Scripts/seatpredict-predict.exe run --snapshot "$SNAP" --strategy reform_threat_consolidation \
    --out-dir data/predictions --label baseline_rtc_corrected --reform-polling-correction-pp <REC>
```

Expected: each run prints `Prediction at <path>` and the new files appear in `data/predictions/`.

- [ ] **Step 8.3: Verify the correction landed in the prediction's config**

```bash
.venv/Scripts/python.exe -c "
import json, sqlite3
from pathlib import Path
from contextlib import closing
files = sorted(Path('data/predictions').glob('*baseline_us_corrected*.sqlite'))
assert files, 'no _corrected predictions found'
with closing(sqlite3.connect(str(files[-1]))) as conn:
    row = conn.execute('SELECT scenario_config_json FROM config').fetchone()
cfg = json.loads(row[0])
print('corrected uniform_swing config:', cfg)
assert cfg['reform_polling_correction_pp'] != 0.0, 'correction did not land'
print('OK — correction value =', cfg['reform_polling_correction_pp'])
"
```

Expected: prints config dict including non-zero `reform_polling_correction_pp`.

- [ ] **Step 8.4: Run the FULL test suite one final time**

```bash
.venv/Scripts/python.exe -m pytest -v
```

Expected: ALL tests pass. Report numbers in commit message.

- [ ] **Step 8.5: Record the implementation completion in better-memory**

Use the `mcp__better-memory__memory_observe` tool with:

```
content: "Plan-c (reform polling bias correction) is COMPLETE on branch plan-c-reform-bias-correction. New parameter reform_polling_correction_pp on ScenarioConfig (default 0.0) is plumbed through compute_swing into both strategies and exposed via --reform-polling-correction-pp on seatpredict-predict run/sweep. New artifact data/derived/reform_polling_bias.json computed by notebook 05 from snapshot polls + by-elections + new data/hand_curated/local_elections.yaml. New /add-local-election skill in .claude/skills/ codifies the May-each-year PNS curation procedure (BBC/Sky/Britain Elects/Wikipedia, median across sources). All event types weight 1.0 per user direction (by-elections are a behavioural turnout-validation signal). Spec at docs/superpowers/specs/2026-04-26-reform-polling-bias-correction-design.md; plan at docs/superpowers/plans/2026-04-26-reform-polling-bias-correction.md. NN/NN tests pass."
component: "prediction_engine"
theme: "milestone"
outcome: "success"
```

(Replace NN/NN with the actual test count from Step 8.4.)

- [ ] **Step 8.6: Commit completion artifacts**

```bash
git add data/derived/reform_polling_bias.json
git commit -m "data: seed reform_polling_bias.json from real snapshot

End-to-end verification: notebook 05 produced this JSON from
the latest snapshot (data/snapshots/<...>.sqlite) plus
data/hand_curated/local_elections.yaml. The recommended
correction value is what users should pass via
--reform-polling-correction-pp on seatpredict-predict.
Both strategies tested with the correction applied and the
value round-trips through the prediction file's config."
```

---

## Self-review

Spec coverage check (against `docs/superpowers/specs/2026-04-26-reform-polling-bias-correction-design.md`):

- §1 goals — covered by Tasks 1-8 collectively (param + JSON + skill + notebook).
- §2 architecture — JSON in `data/derived/`, no snapshot bump, correction applied in `compute_swing` then renormalised by existing `project_raw_shares`. Tasks 1-3.
- §3 file layout — every `Create:` and `Modify:` matches the spec's §3 table.
- §4 data inputs — Task 5 implements §4.2 YAML; Task 6 reads §4.1 polls + by-elections from snapshot.
- §5 bias computation — Task 6 (compute, weights, aggregation); §5.4 JSON schema in cross-task note N9 + asserted in Task 6 tests.
- §6 strategy integration — Tasks 1, 2, 3.
- §7 the skill — Task 4.
- §8 notebook 05 — Task 7.
- §9 module surface — Tasks 5, 6 use the exact dataclass + function signatures from §9.
- §10 acceptance criteria — every checkbox is covered by at least one task step.
- §11 non-goals — no transition matrix, no pipeline framework, no cross-tab parsing — confirmed nothing in tasks builds these.
- §12 risks — equal weighting (§12.1) is in N5 + Task 6 EVENT_WEIGHTS; pre-event window (§12.2) is `FINAL_WEEK_WINDOW_DAYS = 7`; PNS divergence (§12.3) is in the skill; pollster name normalisation (§12.4) is in N7 + Task 6's `_normalise_pollster`; missing YAML graceful path (§12.5) is in Task 5's `load_local_elections` and tested; renormalisation note (§12.6) is in N2.

Type / signature consistency:
- `LocalElectionEvent` and `LocalElectionPNSSource` defined in Task 5, imported by Task 6's tests and `compute_reform_bias`. Field names match.
- `BiasResult` defined in Task 6 with `aggregate_bias_pp`, `recommended_reform_polling_correction_pp`, `n_events_used`, `n_events_with_polls`, `per_event`, `per_pollster`, `method` — these names are referenced consistently in `write_bias_json`, the notebook 05 cells, and the Step 8.1 JSON-validation script.
- `EVENT_WEIGHTS` constant defined in Task 6, asserted by Task 6 tests, and referenced in spec N5.

Placeholder scan: no TBDs, no "implement later", no "fill in"s. The single user-action step in Task 5 (running the `/add-local-election` skill to seed the YAML) is explicit about its outputs and provides fallback values.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-26-reform-polling-bias-correction.md`. Two execution options:

**1. Subagent-Driven (recommended)** — Dispatch a fresh subagent per task with two-stage review (spec compliance + code quality) between tasks. Fast iteration, isolated context.

**2. Inline Execution** — Execute tasks in this session using `superpowers:executing-plans`, batched with checkpoints for review.

Which approach?
