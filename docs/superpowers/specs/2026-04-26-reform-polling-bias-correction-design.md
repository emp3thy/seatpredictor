# Reform Polling Bias Correction — Design Spec

**Date:** 2026-04-26
**Status:** Draft (pending user review of this written spec)
**Scope:** Single-feature addition — measure pollster bias on Reform from real electoral events, expose as a corrective parameter on both existing strategies
**Branch:** intended for `plan-c-reform-bias` once `firstrunfixes` is merged; written here while on `firstrunfixes`

---

## 1. Goals

Add a single mechanism to correct for systematic pollster mis-estimation of Reform's vote share, derived from real electoral events we have access to.

The mechanism has three parts:

1. **Empirical bias measurement.** Compare pre-event national poll means for Reform against the actual Reform result at every electoral event in our data: by-elections (already in the snapshot) and local-election Projected National Share (newly hand-curated). Aggregate to a single corrective number.

2. **Persisted artifact.** Write the result to `data/derived/reform_polling_bias.json` so it can be inspected, version-controlled alongside the snapshot it was derived from, and consumed by strategies without recomputing.

3. **Strategy parameterisation.** Add a single optional config field — `reform_polling_correction_pp: float = 0.0` — to **both** existing strategies (`uniform_swing` and `reform_threat_consolidation`). When non-zero, the strategy adds the correction to Reform's projected GB swing before per-seat projection and renormalises shares to 100. The user reads the recommended value from the bias notebook and passes it on the predict CLI; the default of `0.0` is a no-op preserving current behaviour.

Plus a small ancillary deliverable to make the data-curation cadence sustainable:

4. **A `/add-local-election` skill** that walks me through sourcing PNS from BBC / Sky / Britain Elects each May and appending to the YAML.

### What problem this solves

Pollsters have historically had a hard time with Reform — the GE 2024 result (14.3%) came in materially above most pre-election poll averages, and there's anecdotal evidence of a "shy Reform voter" effect. Both existing strategies project from polls without any calibration against actual electoral outcomes. This adds the simplest possible correction loop: measure the gap on events that have actually happened, then let the user dial it in or out as a single number.

### Explicit non-goals

The following were considered and **deliberately cut** during design:

- No transition matrix (where do Reform's lost votes go).
- No cross-tab parsing of pollster voter-flow tables.
- No regression on Δshares between consecutive polls.
- No `Stage` / `Pipeline` framework.
- No `additional_drop_pp` scenario parameter.
- No per-pollster transition matrices.

These may be added in a future spec if they prove necessary, but they are not needed to deliver the bias correction the user wants today.

---

## 2. Architecture

```
   ┌─────────────────────────────────────┐    ┌─────────────────────────────────────┐
   │  data/snapshots/<as-of>__v1__hash   │    │ data/hand_curated/                  │
   │    polls table                       │    │   by_elections.yaml (existing)      │
   │    byelections_results table         │    │   local_elections.yaml (NEW)        │
   └─────────────────────────────────────┘    └─────────────────────────────────────┘
                       │                                              │
                       └──────────────┬───────────────────────────────┘
                                      ▼
                  ┌──────────────────────────────────────┐
                  │ prediction_engine/analysis/poll_bias │
                  │   compute_reform_bias(snapshot,      │
                  │                       local_yaml)    │
                  └──────────────────────────────────────┘
                                      │
                                      ▼
                ┌──────────────────────────────────────────┐
                │ data/derived/reform_polling_bias.json    │
                │   aggregate_bias_pp                      │
                │   per_event[]                            │
                │   per_pollster{}                         │
                └──────────────────────────────────────────┘
                                      │
                                      ▼
                ┌──────────────────────────────────────────┐
                │ User reads the JSON's recommended value  │
                │ and passes it as a CLI flag to predict.  │
                └──────────────────────────────────────────┘
                                      │
              ┌───────────────────────┴────────────────────────┐
              ▼                                                ▼
   ┌──────────────────────────┐                    ┌──────────────────────────┐
   │ uniform_swing strategy   │                    │ reform_threat strategy   │
   │   adds correction to     │                    │   adds correction to     │
   │   Reform's GB swing      │                    │   Reform's GB swing      │
   │   before per-seat proj   │                    │   before per-seat proj   │
   └──────────────────────────┘                    └──────────────────────────┘
```

The correction is applied at the same point in both strategies: in the swing-computation step that already exists, between `compute_swing(...)` and `project_raw_shares(...)`. This keeps the per-seat logic of both strategies completely untouched.

### Why JSON rather than the snapshot

Same reasoning as the previous spec: avoids a `SCHEMA_VERSION` bump in v1. The JSON records `derived_from_snapshot_hash` and `derived_from_local_elections_yaml_hash` so it's reproducible. Promotion into the snapshot is a future revisit if it becomes load-bearing.

### Where the correction "comes from" in the renormalisation

When we shift Reform up by `correction_pp`, the other parties' shares must come down by the same total to keep the per-seat sum at 100. v1 takes it **proportionally** from every other party (i.e. each non-Reform party scales by `(100 - reform_new) / (100 - reform_old)`). This is the simplest defensible choice and matches what users expect from "uniform swing"-style adjustments. A more sophisticated redistribution (e.g. weighted by transition matrix) is exactly the deferred work and not needed here.

---

## 3. Project layout

```
data/
  hand_curated/
    by_elections.yaml             # existing, unchanged
    local_elections.yaml          # NEW
  derived/                         # NEW directory
    reform_polling_bias.json      # written by notebook 05

data_engine/
  sources/
    local_elections.py            # NEW: parse local_elections.yaml -> DataFrame

prediction_engine/
  analysis/
    poll_bias.py                  # NEW: compute_reform_bias()
  polls.py                        # MODIFIED: optional correction_pp param on compute_swing
  strategies/
    uniform_swing.py              # MODIFIED: pass correction through
    reform_threat_consolidation.py # MODIFIED: pass correction through
  cli.py                          # MODIFIED: --reform-polling-correction-pp flag

schema/
  prediction.py                   # MODIFIED: add field on a shared base scenario config

notebooks/
  05_reform_polling_bias.ipynb    # NEW: built by scripts/build_notebooks.py

scripts/
  build_notebooks.py              # MODIFIED: add notebook 05 spec

tests/
  data_engine/
    test_local_elections.py       # NEW
  prediction_engine/
    test_poll_bias.py             # NEW
    test_correction_in_strategies.py  # NEW: verifies both strategies honour the param
  fixtures/
    local_elections_sample.yaml   # NEW

.claude/
  skills/
    add-local-election/            # NEW skill
      SKILL.md                     # walks me through sourcing PNS each May
```

`data/hand_curated/` already exists. `data/derived/` is new — committed to git (small, deterministic, useful for downstream reproducibility).

---

## 4. Data inputs

### 4.1 By-elections — already in snapshot

`byelections_events` and `byelections_results` tables. We have 4 events (Runcorn 2025-05-01, Hamilton 2025-06-05, Caerphilly 2025-10-23, Gorton 2026-02-26). For each event we know:
- Date, constituency.
- Per-party actual share at the event.

### 4.2 Local elections — new hand-curated file

`data/hand_curated/local_elections.yaml`:

```yaml
events:
  - date: 2025-05-01
    name: "May 2025 county and unitary elections"
    pns:
      sources:
        - source: "BBC News"
          source_url: "https://www.bbc.co.uk/news/...."
          shares:
            con: 23.0
            lab: 20.0
            ld: 17.0
            reform: 30.0
            green: 5.0
            other: 5.0
        - source: "Sky News"
          source_url: "https://news.sky.com/...."
          shares:
            con: 23.0
            lab: 20.0
            ld: 18.0
            reform: 29.0
            green: 5.0
            other: 5.0
      consolidated:
        method: "median_across_sources"
        shares:
          con: 23.0
          lab: 20.0
          ld: 17.5
          reform: 29.5
          green: 5.0
          other: 5.0
    notes: "PNS published variously; sources broadly agree."
```

**Schema requirements:**
- `pns.sources[]` — one entry per published PNS source. At least one required.
- `pns.consolidated.shares` — the value that the analysis actually consumes. The skill rules in §7 govern how this is computed; we do not auto-recompute it on read.
- Party keys match the `PartyCode` enum (lower-case strings).
- Shares should sum to ~100 ± 2; loader emits warning if outside.

A Python dataclass `LocalElectionEvent` in `data_engine/sources/local_elections.py` deserialises this; `load_local_elections(path) -> list[LocalElectionEvent]` returns events sorted by date.

The YAML is loaded at notebook runtime (not baked into the snapshot in v1 — see Section 2). The bias JSON records the YAML's SHA-256 hash for reproducibility.

### 4.3 Polls — already in snapshot

Used to compute the **final-week pollster mean for Reform** for each event. Filtered by `published_date ∈ [event_date - 7d, event_date - 1d]`. Geography = "GB" for both by-elections and local-election PNS.

---

## 5. Bias computation

### 5.1 Per-event diff

For each event:

```
poll_mean_reform = mean of poll_share_reform over polls published in
                   [event_date - 7d, event_date - 1d], geography=GB

actual_reform =
    by_election:    Reform's share in that one constituency's result
    local_election: pns.consolidated.shares.reform

bias_pp = actual_reform - poll_mean_reform
```

Per-pollster decomposition for the same event:
```
For each pollster active in the window:
    pollster_poll_mean_reform = mean of that pollster's reform share in window
    pollster_bias_pp = actual_reform - pollster_poll_mean_reform
```

If no polls fall in the window for an event, record `n_polls: 0` and exclude that event from per-pollster aggregation. The event is still listed in the JSON as descriptive — it is not silently dropped.

### 5.2 Per-event weights

By-elections and local elections do not contribute equally:

| Event type | Weight | Reasoning |
|---|---|---|
| GE | (no GE events post-snapshot — N/A for v1) | — |
| Local election | 1.0 | PNS is a national-equivalent estimate; sample-frame matches the polls. |
| By-election | 0.25 | Single constituency vs national poll — conflates polling bias with sampling-frame mismatch. Down-weighted but kept (descriptive value). |

These weights are constants in v1, exposed in the JSON's `method` block so the user can see them and (later) override.

### 5.3 Aggregation

```
aggregate_bias_pp =
    sum(bias_pp * weight for each event) /
    sum(weight             for each event)
```

Per-pollster aggregate uses the same weighting:

```
per_pollster[p].mean_bias_pp =
    sum(pollster_bias_pp * weight for events where p polled in window) /
    sum(weight                     for events where p polled in window)
per_pollster[p].n_events_with_polls = count of those events
per_pollster[p].reliability = "high" if n_events_with_polls >= 3 else "low"
```

### 5.4 Persisted artifact schema

`data/derived/reform_polling_bias.json`:

```json
{
  "schema_version": 1,
  "generated_at_utc": "2026-04-26T15:45:00Z",
  "derived_from_snapshot_hash": "a80a24d29233",
  "derived_from_snapshot_as_of_date": "2026-04-26",
  "derived_from_local_elections_yaml_sha256": "ab12cd34...",
  "method": {
    "description": "actual_minus_final_week_poll_mean, weighted",
    "final_week_window_days": 7,
    "geography": "GB",
    "weights": {
      "ge": 1.0,
      "local_election": 1.0,
      "by_election": 0.25
    }
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
    "more_in_common": {"mean_bias_pp": 0.4, "n_events_with_polls": 4, "reliability": "high"},
    "techne":         {"mean_bias_pp": -0.2, "n_events_with_polls": 2, "reliability": "low"}
  },
  "per_event": [
    {"event_id": "may_2025_local_elections", "type": "local_election",
     "date": "2025-05-01", "actual_share_pp": 29.5, "actual_source": "median_across_sources",
     "poll_mean_share_pp": 14.0, "bias_pp": 15.5,
     "weight": 1.0, "n_polls_in_window": 12,
     "pollsters_in_window": ["yougov", "opinium", "techne", "more_in_common"]},
    {"event_id": "runcorn_helsby_2025", "type": "by_election",
     "date": "2025-05-01", "actual_share_pp": 38.7,
     "poll_mean_share_pp": 14.5, "bias_pp": 24.2,
     "weight": 0.25, "n_polls_in_window": 6,
     "pollsters_in_window": ["yougov", "opinium"]},
    ...
  ]
}
```

`recommended_reform_polling_correction_pp` is a deliberate convenience: it equals `aggregate.bias_pp` and is the value the notebook tells the user to copy onto the predict CLI.

---

## 6. Strategy integration

### 6.1 Config field

Add a single field to **both** strategies' scenario configs:

```python
# schema/prediction.py
class _BaseScenarioConfig(BaseModel, extra="forbid"):
    reform_polling_correction_pp: float = 0.0   # NEW

class UniformSwingConfig(_BaseScenarioConfig):
    polls_window_days: int = 14   # existing
    # ... whatever else is currently there

class ReformThreatConfig(_BaseScenarioConfig):
    multiplier: float = 1.0
    clarity_threshold: float = 5.0
    polls_window_days: int = 14
```

If both configs already inherit a base, this is one new field on the base. If they don't, it's added to both.

### 6.2 Where it's applied

In `prediction_engine/polls.py` `compute_swing()` (or equivalent):

```python
def compute_swing(polls, results_2024, *, as_of, window_days, geography,
                  reform_polling_correction_pp: float = 0.0):
    swing = ... existing computation ...
    if reform_polling_correction_pp != 0.0:
        swing[PartyCode.REFORM] += reform_polling_correction_pp
        # Other parties absorb proportionally — the per-seat projection's
        # final renormalisation step handles this. No work here.
    return swing
```

The two strategies pass through the new config field when calling `compute_swing()`. The per-seat projection's final renormalisation step (`shares = shares * 100 / sum(shares)`) handles the proportional take-down from other parties automatically.

### 6.3 CLI

`seatpredict-predict run` gains `--reform-polling-correction-pp FLOAT` (default 0.0). The flag is forwarded into the scenario config dict when constructing the strategy, regardless of which strategy is being run.

### 6.4 Run config persistence

The existing `RunConfig.scenario_config_json` already serializes the full config dict, so the new field is automatically captured in the prediction file's manifest — no extra work, but explicitly verified by a test.

---

## 7. The `/add-local-election` skill

Lives at `.claude/skills/add-local-election/SKILL.md`. Self-contained; no scripts.

The skill's prose tells me to:

1. **Confirm the date** with the user (the skill takes an optional date argument; if absent, prompts).
2. **Look up PNS values from these sources, in order:**
   - BBC News local-elections live page for that election: `https://www.bbc.co.uk/news/topics/cn4x6dw8430t` (or the per-event live page).
   - Sky News election results.
   - Britain Elects (their published spreadsheet on social media or `britainelects.com`).
   - Wikipedia `<year>_United_Kingdom_local_elections` page (PNS table at top).
3. **Reconciliation rule:**
   - Record every source found with URL + per-party shares.
   - Compute `consolidated.shares` as the **median** across sources (per party); method `"median_across_sources"`.
   - If only one source is available, use it directly; method `"sole_source"`.
   - If sources disagree on Reform by > 2pp, flag this in `notes` and prompt the user to confirm before writing.
4. **Write to YAML** by appending a new event to `data/hand_curated/local_elections.yaml` (preserving file header / events ordering).
5. **Validate** by loading the YAML through `data_engine.sources.local_elections.load_local_elections(path)` to confirm parse + schema.
6. **Suggest follow-up:** "Re-run notebook 05 to refresh `reform_polling_bias.json`".

The skill is what lets May 2027 / May 2028 etc. additions happen without me having to re-derive the procedure each time.

---

## 8. Notebook 05 — `notebooks/05_reform_polling_bias.ipynb`

Built via `scripts/build_notebooks.py` using the existing `_PRELUDE`. Cells:

| # | Type | Content |
|---|---|---|
| 1 | md | Title; explains what bias correction is and the by-election caveat. |
| 2 | code | `_PRELUDE` (auto-prepended). |
| 3 | code | Load latest snapshot + load `local_elections.yaml`; show event count summary. |
| 4 | code | Run `compute_reform_bias(snapshot, local_elections)`; display the per-event diffs table. |
| 5 | code | Bar chart: per-event `bias_pp` with weight as bar opacity. |
| 6 | code | Per-pollster bias table (low-reliability rows visually flagged). |
| 7 | code | Headline aggregate value + the recommended correction string ready to paste into the CLI. |
| 8 | code | Write `data/derived/reform_polling_bias.json`. Print the resulting path. |
| 9 | md | Interpretation + caveats (small N; positive bias = pollsters under-state Reform). |

---

## 9. Module surface (key signatures)

```python
# data_engine/sources/local_elections.py

@dataclass(frozen=True)
class LocalElectionPNSSource:
    source: str
    source_url: str
    shares: dict[str, float]    # party_value -> percentage

@dataclass(frozen=True)
class LocalElectionEvent:
    date: date
    name: str
    sources: list[LocalElectionPNSSource]
    consolidated_shares: dict[str, float]
    consolidated_method: str    # "median_across_sources" | "sole_source"
    notes: str | None

def load_local_elections(path: Path) -> list[LocalElectionEvent]:
    """Parse local_elections.yaml. Sorted by date ascending. Validates
    that consolidated.shares sums to ~100 ± 2; warns if not.
    Raises FileNotFoundError if path missing (graceful: notebook 05 reports
    'no local_elections.yaml — bias analysis runs on by-elections only')."""

# prediction_engine/analysis/poll_bias.py

@dataclass
class BiasPerEvent:
    event_id: str
    type: str                    # "local_election" | "by_election" | "ge"
    date: date
    actual_share_pp: float
    actual_source: str | None
    poll_mean_share_pp: float | None      # None if no polls in window
    bias_pp: float | None
    weight: float
    n_polls_in_window: int
    pollsters_in_window: list[str]

@dataclass
class BiasResult:
    aggregate_bias_pp: float
    recommended_reform_polling_correction_pp: float
    n_events_used: int
    n_events_with_polls: int
    per_event: list[BiasPerEvent]
    per_pollster: dict[str, dict]    # name -> {mean_bias_pp, n_events_with_polls, reliability}
    method: dict                      # the JSON method block

EVENT_WEIGHTS = {"ge": 1.0, "local_election": 1.0, "by_election": 0.25}
FINAL_WEEK_WINDOW_DAYS = 7

def compute_reform_bias(snapshot: Snapshot,
                         local_elections: list[LocalElectionEvent] | None = None,
                         weights: dict[str, float] = EVENT_WEIGHTS,
                         final_week_window_days: int = FINAL_WEEK_WINDOW_DAYS,
                         ) -> BiasResult: ...

def write_bias_json(result: BiasResult, snapshot: Snapshot,
                     local_elections_yaml_path: Path | None,
                     out_path: Path) -> Path: ...

# prediction_engine/polls.py — modified

def compute_swing(polls, results_2024, *, as_of, window_days, geography,
                   reform_polling_correction_pp: float = 0.0,
                   ) -> dict[PartyCode, float]: ...
```

---

## 10. Acceptance criteria

For this spec to be considered done:

- [ ] `data/hand_curated/local_elections.yaml` exists with at least the May 2025 entry, sourced via the `/add-local-election` skill.
- [ ] `data_engine/sources/local_elections.py` exists with `load_local_elections()` per §9.
- [ ] `prediction_engine/analysis/poll_bias.py` exists with `compute_reform_bias()` and `write_bias_json()` per §9.
- [ ] `schema/prediction.py` adds `reform_polling_correction_pp: float = 0.0` to both `UniformSwingConfig` and `ReformThreatConfig` (via shared base if one exists).
- [ ] `prediction_engine/polls.py` `compute_swing()` accepts and applies `reform_polling_correction_pp`.
- [ ] Both strategies (`uniform_swing.py`, `reform_threat_consolidation.py`) thread the new config field through to `compute_swing()`.
- [ ] `seatpredict-predict run` accepts `--reform-polling-correction-pp FLOAT` and forwards it.
- [ ] `notebooks/05_reform_polling_bias.ipynb` exists, generated by `scripts/build_notebooks.py`, and executes end-to-end via `nbconvert` from inside `notebooks/`.
- [ ] Notebook 05's last code cell writes a valid `data/derived/reform_polling_bias.json` referenced in §5.4.
- [ ] `.claude/skills/add-local-election/SKILL.md` exists and matches the §7 procedure.
- [ ] Unit tests:
  - `test_local_elections.py` — loads fixture YAML, asserts schema + warnings on bad sums.
  - `test_poll_bias.py` — synthetic snapshot with known events, assert per-event bias values + aggregate.
  - `test_correction_in_strategies.py` — runs both strategies once at `correction = 0.0` and once at `correction = +2.0`, asserts Reform's projected GB swing is exactly 2pp higher in the latter, and that other parties' shares correspondingly drop in the renormalised per-seat output.
- [ ] `RunConfig.scenario_config_json` round-trips the new field (asserted by a test).
- [ ] better-memory updated with: artifact path, consumer contract for the new config field, and the local-elections data convention.

---

## 11. Out of scope (explicit non-goals — same as §1, restated for grep-ability)

- No transition matrix.
- No cross-tab parsing.
- No regression on Δshares between consecutive polls.
- No `Stage` / `Pipeline` framework.
- No `additional_drop_pp` parameter.
- No promotion of either YAML or JSON into the snapshot schema (deferred to a possible v2).
- No automated fetcher for local-election PNS — the skill is the maintenance loop.
- No back-fill of historical bias values (always uses latest snapshot).
- No scope to revise existing strategies' per-seat logic.

---

## 12. Risks & open questions

### 12.1 By-election weight is a judgement call

`0.25` is a guess. Lower = bias is dominated by local elections (we have only 1-2 of those); higher = by-election noise leaks into the aggregate. The weight is exposed in the JSON `method` block and easily revisitable. v1 leaves it at 0.25; if results look obviously dominated by one event type, it can be tuned.

### 12.2 Pre-event poll window

`final_week_window_days = 7` is conventional but arbitrary. Some events have very few polls in 7 days; widening to 14 days adds signal but stretches "final-week" semantically. Default 7; configurable in code. The notebook could expose a slider for sensitivity, deferred to v2 unless asked.

### 12.3 PNS sources occasionally diverge

If BBC and Sky publish PNS values that disagree by >2pp, the skill prompts before writing. The user has the call. If this becomes routine we add `divergence_flag` propagation into the bias JSON.

### 12.4 Pollster name normalisation

The polls table and the per-pollster bias decomposition need to use the same names. The polls table stores the names parsed from Wikipedia (capitalised). v1 lower-cases for matching; expects `pollster.lower().replace(" ", "_")` as the canonical key. Documented in `compute_reform_bias` docstring and tested.

### 12.5 What if `local_elections.yaml` is missing entirely

The notebook should run with `local_elections=None` and emit a markdown warning "no local-election data loaded — aggregate uses by-elections only (down-weighted; expect noisy result)." `compute_reform_bias` is designed for `local_elections=None`. v1 ships with the May 2025 entry seeded so this path is the fallback, not the norm.

### 12.6 Sum check on per-seat renormalisation

Adding 2pp to Reform and renormalising across other parties means each non-Reform party multiplies by `(100 - reform_new) / (100 - reform_old)`. In a seat where a non-Reform party is at, say, 40%, that's a meaningful absolute drop. The user should understand this is "uniform proportional take-down". Documented in §6.3 and visible in the notebook 05 worked example.
