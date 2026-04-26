# Polling Analytics — Design Spec

**Date:** 2026-04-26
**Status:** Draft (pending user review of this written spec)
**Scope:** Analysis-layer addition — produces persisted artifacts consumed by future strategy work
**Branch:** intended for `plan-c-polling-analytics` once `firstrunfixes` is merged; written here while on `firstrunfixes`

---

## 1. Goals

Two separate but related analyses of the polls + election-result data already in our snapshots, both producing JSON artifacts that downstream strategy work will consume.

1. **Poll-transition matrix (A.1).** Estimate "for each 1pp change in Reform's national headline support, how does the share of every other party simultaneously move?" Two methods, cross-validated:
   - **Regression** on the polls table we already ingest.
   - **Cross-tabs** (voter-flow tables) from individual pollsters, parsed from manually-curated downloaded files.
2. **Reform polling bias (A.2).** Quantify whether published polls systematically over- or under-state Reform's actual vote share, using the validation events we have (GE 2024 + 4 by-elections), broken down per pollster (house effects) and aggregated.

Both outputs persist to `data/derived/` as JSON, with provenance, so a later spec can:
- Add a `apply_reform_bias_correction` stage that auto-loads the bias JSON, shifts Reform's projected national share, and redistributes the shift to other parties using the transition matrix.
- Add a `apply_drop_scenario` stage that subtracts a configurable hypothetical drop from Reform and redistributes via the same matrix.
- Compose those stages with the existing `reform_threat_consolidation` per-seat logic into a pipeline.

The strategy work itself is **explicitly out of scope** for this spec — see Section 11.

### What problem this solves

The existing `reform_threat_consolidation` strategy operates on raw projected shares from observed polls. It cannot answer "what if Reform drops further" or "what if pollsters are systematically under-counting Reform" — both of which the user has identified as scenarios they want to explore. This spec produces the empirical inputs those scenarios need.

---

## 2. Architecture

```
                              ┌──────────────────────────────────────────────────┐
                              │   data/snapshots/<as-of>__v1__<hash>.sqlite      │
                              │     polls table (all polls since GE 2024)        │
                              │     results_2024 table                            │
                              │     byelections_events + byelections_results     │
                              └──────────────────────────────────────────────────┘
                                             │
              ┌──────────────────────────────┼──────────────────────────────┐
              ▼                              ▼                              ▼
   ┌────────────────────────┐    ┌────────────────────────┐    ┌────────────────────────┐
   │ A.1 regression         │    │ A.1 cross-tab parsers  │    │ A.2 bias analysis      │
   │ (paired-difference     │    │ (manual files in       │    │ (per-event &           │
   │  per pollster, OLS,    │    │  data/hand_curated/    │    │  per-pollster diffs    │
   │  bootstrap CIs)        │    │  pollster_tables/)     │    │  vs actuals)           │
   └────────────────────────┘    └────────────────────────┘    └────────────────────────┘
              │                              │                              │
              └──────────────────────────────┘                              │
                              ▼                                             ▼
            ┌────────────────────────────────────┐         ┌────────────────────────────────┐
            │ data/derived/                      │         │ data/derived/                  │
            │   poll_transition_matrix.json      │         │   reform_polling_bias.json     │
            └────────────────────────────────────┘         └────────────────────────────────┘
                              │                                             │
                              └──────────────────┬──────────────────────────┘
                                                 ▼
                                ┌──────────────────────────────────┐
                                │  notebooks/05_poll_analytics.ipynb│
                                │  builds + displays both          │
                                └──────────────────────────────────┘

                                         (consumed by Spec B; out of scope here)
```

### Why JSON in `data/derived/` (not in the snapshot)

The snapshot schema is locked at v1. Promoting these artifacts into the snapshot requires a `SCHEMA_VERSION` bump and migration plan, which is appropriate once the analysis is proven. For the first version, JSON files are produced **on-demand** by re-running the notebook (or a future CLI), with their provenance fields recording which snapshot they were derived from. A consuming strategy fails loud if the JSON is missing or its `derived_from_snapshot_hash` doesn't match the snapshot it's running against.

This decision is revisitable — Section 11.3.

---

## 3. Project layout

New files only. Existing modules untouched.

```
prediction_engine/
  analysis/
    poll_transitions.py            # NEW: regression module (Approach A.1)
    poll_bias.py                   # NEW: bias analysis module (A.2)
    pollster_tables/               # NEW package: cross-tab parsers
      __init__.py                  # registry: pollster_name -> parser_fn
      common.py                    # shared schema + validation helpers
      techne.py                    # parse_techne(path) -> DataFrame
      find_out_now.py
      more_in_common.py
      opinium.py                   # added on demand by iterative loop
      bmg.py                       # added on demand
      yougov.py                    # added on demand (PDF, harder)

data/
  hand_curated/
    pollster_tables/               # NEW: manually-saved pollster tables
      <pollster>/<YYYY-MM-DD>.{csv,xlsx,html,pdf}
      <pollster>/<YYYY-MM-DD>.meta.json
  derived/                          # NEW: regenerable analysis outputs
    poll_transition_matrix.json    # written by notebook 05
    reform_polling_bias.json       # written by notebook 05

notebooks/
  05_poll_analytics.ipynb          # NEW: built by scripts/build_notebooks.py

tests/
  prediction_engine/
    test_poll_transitions.py        # NEW
    test_poll_bias.py               # NEW
    pollster_tables/
      test_techne.py                # NEW: per-pollster parser tests
      test_aggregate.py             # NEW: iterative-value-test
  fixtures/
    pollster_tables/
      techne_2026-04-15.xlsx        # NEW: tiny synthetic fixtures
      ...

scripts/
  build_notebooks.py                # MODIFIED: add notebook 05 spec
```

Naming follows existing convention (`prediction_engine/analysis/<feature>.py`).

`data/hand_curated/` already exists (`by_elections.yaml`); we extend it with `pollster_tables/`.

`data/derived/` is new — needs adding to `.gitignore`? No — it's small JSON, deterministic from snapshot + curated tables, and being committed makes downstream Spec-B strategy work reproducible without re-running notebook 05. It should be committed.

---

## 4. Data inputs

### 4.1 Polls table (already in snapshot)

Schema as already defined: `pollster`, `published_date`, `geography`, `share_<party>` columns. Used by both A.1 regression and A.2 bias analysis.

### 4.2 Pollster cross-tab tables (manually curated)

Each file represents one pollster's published voter-flow table for one fieldwork period. Stored as:

```
data/hand_curated/pollster_tables/<pollster>/<YYYY-MM-DD>.<ext>
data/hand_curated/pollster_tables/<pollster>/<YYYY-MM-DD>.meta.json
```

`<YYYY-MM-DD>` is the **fieldwork end date**, not publication date.

`meta.json` schema:

```json
{
  "pollster": "techne",
  "source_url": "https://techneuk.com/.../voting-intention-2026-04-15.xlsx",
  "fieldwork_start": "2026-04-13",
  "fieldwork_end": "2026-04-15",
  "sample_size": 1632,
  "weighting_note": "Weighted to GE2024 vote and demographic profile",
  "downloaded_at_utc": "2026-04-26T15:30:00Z",
  "downloaded_by": "manual"
}
```

The data file contains the cross-tab. After parsing, the parser must produce a normalized long-form DataFrame regardless of how the source pollster lays it out.

**Initial seed:** ship with at least 2 pollster files committed (per Section 5's iterative loop, ordered by sample size descending).

### 4.3 Election results for bias analysis (already in snapshot)

- `results_2024` — GE 2024 per-constituency results, aggregable to a national Reform vote share.
- `byelections_results` — 4 by-elections; per-event Reform actual share.

---

## 5. Analysis A.1 — Poll-transition matrix

### 5.1 Regression method

For each pollster, sort their polls by `published_date`, then compute Δshares between consecutive polls **of the same pollster**:

```
ΔX_party,t = X_pollster,t - X_pollster,t-1     for each party X
```

Paired same-pollster differencing cancels constant pollster house effects.

For each non-Reform party `X`, fit OLS:

```
ΔX_t = β_X · ΔReform_t + ε_t
```

Run a **pollster-cluster bootstrap** (resample at the pollster level, 1000 iterations) to compute 95% CIs on each `β_X`.

Sanity-check: `Σ β_X` over non-Reform parties (including DK / WNV / Other if available) should be approximately +1 (every percentage point Reform loses must materialise somewhere). Assert this lies within `[0.85, 1.15]` and surface a warning in the notebook if not.

Two specifications produced:

| Variant | Filter | Use |
|---|---|---|
| Symmetric | All consecutive-pair Δs | Headline coefficient table |
| Reform-drops-only | Rows where `ΔReform < 0` | The directional answer to user's question |

### 5.2 Cross-tab method

Each pollster's table gives `(prior_vote_2024, current_vi) → share_pct` percentages.

For one pollster:
1. Validate that rows sum to ~100% (i.e. row-wise from `prior_vote_2024`). Transpose if necessary.
2. Extract the **`prior_vote_2024 = Reform`** row → "of voters who chose Reform in GE 2024, where are they now (Reform-stay, defection-to-X, DK, WNV)".
3. Compute the **defection vector**: of those NOT staying with Reform, what fraction goes to each of `Lab`, `LD`, `Con`, `Green`, other categories.

Per-pollster defection vector schema:

```python
{"pollster": "techne", "fieldwork_end": "2026-04-15",
 "reform_stay_pct": 73.2,
 "defection_share": {"lab": 0.18, "ld": 0.12, "con": 0.41, "green": 0.08,
                     "dk": 0.15, "other": 0.06}}
```

### 5.3 Iterative add-and-test loop

Order pollsters by **sample size descending** (largest first). For pollsters tied or with no sample size declared, fall back to alphabetical.

Process:

```
included = []
prior_aggregate = None
shift_history = []          # entries: {after_n, pollster, max_cell_shift}
for p in pollsters_sorted_by_sample_size_desc:
    candidate_included = included + [p]
    new_aggregate = mean(c.defection_vector for c in candidate_included)
    if prior_aggregate is None:
        # first pollster: no comparison possible yet
        max_cell_shift = None
    else:
        # both dicts have the same key set (cells = parties + dk + other)
        max_cell_shift = max(|new_aggregate[k] - prior_aggregate[k]| for k in new_aggregate)
    shift_history.append({"after_n": len(candidate_included),
                          "pollster": p.name,
                          "max_cell_shift": max_cell_shift})
    # The first 2 pollsters always go in (need at least one shift measurement).
    # Stopping evaluated from N=3 onwards.
    if len(candidate_included) >= 3 and max_cell_shift < threshold_pp:
        # adding p did NOT shift any cell beyond threshold → stop, do NOT include p
        break
    included = candidate_included
    prior_aggregate = new_aggregate
return {"included": [p.name for p in included],
        "stopped_at": len(included),
        "shift_history": shift_history,
        "stop_reason": "max_cell_shift_below_threshold" if len(included) < N_total
                       else "exhausted_pollsters"}
```

`threshold_pp` defaults to `1.0`, exposed as a notebook config so the user can tighten or relax after seeing the first run.

The first 2 pollsters always go in (need at least one shift measurement to compare against). Stopping evaluated from N=3 onwards. If the loop runs out of pollsters without ever stopping, `stop_reason: "exhausted_pollsters"` and all available pollsters are included.

### 5.4 Combining regression and cross-tab into the persisted matrix

The two methods answer slightly different questions:

- **Regression β** is a **marginal headline rate**: for each 1pp Reform headline change, how do other parties' headline numbers move. This is what a strategy-side scenario needs.
- **Cross-tab defection** is a **rate at which 2024-Reform voters are leaving**: not directly comparable as a marginal headline rate without conversion.

For Spec B (strategy use), regression `β` is the **primary** input. The cross-tab numbers persist alongside as a sanity check / second opinion. The persisted JSON's `primary_method` field flags this:

```json
{
  "schema_version": 1,
  "derived_from_snapshot_hash": "a80a24d29233",
  "as_of_date": "2026-04-26",
  "regression": {
    "method": "paired_difference_OLS_pollster_cluster_bootstrap",
    "n_pairs": 412,
    "n_pollsters": 11,
    "coefficients": {
      "lab":   {"beta": 0.42, "ci_lo": 0.36, "ci_hi": 0.48},
      "ld":    {"beta": 0.21, "ci_lo": 0.16, "ci_hi": 0.27},
      "con":   {"beta": 0.18, "ci_lo": 0.10, "ci_hi": 0.25},
      "green": {"beta": 0.10, "ci_lo": 0.06, "ci_hi": 0.15},
      "dk":    {"beta": 0.09, "ci_lo": 0.04, "ci_hi": 0.14},
      ...
    },
    "sum_check": 1.00
  },
  "cross_tab": {
    "pollsters_included": ["techne", "find_out_now", "more_in_common"],
    "pollster_count": 3,
    "stop_reason": "max_cell_shift_below_threshold",
    "stop_threshold_pp": 1.0,
    "shift_history": [{"after_n": 1, "pollster": "yougov",        "max_cell_shift": null},
                      {"after_n": 2, "pollster": "more_in_common", "max_cell_shift": 4.2},
                      {"after_n": 3, "pollster": "opinium",        "max_cell_shift": 0.7}],
    "reform_stay_pct_mean": 71.5,
    "defection_share_aggregate": {
      "lab": 0.20, "ld": 0.13, "con": 0.39, "green": 0.09,
      "dk": 0.13, "other": 0.06
    }
  },
  "primary_method": "regression"
}
```

A consuming strategy reads `regression.coefficients` for its math; `cross_tab` is informational only.

---

## 6. Analysis A.2 — Reform polling bias

### 6.1 Method

For every election event in our data:

| Event | Actual Reform share | Source |
|---|---|---|
| GE 2024 | from `results_2024` (national aggregation) | snapshot |
| Runcorn & Helsby 2025-05-01 | from `byelections_results` | snapshot |
| Hamilton, Larkhall & Stonehouse 2025-06-05 | from `byelections_results` | snapshot |
| Caerphilly 2025-10-23 | from `byelections_results` | snapshot |
| Gorton & Denton 2026-02-26 | from `byelections_results` | snapshot |

For each event:

1. Take the **final-week pollster average** for Reform (polls with `published_date ∈ [event_date - 7d, event_date - 1d]`). Filter by relevant geography (national for GE, omit for by-election since we don't have by-election-specific polls).
2. Compute `bias_t = actual_t - final_week_poll_mean_t`. Positive bias = Reform out-performed polls; negative = pollsters over-stated Reform.
3. Per-pollster: `pollster_bias_pollster,t = actual_t - final_week_pollster_mean_t` for each pollster active in that window.

Aggregate across events:

```python
{
  "aggregate_bias_pp": mean(bias_t),
  "aggregate_bias_n_events": 5,
  "per_pollster_bias_pp": {
    "yougov": {"mean_bias_pp": +1.2, "n_events": 5},
    "more_in_common": {"mean_bias_pp": +0.4, "n_events": 4},
    ...
  },
  "per_event": [
    {"event": "ge_2024", "date": "2024-07-04", "actual_pp": 14.3,
     "poll_mean_pp": 12.1, "bias_pp": +2.2, "n_polls": 18},
    ...
  ]
}
```

### 6.2 Honest scope caveat

Five events is small. The aggregate is descriptive, not statistically powerful. Per-pollster numbers with `n_events < 3` should be flagged in the JSON (`"reliability": "low"`) and visually de-emphasized in the notebook. This is documented at the top of notebook 05's bias section.

By-election polls are sparse — many by-elections will have zero polls in the final-week window. The JSON records `n_polls: 0` and excludes that event from per-pollster aggregation; it remains a recorded "no signal" event rather than disappearing silently.

### 6.3 Persisted artifact schema

```json
{
  "schema_version": 1,
  "derived_from_snapshot_hash": "a80a24d29233",
  "as_of_date": "2026-04-26",
  "method": "actual_minus_final_week_poll_mean",
  "final_week_window_days": 7,
  "events_used": 5,
  "events_with_polls": 4,
  "aggregate": {
    "bias_pp": +1.4,
    "bias_pp_unweighted_mean_of_events": +1.4,
    "interpretation": "positive = Reform out-performed pre-event polling on average"
  },
  "per_pollster": {
    "yougov":         {"mean_bias_pp": +1.2, "n_events": 5, "reliability": "high"},
    "more_in_common": {"mean_bias_pp": +0.4, "n_events": 4, "reliability": "high"},
    "techne":         {"mean_bias_pp": -0.2, "n_events": 2, "reliability": "low"}
  },
  "per_event": [
    {"event": "ge_2024", "date": "2024-07-04",
     "actual_share_pp": 14.3, "poll_mean_share_pp": 12.1,
     "bias_pp": +2.2, "n_polls_in_window": 18,
     "pollsters_in_window": ["yougov", "opinium", ...]}
  ]
}
```

---

## 7. Notebook 05 — `notebooks/05_poll_analytics.ipynb`

Built via `scripts/build_notebooks.py` (extends the existing pattern; uses the same `_PRELUDE` cell so cwd is the repo root).

Cell layout:

| # | Type | Content |
|---|---|---|
| 1 | md | Title + scope + caveats. Notes that A.1 regression is correlation, not voter-level switching; A.2 bias has small N. |
| 2 | code | `_PRELUDE` (auto-prepended). |
| 3 | md | Section A.1 header. |
| 4 | code | Load latest snapshot, run `compute_paired_diffs`, run `fit_reform_drop_regression(symmetric=True)` and `(symmetric=False)`. |
| 5 | code | Display coefficient table (party × β × CI lo × CI hi) for both variants side-by-side. |
| 6 | code | Horizontal bar chart of β with CI whiskers. |
| 7 | code | Inventory `data/hand_curated/pollster_tables/`; load all parsers; run `iterative_value_test`; show shift-history table. |
| 8 | code | Plot: max_cell_shift vs N pollsters; mark stop point. |
| 9 | code | Final aggregated transition matrix (heatmap). |
| 10 | code | Write `data/derived/poll_transition_matrix.json`. Print path + sum_check value. |
| 11 | md | Section A.2 header. |
| 12 | code | `compute_reform_bias(snapshot)` → per-event diffs table. |
| 13 | code | Per-pollster bias table (low-reliability rows visually flagged). |
| 14 | code | Bar chart: bias per event. |
| 15 | code | Write `data/derived/reform_polling_bias.json`. Print path + aggregate value. |
| 16 | md | Side-by-side comparison: regression coefficients vs cross-tab defection rates. Brief markdown commentary on agreement/disagreement. |

---

## 8. Module surface (key signatures)

```python
# prediction_engine/analysis/poll_transitions.py

def compute_paired_diffs(polls: pd.DataFrame, geography: str = "GB") -> pd.DataFrame:
    """For each pollster, sort by published_date and compute Δshare per party
    between consecutive polls. Returns long-form: pollster, t_lag, t,
    delta_<party>, sample_size."""

@dataclass
class RegressionResult:
    coefficients: dict[PartyCode, tuple[float, float, float]]  # beta, ci_lo, ci_hi
    sum_check: float
    n_pairs: int
    n_pollsters: int
    method: str

def fit_reform_drop_regression(diffs: pd.DataFrame,
                                restrict_to_drops: bool = False,
                                bootstrap_iterations: int = 1000) -> RegressionResult:
    """OLS of ΔX = β · ΔReform per non-Reform party, with pollster-cluster
    bootstrap CIs. If restrict_to_drops, filters to ΔReform < 0 first."""

# prediction_engine/analysis/pollster_tables/__init__.py

PARSERS: dict[str, Callable[[Path], pd.DataFrame]]  # populated by submodule imports

def parse(pollster: str, path: Path) -> pd.DataFrame: ...
    # dispatches to PARSERS[pollster]; raises if unknown.

# prediction_engine/analysis/pollster_tables/common.py

@dataclass
class CrossTab:
    pollster: str
    fieldwork_end: date
    sample_size: int | None
    long_form: pd.DataFrame   # cols: prior_vote_2024, current_vi, share_pct

def load_meta(path: Path) -> dict: ...

def validate_and_normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Validates rows sum to ~100%; transposes if column-wise; standardises
    party labels to PartyCode values."""

# prediction_engine/analysis/pollster_tables/aggregate.py

def aggregate_across_pollsters(parsed: list[CrossTab],
                                weights: list[float] | None = None) -> pd.DataFrame: ...

def iterative_value_test(parsed_in_order: list[CrossTab],
                          threshold_pp: float = 1.0) -> dict: ...
    # Returns dict: included, stopped_at, shift_history

# prediction_engine/analysis/poll_bias.py

@dataclass
class BiasResult:
    aggregate_bias_pp: float
    n_events: int
    per_event: list[dict]
    per_pollster: dict[str, dict]

def compute_reform_bias(snapshot: Snapshot,
                         final_week_window_days: int = 7) -> BiasResult: ...
```

---

## 9. Acceptance criteria

For Spec A to be considered done:

- [ ] All new modules under `prediction_engine/analysis/` and `prediction_engine/analysis/pollster_tables/` exist with the signatures in Section 8.
- [ ] At least 2 pollster table files committed under `data/hand_curated/pollster_tables/` (the two largest-sample-size pollsters whose formats are accessible). At least one fixture per parser under `tests/fixtures/pollster_tables/`.
- [ ] Unit tests cover: `compute_paired_diffs`, `fit_reform_drop_regression` (synthetic data with known β), `validate_and_normalise` (row-wise, column-wise, malformed), `iterative_value_test` (3-way synthetic where the third pollster shifts cells significantly vs one where it doesn't), `compute_reform_bias` (synthetic snapshot).
- [ ] At least one `parse_<pollster>` parser tested against its real saved fixture.
- [ ] Notebook 05 exists, generated by `scripts/build_notebooks.py`, executes end-to-end via `nbconvert` from inside `notebooks/` (per the existing convention).
- [ ] Notebook 05's last cells write valid `poll_transition_matrix.json` and `reform_polling_bias.json` to `data/derived/`. JSON files are committed.
- [ ] JSON `schema_version: 1` and `derived_from_snapshot_hash` populated in both files.
- [ ] Regression sum-check (`Σ β_X` for non-Reform ≈ 1.0) is asserted as a unit test against synthetic data and surfaced as a warning (not a hard fail) in the notebook.
- [ ] Iterative-value-test in the notebook reports a definite stop point ("included N pollsters, stopped because adding pollster N+1 shifted no cell by more than 1pp").
- [ ] Bias analysis notebook section flags any per-pollster row with `n_events < 3` as low-reliability.
- [ ] `MEMORY.md` / better-memory updated with the new artifact paths and consumer contracts so future sessions can find them.

---

## 10. Out of scope (explicit non-goals)

- **No new strategy.** The `reform_threat_consolidation` strategy is unchanged. No `apply_reform_bias_correction` or `apply_drop_scenario` stage exists yet.
- **No pipeline framework.** The `Stage` protocol and `Pipeline` dataclass are Spec B. Spec A produces inputs that those stages will consume.
- **No automated fetchers.** All pollster table files are downloaded manually and saved to `data/hand_curated/`. CLI fetchers for cross-tabs are deferred to a possible future spec.
- **No snapshot schema bump.** JSON artifacts live in `data/derived/`, not in the snapshot. Promotion into the snapshot schema is a separate decision once the analysis is proven.
- **No back-fill of historical transition matrices.** The matrix is computed from polls in the latest snapshot only; we don't compute one matrix per historical as-of date.
- **No region/nation-specific transition matrices.** GB-wide only, since GB-wide is the only geography covered by both the polls table and the cross-tab tables we'll have.
- **No prediction recomputation.** No existing prediction files are invalidated or rebuilt.

---

## 11. Risks, open questions, and revisitable decisions

### 11.1 Pollster format diversity

We don't know in advance which pollster formats are easy or hard to parse. The iterative loop is designed to fail fast: if pollster 3's format is hard, we don't add it unless adding it materially changes the answer. The sample-size-descending order means we prioritize statistical contribution over parser convenience — which is right but means we may have to write a hard parser for pollster 1.

**Mitigation:** if pollster 1 (largest sample) turns out to be PDF-only (likely YouGov), document parsing as a discrete sub-task in the implementation plan and consider whether to use `pdfplumber` or a lighter-weight tabula-py adapter.

### 11.2 Aggregation weighting

Section 5.2 uses simple unweighted means across pollsters. Sample-size weighting is the obvious next step but isn't done in v1 because (a) it makes the iterative add-and-test threshold harder to reason about, and (b) it requires confidence in declared sample sizes across pollsters. The JSON has space for it (`weights` field) when ready.

### 11.3 Where the JSON artifacts ultimately live

`data/derived/` is the v1 home. If Spec B's strategy sees this used heavily, the right next step is promoting both JSONs into the snapshot schema (new tables: `poll_transition_matrix`, `reform_polling_bias`) so a single snapshot file fully determines a prediction. That requires `SCHEMA_VERSION` bump and is appropriate once the analysis is stable.

### 11.4 Validation against actuals after GE 2027 (or whenever)

Once the next GE happens, A.2's bias analysis will have meaningful N. Until then it's descriptive. Document this expectation in the notebook so the user doesn't over-interpret early bias estimates.

### 11.5 Asymmetric voter behavior

The "where does Reform's lost vote go" answer (regression on `ΔReform < 0` only) might differ from "where does Reform's gained vote come from" (regression on `ΔReform > 0` only). Section 5.1 produces both via the `restrict_to_drops` flag. If they differ materially, Spec B's drop-scenario stage uses the drops-only β, which is the user's stated question.

---

## 12. Downstream consumers (Spec B preview)

Spec B will introduce, separately:

- `prediction_engine/pipeline/stages.py` — small `Stage` protocol + `Pipeline` dataclass.
- `prediction_engine/pipeline/stages/apply_reform_bias_correction.py` — auto-loads `reform_polling_bias.json`, shifts Reform's projected national share, redistributes via `poll_transition_matrix.json`.
- `prediction_engine/pipeline/stages/apply_drop_scenario.py` — config-driven drop_pp; redistributes via the same matrix.
- Refactor of `reform_threat_consolidation` to use the Pipeline machinery, optionally composing the new stages before its existing per-seat logic.
- `notebooks/06_drop_scenario_sweep.ipynb` — sweeps `additional_drop_pp` and `bias_correction_pp`; surfaces the seat-count surface.

Spec A's JSON schemas (Sections 5.4, 6.3) are the contract Spec B will read. Field names and types in those schemas are therefore part of the spec's stable surface — changes after Spec B is implemented require coordinated migration.
