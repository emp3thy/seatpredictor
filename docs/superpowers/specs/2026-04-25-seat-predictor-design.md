# Seat Predictor — Design Spec

**Date:** 2026-04-25
**Status:** Approved (pending user review of this written spec)
**Scope:** v1 implementation

---

## 1. Goals

Build a UK Westminster general-election seat predictor focused on the **tactical-consolidation dynamic** observed in recent Reform-led contests (Caerphilly Senedd 2025, Gorton & Denton Westminster 2025, etc.).

The system has **two distinct components** with a clear contract between them:

1. **Data engine** — collects polling, past-election results, and by-election templates; produces self-contained, idempotent, replayable snapshots.
2. **Prediction engine** — applies a swappable Strategy to a snapshot to produce per-seat predictions and national totals.

Predictions are deterministic, fully reproducible from `(snapshot_id, strategy, scenario_config)`, and produced offline from local files only.

### What problem this solves

Conventional uniform-swing models break in a fragmented multi-party landscape where no party clears 35%. The seat outcome depends heavily on whether the *anti-leader* vote can coordinate behind a locally credible challenger. Caerphilly demonstrated this with Plaid as the consolidator (despite Labour's century-long historical dominance there); Gorton demonstrated it with Labour. The system models this dynamic explicitly, scoped to Reform-as-threat contests where the by-election library has empirical signal.

### Reference points

- Electoral Calculus (https://www.electoralcalculus.co.uk/prediction_main.html) — uniform-swing baseline.
- Politico Poll of Polls (https://www.politico.eu/europe-poll-of-polls/united-kingdom/) — polling aggregation reference.
- Caerphilly Senedd by-election (23 Oct 2025) — Plaid won 47.4%; Reform 36.0%; Labour collapsed from 46.0% (2021 prior) to 11.0%. The left-bloc consolidated behind Plaid as the locally-credible challenger.
- Gorton & Denton Westminster by-election (26 Feb 2026) — Green won 40.7%; Reform 28.7%; Labour third at 25.4%, down from ~50%+ in 2024. Despite Labour being the prior incumbent, the left-bloc consolidated behind Green — the consolidator is identified by current projected strength, not historical incumbency.
- Runcorn & Helsby Westminster by-election (1 May 2025) — Reform won by 6 votes over Labour; Lab was the would-be consolidator but the consolidation effort fell just short.
- Hamilton, Larkhall & Stonehouse Holyrood by-election (5 Jun 2025) — Scottish Labour 31.6% over SNP 29.4% with Reform third at 26.1%.

---

## 2. Architecture

```
┌─────────────────┐     ┌────────────────────┐     ┌──────────────────────┐
│  data_engine    │     │ prediction_engine  │     │ analysis / notebooks │
│  (collects)     │ ──► │ (applies strategy) │ ──► │ (drill-down, sweeps) │
└─────────────────┘     └────────────────────┘     └──────────────────────┘
        │                       │                            │
        └───────────────► schema/ (shared) ◄─────────────────┘
                  Pydantic models = the contract
```

### Two-layer data architecture

```
   SOURCES (network)
         │   fetch  (idempotent per fetch-date; cache-or-skip)
         ▼
   RAW CACHE   (data/raw_cache/)
         │   transform  (idempotent per as-of + schema_version)
         ▼
   SNAPSHOTS   (data/snapshots/<as-of>__v<schema>__<hash>/)
         │   read-only contract
         ▼
   PREDICTION ENGINE
```

### Idempotency contract

Three rules:

1. **Fetch is idempotent per `(source, fetch_date)`.** First call writes raw HTML/CSV/JSON to `raw_cache/`; subsequent calls hit the cache and skip. `--refresh` forces re-fetch.
2. **Transform is idempotent per `(raw_cache_state, as_of_date, schema_version)`.** The snapshot file is named by a content hash over those three input keys. Same hash → no-op (the existing snapshot is reused). Schema bump → new snapshot file; old one untouched, old predictions remain reproducible. Note: the hash is computed over *inputs* (raw cache state etc.), not over the snapshot file's bytes — SQLite files are not byte-deterministic across regenerations, but we never regenerate when input hashes match, so this doesn't matter.
3. **Re-runs are time-travel by `--as-of`.** Polls are stored in raw cache with their `published_date`. "Snapshot as-of June 1" filters the cache to polls published on or before June 1. The HoC results CSV is static. The by-elections YAML is git-history-versioned.

### Initial backfill caveat

The first ever `fetch` call must scrape every poll currently visible on Wikipedia's polling pages (which usually go back to the prior GE), so historical `--as-of` queries have depth from day one.

---

## 3. Project layout

Single repository, monorepo with two packages.

```
seatpredictor/
  pyproject.toml              # uv-managed, single venv
  schema/                     # shared Pydantic models — the contract
    __init__.py
    poll.py
    constituency.py
    byelection.py
    transfer_weights.py
    snapshot.py
    prediction.py
  data_engine/
    sources/
      wikipedia_polls.py
      hoc_results.py
      byelections.py
    transforms/
      transfer_matrix.py      # derives reform_threat matrix from byelections
    snapshot.py               # orchestrates a full pull → snapshot
    cli.py                    # `seatpredict-data ...`
  prediction_engine/
    strategies/
      base.py                 # Strategy ABC + registry
      uniform_swing.py
      reform_threat_consolidation.py
    analysis/
      poll_trends.py
      drilldown.py
      sweep.py
    runner.py                 # loads snapshot, applies strategy, writes prediction
    cli.py                    # `seatpredict-predict ...`
  notebooks/
    01_polling_trends.ipynb
    02_constituency_drilldown.ipynb
    03_strategy_comparison.ipynb
    04_scenario_sweep.ipynb
  data/
    raw_cache/                # gitignored
    snapshots/                # gitignored
    predictions/              # gitignored
    hand_curated/
      by_elections.yaml       # tracked in git
  tests/
    schema/
    data_engine/
    prediction_engine/
    fixtures/
  README.md
```

Single venv, single git history. Both engines depend on `schema/`. The "two projects" requirement is realised at the package boundary, not the repo boundary.

### Tech stack

- Python (single venv, managed by `uv`).
- SQLite (single-file storage for snapshots and predictions; stdlib `sqlite3` plus SQLAlchemy core for query construction).
- Pandas (`pd.read_sql_table` / `pd.to_sql` for DataFrame round-trips).
- Pydantic v2 (typed models, runtime validation at I/O boundaries).
- BeautifulSoup + httpx (scraping with caching).
- Pytest (testing).
- Click or Typer (CLI).
- JupyterLab (or VS Code's notebook editor) for the notebooks layer.

---

## 4. Data engine

### Three sources, fetch → cache → transform shape

#### 4.1 Wikipedia polls (`data_engine/sources/wikipedia_polls.py`)

- **Fetch:** GET the "Opinion polling for the next UK general election" page plus regional-breakdown pages where they exist (Scotland, Wales, London). Save raw HTML to `raw_cache/wikipedia_polls/<fetched_at>/page.html` plus `meta.json` (URL, ETag, timestamp). Cache key is the URL; re-fetch only on cache miss for the day or `--refresh`.
- **Transform:** parse all polling tables. One row per poll: `pollster`, `fieldwork_start`, `fieldwork_end`, `published_date`, `sample_size`, `geography` (`GB` / `Scotland` / `Wales` / `London`), and one column per party (`con`, `lab`, `ld`, `reform`, `green`, `snp`, `plaid`, `other`).
- **As-of filter:** `published_date <= as_of_date`.

#### 4.2 HoC Library 2024 results (`data_engine/sources/hoc_results.py`)

- **Source:** GE 2024 results CSV (briefing paper CBP-10009).
- **Fetch:** download once; static thereafter; lives in raw cache.
- **Transform:** normalise to one row per `constituency × party` with `ons_code`, `constituency_name`, `region`, `nation`, `party`, `votes`, `share`. Plus a derived winner-per-constituency view.

#### 4.3 By-election templates (`data_engine/sources/byelections.py`)

- **Source:** `data/hand_curated/by_elections.yaml`, git-tracked, hand-edited as new by-elections happen.
- **Per-entry schema:** date, name, type (`westminster_byelection` / `senedd` / `holyrood`), region/nation, candidates (party, votes, share), prior result (party shares from the preceding contest in the same seat), `narrative_url` linking to Wikipedia for context, **`threat_party`** (the party the tactical consolidation was *against* — typically the polling-frontrunner heading into the contest, even if they didn't win on the day; e.g. `reform` for Caerphilly Senedd 2025 even though Plaid won), and **`exclude_from_matrix`** (boolean default false; set true for by-elections driven by atypical dynamics — e.g. a scandal-driven contest where flows aren't generalisable).
- **Validation:** Pydantic at parse time. Shares must sum to ~100% per event (tolerance ±0.5pp). `threat_party` must match a known party. If `threat_party` is null, the event is treated as `exclude_from_matrix=true` automatically.
- **As-of filter:** `date <= as_of_date`.

### 4.4 Derived: reform_threat transfer matrix (`data_engine/transforms/transfer_matrix.py`)

Computed from `byelections.parquet`, not hand-edited. For each by-election event with `threat_party == reform` and `exclude_from_matrix == false`:

- Identify the **consolidator** = party with biggest gain over its prior share among left-bloc parties (`Lab`, `LD`, `Green`, `SNP`, `Plaid`).
- For every other non-threat-party `p` with prior share above 2%, compute `flow_rate[p → consolidator] = (prior_share[p] − actual_share[p]) / prior_share[p]`, clamped `[0, 1]`.
- Reform's own share movement is not extracted (Reform is the threat, not a flow source).

Aggregate per region:

- For each `(region, consolidator, source)` cell, average the observed `flow_rate` across all events in that region where the cell was observable.
- No observations for a cell → cell is `null`. **Strict: no cross-bloc / cross-region inference in v1.**

Stored as the `transfer_weights` and `transfer_weights_provenance` tables inside the snapshot SQLite file. The logical structure (nation → consolidator → source → weight) is shown below as JSON for clarity, but it lives as relational rows. Outer keys are nations (`england` / `wales` / `scotland`); within each nation, one sub-dict per *possible* consolidator party (any left-bloc party that could be the locally strongest in that nation); innermost dict maps source-party → weight.

```json
{
  "reform_threat": {
    "england":  {
      "lab":   { "ld": 0.62, "green": 0.50, "con": 0.15 },
      "ld":    { "lab": 0.40, "green": 0.30, "con": 0.10 },
      "green": null,
      "con":   null
    },
    "wales":    {
      "plaid": { "lab": 0.60, "ld": 0.45, "green": 0.40, "con": 0.10 },
      "lab":   null,
      "ld":    null,
      "green": null
    },
    "scotland": {
      "snp":   { "lab": 0.55, "ld": 0.40, "green": 0.30, "con": 0.05 },
      "lab":   null,
      "ld":    null,
      "green": null
    }
  },
  "base": null,
  "provenance": {
    "england":  {
      "lab":   { "events": ["runcorn_helsby_2025"],   "n": 1 },
      "green": { "events": ["gorton_denton_2026"],    "n": 1 }
    },
    "wales":    { "plaid": { "events": ["caerphilly_senedd_2025"], "n": 1 } },
    "scotland": { "lab":   { "events": ["hamilton_larkhall_stonehouse_2025"], "n": 1 } }
  }
}
```

A consolidator slot is `null` (not an empty dict) when no by-election in that nation has had that party act as the consolidator. A consolidator slot may exist but have specific source cells null — meaning the consolidator is observed but that particular source party did not have meaningful prior share in any contributing event.

The `base` (non-Reform-threat) matrix is not derivable in v1 (no v1 strategy uses it) and stays null with a documented schema.

### 4.5 Raw cache layout

```
data/raw_cache/
  wikipedia_polls/2026-04-25/page.html + meta.json
  wikipedia_polls/2026-04-26/page.html + meta.json    # one dir per fetch date
  hoc_results/ge_2024/results.csv + meta.json          # static, fetched once
  byelections/                                         # YAML lives in hand_curated/, no separate cache
```

### 4.6 Snapshot layout

A snapshot is a **single SQLite file** named by its content hash:

```
data/snapshots/2026-04-25__v1__a3f2.sqlite
```

Tables inside the file:

| table | rows | purpose |
|---|---|---|
| `manifest` | 1 row | `as_of_date`, `schema_version`, `source_versions` (JSON), `content_hash`, `generated_at` |
| `polls` | one per poll | parsed from Wikipedia, filtered by `published_date <= as_of_date` |
| `results_2024` | constituency × party | from HoC Library CSV |
| `byelections_events` | one per event | event-level metadata: name, date, type, region/nation, `threat_party`, `exclude_from_matrix`, `narrative_url` |
| `byelections_results` | one per (event, candidate) | per-candidate prior and actual shares |
| `transfer_weights` | one per (region, consolidator, source) | derived matrix entries with `weight` and `n` (count of contributing events) |
| `transfer_weights_provenance` | one per (region, consolidator, event) | provenance trace from cell back to contributing events |

**Why single-file SQLite:** keeps the snapshot a single artifact rather than a directory, makes ad-hoc inspection trivial (`sqlite3 snapshot.sqlite ".tables"`, DB Browser for SQLite GUI, `pd.read_sql_table` from Python), gives ACID semantics for free (no risk of partial-snapshot states), and supports natural joins for drill-down queries. The byte-determinism trade-off is irrelevant because idempotency rule 2 hashes inputs, not outputs.

### 4.7 CLI surface

```bash
seatpredict-data fetch                            # refresh raw cache for today
seatpredict-data snapshot                         # produce snapshot for today
seatpredict-data snapshot --as-of 2026-06-01      # historical replay
seatpredict-data backfill --since 2026-01-01      # one-time: weekly snapshots back to date
seatpredict-data snapshot --refresh               # force re-fetch
```

---

## 5. Prediction engine

### 5.1 Strategy contract

```python
class Strategy(ABC):
    name: str                            # registry key
    config_schema: type[ScenarioConfig]  # Pydantic model, strategy-specific knobs

    @abstractmethod
    def predict(
        self, snapshot: Snapshot, scenario: ScenarioConfig
    ) -> PredictionResult: ...
```

- `Snapshot` — typed wrapper that opens the SQLite file and exposes each table as a lazily-loaded DataFrame (`snapshot.polls`, `snapshot.results_2024`, `snapshot.byelections_events`, `snapshot.byelections_results`, `snapshot.transfer_weights`).
- `ScenarioConfig` — strategy-specific knobs, validated by Pydantic.
- `PredictionResult` — `per_seat: DataFrame`, `national_totals: dict`, `run_metadata: dict` (full config + snapshot manifest copied in for self-description).

### 5.2 Per-seat output schema

| column | meaning |
|---|---|
| `ons_code`, `constituency_name`, `nation`, `region` | identity |
| `share_2024_{party}` | 2024 GE vote share |
| `share_raw_{party}` | post-uniform-swing projected share (before tactical adjustment) |
| `share_predicted_{party}` | strategy's final predicted share |
| `predicted_winner`, `predicted_margin` | derived |
| `leader` | party with highest `share_raw` |
| `consolidator` | party in left-bloc with highest `share_raw` (when applicable, else null) |
| `clarity` | consolidator-clarity coefficient `[0, 1]` |
| `matrix_nation` | nation key used for matrix lookup (`england` / `wales` / `scotland`); null for NI |
| `matrix_provenance` | list of by-election events that contributed to the cells used |
| `notes` | list of flags: any combination of `non_reform_leader`, `consolidator_already_leads`, `low_clarity`, `no_matrix_entry`, `matrix_unavailable`, `multiplier_clipped`, `ni_excluded` |

National totals saved as `national.json`: per-party seat counts plus per-region/nation breakdowns.

### 5.3 v1 strategies

#### `uniform_swing`

Baseline. Take national poll average minus 2024 GE national share = swing per party; add to every seat's 2024 share; winner = max. No tactical adjustment. Used as control and as fallback.

`ScenarioConfig`:

```python
class UniformSwingConfig(ScenarioConfig):
    polls_window_days: int = 14            # rolling window for poll average
```

#### `reform_threat_consolidation`

The default v1 tactical-modelling strategy.

**Algorithm:**

1. **Project raw shares.** Apply uniform swing per region (GB-wide where regional polls absent; regional swing where Scotland/Wales/London polls exist) to each seat's 2024 GE result.

2. **Short-circuit if leader is not Reform.** Return uniform-swing result, flag `non_reform_leader`.

3. **Identify consolidator.** The left-bloc set is `{Lab, LD, Green}` in England, `{Lab, LD, Green, Plaid}` in Wales, `{Lab, LD, Green, SNP}` in Scotland. The consolidator is the left-bloc party with the highest `share_raw` in this seat. If the consolidator's `share_raw` ≥ leader's `share_raw`, return uniform-swing result and flag `consolidator_already_leads` (no tactical contest to resolve). If no left-bloc party has meaningful share (>2%), fall back to uniform_swing and flag `matrix_unavailable`.

4. **Compute consolidator clarity.**
   ```
   gap = consolidator_share_raw − next_highest_left_bloc_share_raw
   clarity = clip(gap / clarity_threshold, 0.0, 1.0)
   ```
   Default `clarity_threshold = 5.0` percentage points. `clarity = 0` means the anti-Reform vote is too fragmented to coordinate; no flow happens. If `clarity < 0.5`, flag `low_clarity` in `notes` (informational; flow still applies, just weakly).

5. **Look up matrix entries.** For each non-leader, non-consolidator party `p` with `share_raw[p] > 0`:
   ```
   nation = seat.nation                    # "england" / "wales" / "scotland"
   weight = transfer_weights.reform_threat[nation][consolidator][p]
   ```
   `null` consolidator slot → fall back to uniform_swing for the seat, flag `matrix_unavailable`. Consolidator present but `null` for this `p` → no flow from this source, add `no_matrix_entry` to `notes` (the source's share stays put). The `notes` column is a list of flags (any seat may carry multiple flags).

6. **Apply flows scaled by clarity and multiplier.**
   ```
   moved = share_raw[p] × weight × clarity × multiplier
   share_predicted[p] -= moved
   share_predicted[consolidator] += moved
   ```
   Total redistribution from any source clipped at 100% of its share (flag `multiplier_clipped` if it ever fires).

7. **Re-normalise so shares sum to 1.0; pick winner.**

**Northern Ireland:** NI seats short-circuit to uniform-swing-only with flag `ni_excluded`. Distinct party system; out of v1 modelling scope.

`ScenarioConfig`:

```python
class ReformThreatConfig(ScenarioConfig):
    multiplier: float = 1.0                 # global dampen/amplify
    clarity_threshold: float = 5.0          # percentage points
    polls_window_days: int = 14
```

### 5.4 Determinism

Mandatory. No RNG anywhere without a seed. Same `(snapshot, strategy, config)` produces logically-identical output (row-set equality across all prediction tables). The output SQLite file isn't byte-identical due to SQLite internals, but its contents are.

### 5.5 CLI surface

```bash
seatpredict-predict list-strategies
seatpredict-predict run --snapshot <id> --strategy reform_threat_consolidation \
    --multiplier 1.0 --clarity-threshold 5.0 --label baseline
seatpredict-predict sweep --snapshot <id> --strategy reform_threat_consolidation \
    --multiplier 0.5,0.75,1.0,1.25,1.5 --clarity-threshold 5.0 --label-prefix sweep
seatpredict-predict diff <run_id_1> <run_id_2>
```

### 5.6 Prediction output layout

A prediction run produces a **single SQLite file**:

```
data/predictions/<snapshot_id>__<strategy>__<config_hash>__<label>.sqlite
```

Tables:

| table | purpose |
|---|---|
| `seats` | per-seat predictions (full schema from §5.2) |
| `national` | per-party seat counts plus per-region/nation breakdowns |
| `config` | 1 row: snapshot id, strategy, full scenario config (JSON), schema_version, run_id, generated_at — full copy for reproducibility |
| `notes_index` | denormalised (ons_code, flag) view for fast filtering by flag |

---

## 6. Analysis layer

### 6.1 Notebooks

Four notebooks, each opening a snapshot or a prediction run and answering one question.

| notebook | question |
|---|---|
| `01_polling_trends.ipynb` | How are polls moving? Per-party trend lines, GB and regional. Sanity check on data engine output. |
| `02_constituency_drilldown.ipynb` | Pick a seat (or list of seats); show projected raw shares, the consolidator, clarity, matrix entries used and their by-election provenance, the flows applied, and the final prediction. The place to stress-test the model on individual seats. |
| `03_strategy_comparison.ipynb` | uniform_swing vs reform_threat_consolidation. List seats that flip; chart national-total deltas. |
| `04_scenario_sweep.ipynb` | Sweep `multiplier` × `clarity_threshold` 2-D; plot per-party national seat counts as a function of each. |

### 6.2 CLI helpers backing the notebooks

```bash
seatpredict-analyze drilldown --run <prediction_id> --seat "Caerphilly" --explain
seatpredict-analyze flips --runs <run_a> <run_b>
```

Anything that becomes a routine output gets a CLI command and a function in `prediction_engine/analysis/`. Notebooks call those functions; notebooks aren't the source of truth.

### 6.3 Notebook setup hand-holding

User is new to Jupyter notebooks. The README ships a 5-line "first time running a notebook" walkthrough covering both options:

- **VS Code's built-in notebook editor.** Open `.ipynb` in VS Code, pick the project venv as kernel, run cells with Shift+Enter.
- **JupyterLab in a browser.** `uv run jupyter lab` from the project root opens the UI in a browser.

Notebooks are short (30–50 cells), every cell does one observable thing, every chart has a one-sentence interpretation cell next to it.

---

## 7. Testing

### 7.1 Schema/contract tests (`tests/schema/`)

The most important layer. Every Pydantic model has a round-trip test: construct → write to SQLite (`pd.to_sql`) → read back (`pd.read_sql_table`) → field-equal assertion. Snapshot manifest validation verifies `schema_version` increments and `content_hash` is reproducible from inputs (input-hash equality, not file-byte equality).

### 7.2 Data engine tests (`tests/data_engine/`)

- **Wikipedia parser** — golden-file tests against committed sample HTML in `tests/fixtures/`. Re-running the parser must produce row-identical results (compared via canonical sort + DataFrame equality) for the fixture.
- **HoC results loader** — golden-file test against a 5-constituency sample. Verifies column types, share calculations, winner derivation.
- **By-elections loader** — `by_elections.yaml` validates against the Pydantic model on every CI run.
- **Idempotency** — running snapshot twice with no source changes produces the same content hash (input-hash equality). The two SQLite files are not byte-identical but their logical content is — verified by row-set comparison per table.
- **Transfer matrix derivation** — fake by-elections with hand-computed expected aggregates; assert the derived matrix matches.
- **SQLite write/read round-trip** — every snapshot table written can be read back via `pd.read_sql_table` with type-equal columns.

### 7.3 Prediction engine tests (`tests/prediction_engine/`)

- **Determinism** — same `(snapshot, strategy, config)` produces byte-identical output across runs. No RNG.
- **`uniform_swing` baseline** — handcrafted 3-seat fake snapshot with known answers; end-to-end transform.
- **`reform_threat_consolidation`** — handcrafted fake snapshot with known matrix and known seats. Cases:
  - Reform leader, clear consolidator, populated matrix → expected per-seat flows.
  - Reform leader, fragmented bloc → low clarity → minimal flow → leader wins.
  - Non-Reform leader → uniform-swing fallback, `notes` flagged.
  - Sparse matrix (`no_matrix_entry`) → source's share unchanged, flag set.
  - All matrix cells null for relevant consolidator → `matrix_unavailable` fallback.
  - NI seat → `ni_excluded`.
- **Multiplier sweep** — sweep through 5 multiplier values; assert monotonicity (higher multiplier ≥ lower multiplier in flow magnitude for any non-zero cell).

### 7.4 Analysis layer tests

Light. Drill-down is mostly formatting; cover with a smoke test on one fake seat.

### 7.5 What is not tested

We do not unit-test substantive accuracy ("does the prediction match reality"). Reality hasn't happened. The scenario sweep and constituency drill-down exist for human inspection; that's the substantive validation.

---

## 8. Error handling

Three failure modes, handled differently:

- **Source-side failure** (Wikipedia 503, HoC URL 404, malformed YAML): data engine fails loudly with a clear "could not refresh source X" message and exits non-zero. Snapshots aren't produced from partial data — either everything fetched cleanly or nothing did. The cache from the last successful fetch remains untouched.
- **Validation failure** (poll row missing required column, by-election shares not summing to ~100%): fail at parse time with the offending row/event named in the error. Don't silently coerce.
- **Strategy-side fallbacks** (sparse matrix, non-Reform leader, missing consolidator) are *not errors*. Flagged in the per-seat `notes` column. The prediction completes; the user sees which seats relied on fallbacks.

---

## 9. Non-goals (v1 explicitly out of scope)

- **MRP / individual-respondent modelling.** No respondent-level data.
- **Constituency-specific polls / Lord Ashcroft single-seat surveys.** Could be a v2 override mechanism.
- **Northern Ireland.** NI seats short-circuit to uniform-swing only.
- **Live prediction APIs.** Snapshots are local files.
- **Web dashboard.** Notebooks + CLI only.
- **Tactical-vote modelling for non-Reform threats.** Strategy short-circuits on `leader != Reform`.
- **Cross-bloc / cross-region inference for sparse matrix cells.** Strict no-extrapolation in v1.
- **Abstention / turnout modelling.** Voters who don't switch stay with their party.
- **Probabilistic predictions / confidence intervals.** Point estimates only.
- **Calibrating to past general elections.** Matrix and strategy evaluated qualitatively against by-elections, not back-tested.
- **Standalone hand-curated transfer-matrix YAML.** The matrix is derived from the by-election library.
- **External research (BES, YouGov second-preference) as a primary data source.** Optional sanity check only.

---

## 10. Implementation-time research and curation tasks

- Curate `by_elections.yaml` with accurate per-candidate prior and actual shares for every Westminster, Holyrood, and Senedd by-election since the July 2024 GE that has `leader = Reform` or where the dynamic is plausibly relevant. Each entry includes a Wikipedia `narrative_url`.
- Initial backfill of Wikipedia polls so historical `--as-of` queries have depth.
- Sanity-check derived matrix cells (those with low `n`) against any published BES / YouGov / by-election analysis numbers. Flag implausible cells; revisit by-election curation if a cell is wrong.

---

## 11. Open questions deferred to implementation

- Exact `clarity_threshold` default — start at 5pp, may revise after running on real data.
- Exact `polls_window_days` default — start at 14, may revise.
- Whether Welsh / Scottish sub-national poll signal is reliable enough to use as a regional swing input, or whether to fall back to GB-only swing for the Celtic nations.
- Per-by-election review of which events should set `exclude_from_matrix=true` — this is a curation decision made event-by-event when populating `by_elections.yaml`.
- Whether the rolling-window poll average should be unweighted, recency-weighted, or pollster-quality-weighted.
