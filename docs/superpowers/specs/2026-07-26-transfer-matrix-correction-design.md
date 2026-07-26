# Transfer Matrix Correction and Hand-Curated Overrides

**Date:** 2026-07-26
**Status:** Approved

## Problem

The reform-threat consolidation strategy redistributes vote from source parties to a
per-seat consolidator using weights in the snapshot's `transfer_weights` table. Those
weights are derived from by-election data by
`data_engine/transforms/transfer_matrix.py::_compute_flows`:

```python
raw_flow = (prior - actual) / prior
```

This is the fraction of a source party's vote that **disappeared** between the general
election and the by-election. It is not the fraction that moved to the consolidator. In a
Reform-threat by-election the Conservative vote collapses and the bulk of it goes to
Reform, not to Labour. The formula credits all of it to the consolidator.

The snapshot built on 2026-07-26 shows the consequence:

| cell | derived weight |
|---|---|
| england lab ← ld | 0.94 |
| england lab ← green | 0.84 |
| england lab ← con | 0.80 |

Applied at `multiplier = 1.0` with clarity saturating at a 5pp gap, these move 80–94% of
each source party's vote to Labour in a single hop. Reform falls from 153 seats under
uniform swing to 22 under the tactical strategy — a collapse driven by a derivation
error, not by a modelling choice.

A second, smaller defect compounds it: no `england lab ← other` cell exists in the
derived data, so the `no_matrix_entry` flag fires in 143 seats and that vote never moves
at all. The model is simultaneously too aggressive on three sources and silent on a
fourth.

## Goals

1. Correct the derivation so a weight represents transfer to the consolidator rather than
   total vote loss.
2. Provide a hand-curated override mechanism for cells where judgement should displace or
   supplement the derived value.
3. Apply four England Labour-consolidator overrides supplied by the analyst.

## Non-Goals

- Changing the strategy algorithm in `prediction_engine/`. No file under
  `prediction_engine/` is modified.
- Changing `LEFT_BLOC`, consolidator identification, clarity, or the multiplier.
- Regional or demographic swing. Uniform swing remains the projection basis.

## Design

### 1. Corrected derivation

`_compute_flows` is rewritten to use the consolidator's own gain as the transfer budget.
Its signature is unchanged — `ev_results` already contains every party's row, including
the consolidator's, so the gain is read from data the function already receives.

```
consolidator_gain = consolidator.actual_share - consolidator.prior_share

total_loss = sum over all parties p != consolidator of max(0, prior_p - actual_p)
             (Reform and Restore are included in this sum; they normally grow, so they
              contribute 0, but a shrinking Reform must not be excluded from the
              denominator)

scale = clip(consolidator_gain / total_loss, 0.0, 1.0)

for each source p with prior_p > PRIOR_SHARE_THRESHOLD, p not in
{consolidator, REFORM, RESTORE}:
    flow_p = clip(((prior_p - actual_p) / prior_p) * scale, 0.0, 1.0)
```

The sum of transferred vote across all sources cannot exceed the consolidator's actual
gain. This is an accounting identity: shares sum to 100 before and after, so total gains
equal total losses, and `scale` is the fraction of the freed vote that the consolidator
captured. Reform's gain absorbs the remainder without needing to be modelled explicitly.

Guards:

- `total_loss <= 0` → return `{}`.
- `consolidator_gain <= 0` → return `{}`. `_identify_consolidator` already rejects
  non-positive gain, so this is defensive.

Unchanged: `PRIOR_SHARE_THRESHOLD = 2.0`; the exclusion of Reform and Restore as flow
*sources*; `_identify_consolidator` in full.

Worked example:

```
lab    28 -> 41   gain +13   (consolidator)
con    31 ->  6   loss  25
ld     12 ->  7   loss   5
green   7 ->  4   loss   3
reform 20 -> 40   gain +20

total_loss = 33
scale      = 13 / 33 = 0.394

con   -> lab = (25/31) * 0.394 = 0.318   (today: 0.806)
ld    -> lab = ( 5/12) * 0.394 = 0.164   (today: 0.417)
green -> lab = ( 3/ 7) * 0.394 = 0.169   (today: 0.429)
```

Relative ordering between sources within an event is preserved; every weight shrinks by
that event's `scale`.

#### Effect on every cell in the current snapshot

Running the proposed formula over the five eligible by-elections in
`2026-07-26__v3__f83714f07627.sqlite`:

| nation | consolidator | source | today | corrected |
|---|---|---|---:|---:|
| england | lab | con | 0.798 | 0.401 |
| england | lab | green | 0.841 | 0.423 |
| england | lab | ld | 0.941 | 0.473 |
| england | green | con | 0.839 | 0.497 |
| england | green | lab | 0.500 | 0.296 |
| england | green | ld | 0.606 | 0.359 |
| england | green | other | 0.819 | 0.485 |
| wales | plaid | con | 0.855 | 0.305 |
| wales | plaid | lab | 0.761 | 0.271 |
| wales | plaid | ld | 0.500 | 0.178 |
| wales | plaid | other | 0.690 | 0.246 |
| england | ld | con | 0.552 | 0.010 |
| england | ld | green | 0.335 | 0.006 |
| england | ld | lab | 0.268 | 0.005 |
| scotland | green | con | 0.647 | 0.018 |
| scotland | green | snp | 0.361 | 0.010 |
| scotland | green | ld | 0.120 | 0.003 |
| scotland | green | lab | 0.042 | 0.001 |

These are expected values, not test assertions; the implementation must reproduce them
from the data rather than hard-code them.

#### Accepted consequence: near-zero consolidators

The last two blocks collapse to near-zero. Runcorn and Helsby 2025 yields
`scale = 0.018`; Hamilton, Larkhall and Stonehouse 2025 yields `scale = 0.027`. In both
events the consolidator's own gain was very small next to total vote churn, because
Reform absorbed nearly all the freed vote.

This is the formula behaving correctly — those events show almost no tactical
consolidation. The decision is to accept it: the LD-consolidator seats and the Scottish
Green-consolidator seats will barely move under the tactical strategy until better
by-election evidence arrives. No floor is applied to `scale`, no override is written for
those cells, and the affected events are not excluded from the matrix.

Rejected alternatives: a minimum-`scale` floor (distorts every nation to fix two cells);
hand-curated overrides for those cells (no evidential basis, unlike the England Labour
cells which correct a known derivation error); `exclude_from_matrix: true` on the two
events (discards real data and forces `matrix_unavailable`).

### 2. Override file

New file `data/hand_curated/transfer_overrides.yaml`, following the conventions of the
existing `by_elections.yaml` and `local_elections.yaml`:

```yaml
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
      Liberal Democrat tactical switching to Labour is well attested but the derived
      0.94 implied near-total collapse of the LD vote into Labour.

  - nation: england
    consolidator: lab
    source: other
    weight: 0.2
    rationale: >
      "Other" is dominated by fringe parties and independents whose votes are
      belief-driven or personal-relationship-driven, and therefore move less readily
      than mainstream tactical votes. No derived cell exists for this pair.
```

Scope is England only. Scotland (green consolidator) and Wales (plaid consolidator) keep
their derived weights, corrected by the new formula.

Measured against the *corrected* derivation rather than the current one, these overrides
are not a uniform damping factor. They move `con` down (0.401 → 0.20) and `green` and
`ld` up (0.423 → 0.70, 0.473 → 0.65). They encode the analyst's view that left-bloc
tactical switching is stronger than a single by-election showed, while
Conservative-to-Labour switching is considerably weaker. The `rationale` field is where
that judgement is recorded, and it is required for exactly this reason.

### 3. Schema and loader

Add to `schema/transfer_weights.py`:

```python
class TransferWeightOverride(BaseModel):
    nation: Nation
    consolidator: PartyCode
    source: PartyCode
    weight: float = Field(ge=0.0, le=1.0)
    rationale: str = Field(min_length=1)
```

Relax `TransferWeightCell.n` from `Field(gt=0)` to `Field(ge=0)`. A cell with `n == 0` is
hand-curated with no supporting by-election events.

New module `data_engine/sources/transfer_overrides.py`:

```python
def load_transfer_overrides(yaml_path: Path) -> pd.DataFrame:
    """Load transfer_overrides.yaml into a DataFrame with columns
    nation, consolidator, source, weight, rationale.

    A missing file returns an empty DataFrame with those columns — overrides are
    optional. A duplicate (nation, consolidator, source) key raises ValueError;
    silent last-wins would hide a curation mistake.
    """
```

Each entry is validated through `TransferWeightOverride`. The loader mirrors the
structure of `data_engine/sources/byelections.py`.

### 4. Application and provenance

`derive_transfer_matrix` gains an `overrides: pd.DataFrame | None = None` parameter and
applies it after the existing groupby, before returning:

- Key on the full `(nation, consolidator, source)` triple.
- If a derived cell exists for that key, its `weight` is replaced and `n` set to 0.
- If no derived cell exists, a new row is appended with `n = 0`.
- Derived cells untouched by any override keep their computed `weight` and `n >= 1`.

For every distinct `(nation, consolidator)` pair appearing in the overrides, append a
provenance row with `event_id = "hand_curated"`. `TransferWeightProvenance.event_id`
already requires only a non-empty string, so no schema change is needed. This surfaces in
each seat's `matrix_provenance` column, so notebook drilldowns show which seats relied on
curated numbers.

`data_engine/snapshot.py` loads the override file alongside the by-elections file and
passes it to `derive_transfer_matrix` at line 75.

### 5. Downstream effects

No file under `prediction_engine/` changes. `Snapshot.consolidator_observed`,
`lookup_weight`, and `provenance_for_consolidator` are generic queries over
`transfer_weights` and pick up the new cells without modification. Within the strategy:

- `consolidator_observed("england", "lab")` stays true.
- `no_matrix_entry` stops firing for the `other` source in the 143 affected seats.
- All other flags are unaffected.

The snapshot content hash changes, so the 2026-07-26 snapshot is superseded and both
predictions must be rebuilt. Reform's tactical seat count is expected to land well above
22 and below the uniform-swing 153.

## Testing

**`_compute_flows` (unit):**

- The worked example above produces 0.318 / 0.164 / 0.169 within tolerance.
- `consolidator_gain > total_loss` clips `scale` to 1.0.
- An event with no shrinking party (`total_loss == 0`) returns `{}`.
- A source with `prior_share <= 2.0` is still skipped.
- Reform and Restore are still excluded as sources.
- A shrinking Reform is counted in `total_loss` even though it is not a source.

**Loader (unit):**

- A valid file parses to the expected DataFrame.
- A duplicate `(nation, consolidator, source)` key raises.
- A missing or empty `rationale` is rejected.
- A `weight` outside `[0, 1]` is rejected.
- An unknown `nation` or party code is rejected.
- An absent file returns an empty DataFrame rather than raising.

**`derive_transfer_matrix` (integration):**

- An override replaces a derived cell's weight and sets `n = 0`.
- An override for an absent key creates the cell with `n = 0`.
- A derived cell with no matching override retains its weight and `n >= 1`.
- Provenance gains exactly one `hand_curated` row per distinct
  `(nation, consolidator)` in the overrides.
- Passing `overrides=None` reproduces current behaviour exactly.

**Existing tests requiring updates:**

- `tests/data_engine/test_transfer_matrix.py` — asserts directly on `_compute_flows`
  output; the primary casualty of the formula change.
- `tests/data_engine/test_snapshot_orchestrator.py` — asserts on derived cell values and
  on the `derive_transfer_matrix` call.
- `tests/schema/test_transfer_weights.py` — the `n > 0` constraint is being relaxed, so
  any test asserting that `n = 0` is rejected must invert.
- `tests/prediction_engine/conftest.py`, `test_fixture_sanity.py`, and
  `test_reform_threat.py` — all reference hard-coded weights; audit each for values
  derived from the old formula.

## Open Questions

None.
