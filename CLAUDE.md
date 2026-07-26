# seatpredictor

A UK Westminster seat-prediction model. Polling and 2024 constituency results are
ingested into immutable SQLite snapshots; strategies read a snapshot and write
prediction files. Numbers here end up in a published forecast, so a silently wrong
value is worse than a crash.

## Layout

| Path | Responsibility |
|---|---|
| `data_engine/` | Fetch, parse, transform sources into a snapshot |
| `prediction_engine/` | Read a snapshot, run a strategy, write predictions |
| `schema/` | Pydantic models shared by both |
| `data/hand_curated/` | Human-authored YAML inputs (tracked) |
| `data/snapshots/`, `data/predictions/`, `data/raw_cache/` | Generated (gitignored) |

## Commands

Call the venv binaries directly. **Never `uv run`** — it reverts the editable install.

```bash
.venv/Scripts/python.exe -m pytest          # tests
.venv/Scripts/seatpredict-data.exe fetch    # download sources
.venv/Scripts/seatpredict-data.exe snapshot # build a snapshot
.venv/Scripts/seatpredict-predict.exe run --snapshot <path> --strategy <name> --out-dir data/predictions --label <label>
```

Reinstall after a broken editable install:

```bash
uv pip install --config-settings editable_mode=compat -e ".[dev]"
```

## Things that bite

### Snapshot cache invalidation

`build_snapshot` returns an existing snapshot untouched when a file with the same input
hash is already on disk. Two ways to break that silently:

- **A new input file not hashed into `_source_versions`.** Add a YAML under
  `data/hand_curated/` and wire it into the build without adding it to
  `_source_versions` in `data_engine/snapshot.py`, and editing that file leaves the
  hash unchanged — the user gets the stale snapshot back with no indication anything
  was ignored.
- **A transform-semantics change without a `SCHEMA_VERSION` bump.** The input hash
  covers inputs, not parser code. Any change to how `data_engine/transforms/` or the
  source parsers turn inputs into tables must bump `SCHEMA_VERSION`.

### Transfer-matrix bounds

`data_engine/transforms/transfer_matrix.py` derives how much of one party's vote moves
to another. Weights are clipped to `[0, 1]`, and the transferred total must never
exceed the consolidator's actual gain — an accounting identity resting on shares
summing to 100, which `load_byelections` validates at ingest (99.5–100.5 tolerance).

Be suspicious of any change that makes a weight larger without a stated reason. The
formula was wrong once in exactly that direction: it credited a source party's total
vote loss to the consolidator when most of that vote had gone to the threat party.

### pandas concat with an empty frame

`pd.concat` onto a 0-row `pd.DataFrame(columns=[...])` emits a `FutureWarning` and is a
real forward-compat hazard: pandas currently *excludes* the empty frame from dtype
inference and will stop doing so, which can silently turn a float column into an object
column. Casting one column afterwards does not protect the others. Declare dtypes at
construction, or skip the concat when the base frame is empty.

The suite must stay warning-free — CI runs `-W error::FutureWarning`. Suppressing a
warning with a filter instead of fixing its cause is a defect, not a fix.

### Assertions that cannot fail

`Nation` and `PartyCode` are `(str, Enum)` subclasses, so `row["nation"] == "england"`
is true whether the column holds the string or the enum — it does not verify what it
appears to.

Expected values in tests should be derived independently — hand-computed from the
fixture, or the spec's stated figure — never recomputed with the same expression the
implementation uses. A test that mirrors the code passes when both are wrong.

## Domain context

Transfer weights are **inferences from aggregate share deltas** across a handful of
by-elections. There is no voter-movement data in this project — no panel study, no
recontact survey — so they cannot distinguish vote switching from differential turnout.
Some blocks rest on a single event.

Do not describe these weights as measured voter behaviour. Values in
`data/hand_curated/transfer_overrides.yaml` are deliberate analyst judgement calls,
each carrying a required `rationale`; they are not derived and should not be
"corrected" toward derived values. Changing those numbers is an editorial decision.

Anything under `prediction_engine/strategies/` changes published seat counts. A change
there should say what it does to the forecast.

## Known limitation, not yet fixed

`_identify_consolidator` applies a bare `gain > 0` test with no magnitude threshold, so
when the whole left bloc falls a noise-level mover takes the consolidator role — Runcorn
picks the Lib Dems on +0.44pp while Labour fell 14.2; Hamilton picks the Greens on
+0.80pp while the SNP fell 16.6. Every `england/ld` and `scotland/green` matrix cell
descends from those two picks. The `scale` correction suppresses the impact (all land
below 0.02). A minimum-gain threshold of 2.0pp, matching `PRIOR_SHARE_THRESHOLD`, would
drop both blocks and let `matrix_unavailable` fire honestly. See
`docs/superpowers/specs/2026-07-26-transfer-matrix-correction-design.md`.
