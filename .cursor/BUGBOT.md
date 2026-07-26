# Review guidance for seatpredictor

A UK Westminster seat-prediction model. Polling and 2024 results are ingested into
immutable SQLite snapshots; strategies read a snapshot and write prediction files.
Numbers here end up in a published forecast, so a silently wrong value is worse than
a crash.

## Highest-value things to catch

### Snapshot cache invalidation

`build_snapshot` returns an existing snapshot untouched when a file with the same
input hash is already on disk. Two ways to break this silently:

- **A new input file that isn't hashed into `_source_versions`.** If someone adds a
  hand-curated YAML under `data/hand_curated/` and wires it into the build without
  adding it to `_source_versions` in `data_engine/snapshot.py`, editing that file
  leaves the hash unchanged and the user gets the stale snapshot back with no
  indication anything was ignored. Flag any new build input that doesn't reach the
  hash.
- **A transform-semantics change without a `SCHEMA_VERSION` bump.** The input hash
  covers inputs, not parser code. Any change to how `data_engine/transforms/` or the
  source parsers turn inputs into tables must bump `SCHEMA_VERSION`, or cached
  snapshots built by the old code get reused. This is documented at the
  `_source_versions` definition; check it is honoured.

### Numeric correctness in the transfer matrix

`data_engine/transforms/transfer_matrix.py` derives how much of one party's vote
moves to another. Weights are constrained to `[0, 1]` and the transferred total must
never exceed the consolidator's actual gain — that bound is an accounting identity
resting on shares summing to 100, which `load_byelections` validates at ingest
(99.5–100.5 tolerance). Watch for changes that break the invariant, remove the
clipping, or divide by a quantity that can be zero.

Be suspicious of any change that makes a weight larger without a stated reason. The
formula was wrong once in exactly this direction: it credited a source party's total
vote loss to the consolidator, when most of that vote had gone to the threat party.

### Assertions that restate the implementation

Expected values in tests should be derived independently — hand-computed from the
fixture, or the spec's stated figure — not recomputed with the same expression the
implementation uses. A test that mirrors the code passes when both are wrong. Flag
assertions where the expected side reuses the production formula.

Related: an assertion that cannot fail. `Nation` and `PartyCode` are `(str, Enum)`
subclasses, so `row["nation"] == "england"` is true whether the column holds the
string or the enum — it does not verify what it appears to.

## Project conventions

- **Never `uv run`.** It reverts the editable install. Call the venv binaries
  directly (`.venv/Scripts/python.exe -m pytest`, `.venv/Scripts/seatpredict-data.exe`).
  Editable installs use `--config-settings editable_mode=compat`.
- **Generated artifacts are not tracked.** `data/snapshots/`, `data/predictions/` and
  `data/raw_cache/` are gitignored. A commit adding files there — especially via
  `git add -f` — is a mistake.
- **The test suite must stay warning-free.** CI runs `-W error::FutureWarning`.
  Suppressing a warning with a filter instead of fixing its cause is a defect, not a
  fix. A pandas `FutureWarning` on `concat` with an all-empty-column frame was a real
  forward-compatibility bug here: pandas currently excludes the empty frame from
  dtype inference and will stop doing so, which can silently turn a float column into
  an object column. Casting one column afterwards does not protect the others.
- **Strategy changes are higher-stakes than they look.** Anything under
  `prediction_engine/strategies/` changes published seat numbers. Ask whether the
  PR description says what the change does to the forecast; a strategy diff with no
  stated seat-count effect is worth flagging.

## Domain context worth knowing

Transfer weights are **inferences from aggregate share deltas** across a handful of
by-elections — there is no voter-movement data in this project, no panel study, no
recontact survey. They cannot distinguish vote switching from differential turnout.
Some blocks rest on a single event.

So: treat comments or docstrings that describe these weights as *measured* voter
behaviour as inaccurate. Values in `data/hand_curated/transfer_overrides.yaml` are
deliberate analyst judgement calls, each carrying a required `rationale` — they are
not derived and should not be "corrected" toward derived values. Changes to the
numbers in that file are editorial decisions; changes to its `rationale` text should
stay consistent with the number beside it.

## Calibration

Rank by consequence to the forecast. A wrong weight, a broken cache invalidation, or
a lost `clip` bound is serious — those change published numbers silently. Style
preferences, import ordering and line length are not worth a comment. Pandas type
stubs generate a lot of false positives in this repo (`list[str]` not assignable to
`Axes`, `sort_values` overloads); those are pre-existing noise, not findings.
