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

## Quick verification

After install, run:

```bash
uv run seatpredict-data fetch
uv run seatpredict-data snapshot
uv run python scripts/smoke_verify.py
```

The smoke verification asserts: 650 constituencies parsed, shares sum to ~100% per seat, ≥30 polls extracted, all four by-elections seeded, transfer matrix non-empty. If any check fails, the error message indicates which parser to inspect.

## Snapshots

Snapshots land in `data/snapshots/<as-of>__v<schema_version>__<content_hash>.sqlite`. Each is a self-contained SQLite file with seven tables: `manifest`, `polls`, `results_2024`, `byelections_events`, `byelections_results`, `transfer_weights`, `transfer_weights_provenance`. Use `data_engine.sqlite_io.open_snapshot_db` and `read_manifest` / `read_dataframe` to consume them programmatically.

Bumping `data_engine.snapshot.SCHEMA_VERSION` invalidates all old snapshot caches. Bump it whenever a parser change affects output (the input hash captures inputs, not parser code).
