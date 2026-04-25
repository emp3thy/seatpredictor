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
