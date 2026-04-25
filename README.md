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
