---
name: snapshot
description: Use when the user wants to refresh polling data and build a new SQLite snapshot for today. Triggers on phrases like "snapshot", "fetch polls", "refresh data", "new snapshot", "ingest", "update polls". Runs `seatpredict-data fetch` then `seatpredict-data snapshot`.
---

# /snapshot — refresh polls + build today's snapshot

Run the seatpredictor data engine to download today's polls + HoC 2024 results, then build a new SQLite snapshot for the current as-of date.

## Steps

1. Run the binary directly (NOT `uv run` — that reverts the editable install per Plan A's better-memory note):

```bash
.venv/Scripts/seatpredict-data.exe fetch
.venv/Scripts/seatpredict-data.exe snapshot
```

If you see `ModuleNotFoundError`, the editable install has reverted — fix with:

```bash
uv pip install --config-settings editable_mode=compat -e ".[dev]"
```

then retry the snapshot step.

2. Find the snapshot just produced and report its path:

```bash
ls -1t data/snapshots/*.sqlite | head -1
```

3. Tell the user:
- The new snapshot file path
- That `/predict` is the next natural step (runs both strategies on this snapshot)

## Pitfalls

- The `fetch` step is idempotent per `(source, fetch_date)` — re-running on the same day hits the cache. Pass `--refresh` if the user explicitly wants to bypass the cache.
- Re-running `snapshot` on the same day with the same raw cache contents is a no-op (returns the existing path) — that's the idempotency contract.
- If `seatpredict-data.exe` doesn't exist in `.venv/Scripts/`, the venv hasn't been installed yet — run `uv venv && uv pip install --config-settings editable_mode=compat -e ".[dev]"`.
