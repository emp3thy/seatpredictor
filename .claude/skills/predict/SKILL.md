---
name: predict
description: Use when the user wants to run the prediction engine on the latest snapshot, producing both the uniform_swing baseline and the reform_threat_consolidation tactical prediction. Triggers on phrases like "predict", "run predictions", "predict seats", "process", "analyze", "run strategies". Picks the most recent snapshot under `data/snapshots/` and writes prediction files to `data/predictions/`.
---

# /predict — run both strategies on the latest snapshot

Run the seatpredictor prediction engine: pick the most recent snapshot, run both the `uniform_swing` baseline and the `reform_threat_consolidation` tactical strategy, write both as single-file SQLite predictions to `data/predictions/`.

## Steps

1. Find the latest snapshot. Use `ls -1t` to sort by mtime (most recent first):

```bash
SNAP=$(ls -1t data/snapshots/*.sqlite 2>/dev/null | head -1)
```

If no snapshots exist (`$SNAP` empty), tell the user `/snapshot` is the prerequisite and stop.

2. Run both strategies, using the binary directly (NOT `uv run` — Plan A's editable-install gotcha):

```bash
.venv/Scripts/seatpredict-predict.exe run \
    --snapshot "$SNAP" \
    --strategy uniform_swing \
    --out-dir data/predictions \
    --label baseline_us

.venv/Scripts/seatpredict-predict.exe run \
    --snapshot "$SNAP" \
    --strategy reform_threat_consolidation \
    --out-dir data/predictions \
    --label baseline_rtc
```

The runner is idempotent on `(snapshot_content_hash, strategy, config_hash, label)`, so re-running on the same snapshot returns the existing files without re-computing.

3. Report both prediction file paths and a one-line summary of what each represents:
- `*_uniform_swing_*_baseline_us.sqlite` — uniform-swing baseline (no tactical adjustment)
- `*_reform_threat_consolidation_*_baseline_rtc.sqlite` — tactical strategy with all 7 flag paths

4. Suggest `/notebook` as the next step (open the four analysis notebooks against these predictions).

## PowerShell equivalent

If the user is in PowerShell rather than bash:

```powershell
$SNAP = (Get-ChildItem data/snapshots/*.sqlite | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
.venv/Scripts/seatpredict-predict.exe run --snapshot $SNAP --strategy uniform_swing --out-dir data/predictions --label baseline_us
.venv/Scripts/seatpredict-predict.exe run --snapshot $SNAP --strategy reform_threat_consolidation --out-dir data/predictions --label baseline_rtc
```

## Pitfalls

- If `ModuleNotFoundError` fires, the editable install has reverted. Fix: `uv pip install --config-settings editable_mode=compat -e ".[dev]"`.
- Custom multipliers / clarity thresholds: use `seatpredict-predict run --multiplier ... --clarity-threshold ...` directly. This skill always runs both strategies at their default config — for sweeps and one-off runs, invoke the CLI manually.
