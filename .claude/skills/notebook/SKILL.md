---
name: notebook
description: Use when the user wants to open the analysis Jupyter notebooks in a browser to inspect predictions visually. Triggers on phrases like "notebook", "jupyter", "open notebooks", "view results", "explore predictions", "open lab". Starts JupyterLab in the background and reminds the user of which notebook does what.
---

# /notebook — open the analysis notebooks in JupyterLab

Start JupyterLab pointed at the seatpredictor repo so the user can open any of the four analysis notebooks against the latest snapshot + predictions.

## Steps

1. Confirm prerequisites — at least one snapshot and (for notebooks 02/03/04) at least one prediction file. List what's present:

```bash
echo "Snapshots:" && ls -1t data/snapshots/*.sqlite 2>/dev/null | head -3
echo "Predictions:" && ls -1t data/predictions/*.sqlite 2>/dev/null | head -3
```

If snapshots are missing, suggest `/snapshot`. If predictions are missing for notebooks 02–04, suggest `/predict`.

2. Start JupyterLab in the background. From the **repo root** (this matters — notebooks use relative paths like `Path("data/snapshots")`):

```bash
uv run jupyter lab --no-browser
```

(Run this with `run_in_background=true` so the slash command can return without blocking. The server stays running; the user presses Ctrl+C in that terminal to stop it later.)

The first line of output will be a URL like `http://localhost:8888/lab?token=...` — surface that URL to the user.

3. Print the four notebook descriptions so the user knows which to open:

| File | What it does |
|---|---|
| `notebooks/01_polling_trends.ipynb` | per-party 7-day rolling poll average; sanity-check the data engine |
| `notebooks/02_constituency_drilldown.ipynb` | one seat: raw shares → consolidator → flows → predicted shares |
| `notebooks/03_strategy_comparison.ipynb` | uniform_swing vs reform_threat_consolidation; flips and bar charts |
| `notebooks/04_scenario_sweep.ipynb` | sweep `multiplier`; plot per-party seats as a function of multiplier |

## Pitfalls

- Always start JupyterLab from the repo root, not from `notebooks/`. The notebooks reference `data/snapshots` and `data/predictions` relative to the working directory; running `jupyter lab` from `notebooks/` would make those paths fail.
- If the user has never registered the project venv as a Jupyter kernel, the kernel selector will be empty. Fix: `uv run python -m ipykernel install --user --name python3 --display-name "Python 3 (seatpredictor)"`.
- Notebook 04 invokes `run_prediction()` directly to build the sweep — this means you don't need to pre-run `/predict` for that one. But notebooks 02 and 03 read existing prediction files, so `/predict` first is required for them.
