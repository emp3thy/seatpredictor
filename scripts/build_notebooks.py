"""Generate the analysis notebooks from this script.

Run me with: uv run python scripts/build_notebooks.py
The notebooks are committed to git alongside this script. To edit a cell, edit
the dict in NOTEBOOK_SPECS below and rerun.
"""
from pathlib import Path

import nbformat
from nbformat.v4 import new_notebook, new_code_cell, new_markdown_cell


_REPO_ROOT = Path(__file__).resolve().parent.parent


# Prepended to every notebook so cells can use plain Path("data/...") even when
# the kernel cwd is the notebook's directory (JupyterLab default) or anywhere
# else inside the repo. Walks up from cwd to the first pyproject.toml and
# chdirs there. Also exposes _pick_prediction() so the data-loading cells select
# the intended baseline run by label, not by alphabetical filename order — which
# breaks once sweep_* files share the directory.
_PRELUDE = '''import os
from pathlib import Path

def _find_repo_root() -> Path:
    p = Path.cwd().resolve()
    for candidate in [p, *p.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise RuntimeError("could not find repo root (no pyproject.toml in cwd or any parent)")

os.chdir(_find_repo_root())

def _latest_snapshot_hash() -> str:
    """Return the content hash of the most recent snapshot. Filenames are
    YYYY-MM-DD__v<schema>__<hash>.sqlite, so lexical sort = chronological."""
    snaps = sorted(Path("data/snapshots").glob("*.sqlite"))
    if not snaps:
        raise FileNotFoundError("no snapshots in data/snapshots/; run /snapshot first")
    return snaps[-1].stem.split("__")[-1]

def _pick_prediction(strategy_marker: str, label: str) -> Path:
    """Select the prediction file for the LATEST snapshot whose name contains
    strategy_marker AND label. Prediction filenames are
    <snap_hash>__<strategy>__<config_hash>__<label>.sqlite. Fails loud if 0 or
    >1 files match — the previous sorted(glob)[-1] silently picked an
    alphabetically-last file, which became nondeterministic once sweep_* runs
    or older snapshots' predictions shared the directory."""
    snap_hash = _latest_snapshot_hash()
    pred_dir = Path("data/predictions")
    matches = [
        p for p in pred_dir.glob(f"{snap_hash}__*{strategy_marker}*.sqlite")
        if label in p.name
    ]
    if not matches:
        raise FileNotFoundError(
            f"no prediction in {pred_dir} for snapshot {snap_hash} matching "
            f"strategy={strategy_marker!r} label={label!r}; run /predict first"
        )
    if len(matches) > 1:
        names = sorted(p.name for p in matches)
        raise RuntimeError(
            f"multiple predictions for snapshot {snap_hash} match "
            f"strategy={strategy_marker!r} label={label!r}: {names}; "
            f"remove duplicates or pass a more specific label"
        )
    return matches[0]'''


_NB_01_TITLE_MD = "# Polling trends\n\nPer-party 7-day rolling mean from the GB national-VI poll table. Sanity-checks the data engine output."
_NB_01_LOAD = '''from pathlib import Path
import matplotlib.pyplot as plt
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.analysis.poll_trends import rolling_trend

snap_path = sorted(Path("data/snapshots").glob("*.sqlite"))[-1]
snap = Snapshot(snap_path)
trend = rolling_trend(snap, window_days=7, geography="GB")
trend.tail()'''
_NB_01_PLOT = '''ax = trend.plot(figsize=(10, 5))
ax.set_ylabel("Vote share (%)")
ax.set_title(f"7-day rolling per-party national VI trend (as of {snap.manifest.as_of_date})")
plt.show()'''
_NB_01_INTERP = "Lines should be smooth (no sub-cell spikes); Reform should sit above other parties when the snapshot's GB national VI shows it leading."

_NB_02_TITLE_MD = "# Constituency drilldown\n\nPick a seat. Show projected raw shares, the consolidator, clarity, matrix entries, flows, and the final prediction."
_NB_02_LOAD = '''import sqlite3
from contextlib import closing
from prediction_engine.analysis.drilldown import explain_seat

prediction_path = _pick_prediction("reform_threat_consolidation", "baseline_rtc")
with closing(sqlite3.connect(str(prediction_path))) as conn:
    cur = conn.execute("SELECT ons_code FROM seats WHERE notes != '[]' ORDER BY ons_code LIMIT 1")
    row = cur.fetchone()
ons_code = row[0]
report = explain_seat(prediction_path, ons_code=ons_code)
report'''
_NB_02_TABLE = '''import pandas as pd
pd.DataFrame({"raw": report["share_raw"], "predicted": report["share_predicted"]}).T'''
_NB_02_INTERP = "Expect lab/plaid/snp/green's share_predicted > share_raw on Reform-threat seats; the parties in `matrix_provenance` are the by-elections that contributed."

_NB_03_TITLE_MD = "# Strategy comparison\n\nuniform_swing vs reform_threat_consolidation. List flips; chart national-total deltas."
_NB_03_LOAD = '''from prediction_engine.analysis.flips import compute_flips
from prediction_engine.sqlite_io import read_prediction_national

us_run  = _pick_prediction("uniform_swing", "baseline_us")
rtc_run = _pick_prediction("reform_threat_consolidation", "baseline_rtc")
flips = compute_flips(us_run, rtc_run)
flips.head(20)'''
_NB_03_PLOT = '''import matplotlib.pyplot as plt
import pandas as pd
nat_us  = read_prediction_national(us_run)
nat_rtc = read_prediction_national(rtc_run)
us_overall  = nat_us [nat_us ["scope"] == "overall"].set_index("party")["seats"]
rtc_overall = nat_rtc[nat_rtc["scope"] == "overall"].set_index("party")["seats"]
pd.DataFrame({"uniform_swing": us_overall, "reform_threat": rtc_overall}).plot.bar(figsize=(8, 4))
plt.ylabel("Seats")
plt.title("National totals: uniform_swing vs reform_threat_consolidation")
plt.show()'''
_NB_03_INTERP = "If reform_threat trims Reform seats vs uniform_swing while raising Lab/LD/Green/Plaid/SNP, the consolidation strategy is firing as expected."

_NB_04_TITLE_MD = "# Scenario sweep\n\nSweep `multiplier`. Plot per-party national seat counts."
_NB_04_RUN = '''from pathlib import Path
from prediction_engine.runner import run_prediction
from prediction_engine.analysis.sweep import collect_sweep
from schema.prediction import ReformThreatConfig

pred_dir = Path("data/predictions")
snap_path = sorted(Path("data/snapshots").glob("*.sqlite"))[-1]
sweep_paths = []
for m in (0.5, 0.75, 1.0, 1.25, 1.5):
    out = run_prediction(
        snapshot_path=snap_path,
        strategy_name="reform_threat_consolidation",
        scenario=ReformThreatConfig(multiplier=m),
        out_dir=pred_dir,
        label=f"sweep_m{m:.2f}".replace(".", "p"),
    )
    sweep_paths.append(out)

summary = collect_sweep(sweep_paths)
summary'''
_NB_04_PLOT = '''import matplotlib.pyplot as plt
pivot = summary.pivot(index="multiplier", columns="party", values="seats").fillna(0)
pivot.plot(figsize=(10, 5), marker="o")
plt.ylabel("Seats")
plt.title("National seat count vs reform-threat multiplier")
plt.show()'''
_NB_04_INTERP = "Reform's line should be monotonically decreasing; the consolidator parties' lines monotonically increasing."

_NB_05_TITLE_MD = """# Reform polling bias

For every electoral event in our snapshot (by-elections + curated local-election PNS), \
compare the pre-event 7-day national poll mean for Reform against the actual Reform result. \
Aggregate to a single recommended `--reform-polling-correction-pp` value.

**Caveats:** by-elections and local elections are weighted equally (see spec §5.2). \
Per-pollster bias is shown but is descriptive — most pollsters appear in too few events for \
statistically powerful estimates."""

_NB_05_LOAD = '''from pathlib import Path
from data_engine.sources.local_elections import load_local_elections
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.analysis.poll_bias import compute_reform_bias, write_bias_json

snap_path = sorted(Path("data/snapshots").glob("*.sqlite"))[-1]
snap = Snapshot(snap_path)
local_yaml = Path("data/hand_curated/local_elections.yaml")
local_events = load_local_elections(local_yaml)
print(f"Snapshot: {snap_path.name}")
print(f"Local-election events loaded: {len(local_events)}")
print(f"By-election events in snapshot: {len(snap.byelections_events)}")'''

_NB_05_COMPUTE = '''import pandas as pd
result = compute_reform_bias(snap, local_elections=local_events)
per_event_df = pd.DataFrame(result.per_event)
per_event_df'''

_NB_05_PER_POLLSTER = '''per_pollster_df = pd.DataFrame.from_dict(result.per_pollster, orient="index")
per_pollster_df.sort_values("mean_bias_pp", ascending=False)'''

_NB_05_HEADLINE = '''print(f"Aggregate Reform polling bias: {result.aggregate_bias_pp:+.2f} pp")
print(f"Events used: {result.n_events_used} (with polls in window: {result.n_events_with_polls})")
print()
print(f"Recommended CLI flag:")
print(f"  --reform-polling-correction-pp {result.recommended_reform_polling_correction_pp:+.2f}")'''

_NB_05_WRITE = '''out_path = Path("data/derived/reform_polling_bias.json")
write_bias_json(result, snap, local_elections_yaml_path=local_yaml, out_path=out_path)
print(f"Wrote {out_path}")'''

_NB_05_INTERP = """A positive aggregate means pollsters under-state Reform; pass `+aggregate` to \
`seatpredict-predict --reform-polling-correction-pp`. Negative means pollsters over-state — pass the negative value. \
Per-pollster numbers below `n_events_with_polls = 3` are flagged `low` reliability and should be \
read as descriptive only."""


NOTEBOOK_SPECS = [
    ("01_polling_trends.ipynb", [
        ("md", _NB_01_TITLE_MD),
        ("code", _PRELUDE),
        ("code", _NB_01_LOAD),
        ("code", _NB_01_PLOT),
        ("md", _NB_01_INTERP),
    ]),
    ("02_constituency_drilldown.ipynb", [
        ("md", _NB_02_TITLE_MD),
        ("code", _PRELUDE),
        ("code", _NB_02_LOAD),
        ("code", _NB_02_TABLE),
        ("md", _NB_02_INTERP),
    ]),
    ("03_strategy_comparison.ipynb", [
        ("md", _NB_03_TITLE_MD),
        ("code", _PRELUDE),
        ("code", _NB_03_LOAD),
        ("code", _NB_03_PLOT),
        ("md", _NB_03_INTERP),
    ]),
    ("04_scenario_sweep.ipynb", [
        ("md", _NB_04_TITLE_MD),
        ("code", _PRELUDE),
        ("code", _NB_04_RUN),
        ("code", _NB_04_PLOT),
        ("md", _NB_04_INTERP),
    ]),
    ("05_reform_polling_bias.ipynb", [
        ("md", _NB_05_TITLE_MD),
        ("code", _PRELUDE),
        ("code", _NB_05_LOAD),
        ("code", _NB_05_COMPUTE),
        ("code", _NB_05_PER_POLLSTER),
        ("code", _NB_05_HEADLINE),
        ("code", _NB_05_WRITE),
        ("md", _NB_05_INTERP),
    ]),
]


def build():
    out_dir = _REPO_ROOT / "notebooks"
    out_dir.mkdir(exist_ok=True)
    for name, cells in NOTEBOOK_SPECS:
        nb = new_notebook()
        nb.cells = [
            (new_markdown_cell(content) if kind == "md" else new_code_cell(content))
            for kind, content in cells
        ]
        # Pin Python kernel metadata so VS Code / JupyterLab pick the right kernel.
        nb.metadata["kernelspec"] = {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        }
        path = out_dir / name
        with path.open("w", encoding="utf-8") as f:
            nbformat.write(nb, f)
        print(f"wrote {path}")


if __name__ == "__main__":
    build()
