import json
from pathlib import Path

import pandas as pd

from prediction_engine.sqlite_io import read_prediction_config, read_prediction_national


def collect_sweep(prediction_paths: list[Path]) -> pd.DataFrame:
    """For a sweep of prediction files, return overall national totals per run as a long DataFrame.
    Columns: run_id, multiplier, clarity_threshold, party, seats. multiplier and clarity_threshold
    come from the run's scenario_config_json (NaN if the strategy doesn't expose them).
    """
    rows: list[dict] = []
    for p in prediction_paths:
        cfg = read_prediction_config(p)
        scenario = json.loads(cfg.scenario_config_json)
        multiplier = scenario.get("multiplier")
        clarity_threshold = scenario.get("clarity_threshold")
        nat = read_prediction_national(p)
        overall = nat.loc[nat["scope"] == "overall"]
        for _, r in overall.iterrows():
            rows.append({
                "run_id": cfg.run_id,
                "multiplier": multiplier,
                "clarity_threshold": clarity_threshold,
                "party": r["party"],
                "seats": int(r["seats"]),
            })
    return pd.DataFrame(rows, columns=pd.Index(["run_id", "multiplier", "clarity_threshold", "party", "seats"]))
