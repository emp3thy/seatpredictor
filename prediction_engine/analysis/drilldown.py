import json
from pathlib import Path

import pandas as pd

from prediction_engine.sqlite_io import read_prediction_seats, read_prediction_config
from schema.common import PartyCode


def _nullable(v):
    """Coerce SQL NULLs / pandas NaN to None for JSON serialisation. Pandas reads NULL
    floats as float('nan'), and json.dumps(NaN) emits the JS literal `NaN` — invalid
    per RFC 8259 and rejected by strict parsers like jq. None serialises as null."""
    return None if pd.isna(v) else v


def explain_seat(prediction_path: Path, ons_code: str) -> dict:
    """Return a structured drill-down for one seat: raw shares, predicted shares,
    consolidator/clarity/flows, matrix provenance, notes flags. Used by the drilldown notebook."""
    seats = read_prediction_seats(prediction_path)
    matched = seats.loc[seats["ons_code"] == ons_code]
    if matched.empty:
        raise KeyError(f"seat {ons_code} not in prediction")
    row = matched.iloc[0].to_dict()

    cfg = read_prediction_config(prediction_path)
    return {
        "ons_code": row["ons_code"],
        "constituency_name": row["constituency_name"],
        "nation": row["nation"],
        "region": row["region"],
        "share_raw":       {p.value: float(row[f"share_raw_{p.value}"])       for p in PartyCode},
        "share_predicted": {p.value: float(row[f"share_predicted_{p.value}"]) for p in PartyCode},
        "leader": _nullable(row["leader"]),
        "consolidator": _nullable(row["consolidator"]),
        "clarity": _nullable(row["clarity"]),
        "matrix_nation": _nullable(row["matrix_nation"]),
        "matrix_provenance": json.loads(row["matrix_provenance"]) if row["matrix_provenance"] else [],
        "notes": json.loads(row["notes"]) if row["notes"] else [],
        "predicted_winner": row["predicted_winner"],
        "predicted_margin": float(row["predicted_margin"]),
        "run_id": cfg.run_id,
        "strategy": cfg.strategy,
    }
