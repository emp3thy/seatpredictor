import json
from pathlib import Path

from prediction_engine.sqlite_io import read_prediction_seats, read_prediction_config
from schema.common import PartyCode


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
        "leader": row["leader"],
        "consolidator": row["consolidator"],
        "clarity": row["clarity"],
        "matrix_nation": row["matrix_nation"],
        "matrix_provenance": json.loads(row["matrix_provenance"]) if row["matrix_provenance"] else [],
        "notes": json.loads(row["notes"]) if row["notes"] else [],
        "predicted_winner": row["predicted_winner"],
        "predicted_margin": float(row["predicted_margin"]),
        "run_id": cfg.run_id,
        "strategy": cfg.strategy,
    }
