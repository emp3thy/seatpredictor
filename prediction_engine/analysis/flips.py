from pathlib import Path

import pandas as pd

from prediction_engine.sqlite_io import read_prediction_seats


def compute_flips(run_a: Path, run_b: Path) -> pd.DataFrame:
    """Return seats whose predicted_winner differs between two runs.
    Columns: ons_code, constituency_name, winner_a, winner_b.
    Empty DataFrame if no flips."""
    a = read_prediction_seats(run_a).set_index("ons_code")
    b = read_prediction_seats(run_b).set_index("ons_code")
    common = a.index.intersection(b.index)
    rows: list[dict] = []
    for ons in sorted(common):
        wa = a.loc[ons, "predicted_winner"]
        wb = b.loc[ons, "predicted_winner"]
        if wa != wb:
            rows.append({
                "ons_code": ons,
                "constituency_name": a.loc[ons, "constituency_name"],
                "winner_a": wa, "winner_b": wb,
            })
    return pd.DataFrame(rows, columns=pd.Index(["ons_code", "constituency_name", "winner_a", "winner_b"]))
