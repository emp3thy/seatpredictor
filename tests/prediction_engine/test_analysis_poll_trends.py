import pandas as pd
from prediction_engine.snapshot_loader import Snapshot
from prediction_engine.analysis.poll_trends import rolling_trend


def test_rolling_trend_returns_per_party_series(tiny_snapshot_path):
    snap = Snapshot(tiny_snapshot_path)
    trend = rolling_trend(snap, window_days=7)
    assert isinstance(trend, pd.DataFrame)
    # one column per party + 'date' index
    assert {"con", "lab", "ld", "reform", "green", "snp", "plaid", "other"} <= set(trend.columns)
