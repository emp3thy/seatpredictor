import pandas as pd

from prediction_engine.snapshot_loader import Snapshot
from schema.common import PartyCode


def rolling_trend(snapshot: Snapshot, window_days: int = 7, geography: str = "GB") -> pd.DataFrame:
    """Return a rolling per-party poll average per published_date.
    Index: published_date (datetime). Columns: 'con','lab','ld','reform','green','snp','plaid','other'.
    """
    polls = snapshot.polls
    polls = polls.loc[polls["geography"] == geography].copy()
    polls["published_date"] = pd.to_datetime(polls["published_date"])
    polls = polls.sort_values(by="published_date").set_index("published_date")
    party_cols = [p.value for p in PartyCode]
    trend = polls[party_cols].rolling(f"{window_days}D").mean()
    return trend.dropna(how="all")
