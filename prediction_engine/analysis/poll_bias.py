import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from data_engine.sources.local_elections import LocalElectionEvent
from prediction_engine.snapshot_loader import Snapshot

logger = logging.getLogger(__name__)


# Per spec 5.2 (post-revision): all event types weight equally - by-elections
# are a behavioural turnout-validation signal, not noise to suppress.
EVENT_WEIGHTS: dict[str, float] = {"ge": 1.0, "local_election": 1.0, "by_election": 1.0}
FINAL_WEEK_WINDOW_DAYS: int = 7
SCHEMA_VERSION: int = 1


@dataclass
class BiasResult:
    """Output of compute_reform_bias."""
    aggregate_bias_pp: float
    recommended_reform_polling_correction_pp: float
    n_events_used: int
    n_events_with_polls: int
    per_event: list[dict] = field(default_factory=list)
    per_pollster: dict[str, dict] = field(default_factory=dict)
    method: dict = field(default_factory=dict)


def _normalise_pollster(name: str) -> str:
    """Canonical lowercase-snake-case key. Used for the per_pollster dict."""
    return name.strip().lower().replace(" ", "_")


def _final_week_polls(polls: pd.DataFrame, event_date: date,
                      window_days: int = FINAL_WEEK_WINDOW_DAYS) -> pd.DataFrame:
    """GB polls published in [event_date - window_days, event_date - 1] with non-NaN reform."""
    if polls.empty:
        return polls
    lo = (event_date - timedelta(days=window_days)).isoformat()
    hi = (event_date - timedelta(days=1)).isoformat()
    mask = (
        (polls["geography"] == "GB")
        & (polls["published_date"] >= lo)
        & (polls["published_date"] <= hi)
        & polls["reform"].notna()
    )
    return polls.loc[mask]


def _byelection_actual_reform(event_id: str, by_results: pd.DataFrame) -> float | None:
    """Return Reform's actual share at this by-election, or None if Reform didn't stand
    (no row in by_results for party='reform'). Caller must treat None as 'exclude
    from bias aggregate' — see compute_reform_bias's by-election walker."""
    rows = by_results.loc[by_results["event_id"] == event_id]
    if rows.empty:
        raise ValueError(f"no by-election results for event_id={event_id}")
    reform_rows = rows.loc[rows["party"] == "reform"]
    if reform_rows.empty:
        return None
    return float(reform_rows["actual_share"].iloc[0])


def _local_actual_reform(local: LocalElectionEvent) -> tuple[float, str]:
    """Return (Reform's PNS share, consolidated_method) for a local-election event.
    Reform always defaults to 0.0 if missing from consolidated_shares (a national PNS
    metric where Reform stands GB-wide; absent key means the source omitted them, which
    is treated as 0% rather than excluded)."""
    share = float(local.consolidated_shares.get("reform", 0.0))
    return share, local.consolidated_method


def compute_reform_bias(
    snapshot: Snapshot,
    local_elections: list[LocalElectionEvent] | None = None,
    *,
    weights: dict[str, float] = EVENT_WEIGHTS,
    final_week_window_days: int = FINAL_WEEK_WINDOW_DAYS,
) -> BiasResult:
    """Compute Reform polling bias by comparing pre-event national poll means
    against actual results across by-elections + local elections.

    Per-pollster keys are normalised to lowercase snake_case (e.g. "More in Common" -> "more_in_common").
    Events without any final-week polls are listed in per_event with bias_pp=None
    and excluded from the aggregate denominator. If no events have polls,
    aggregate_bias_pp = recommended = 0.0 (explicit no-op).

    Reliability for per-pollster: 'high' if n_events_with_polls >= 3 else 'low'.
    """
    polls = snapshot.polls
    by_events = snapshot.byelections_events
    by_results = snapshot.byelections_results

    per_event_rows: list[dict] = []
    per_pollster_collect: dict[str, list[tuple[float, float]]] = {}   # name -> [(bias_pp, event_weight), ...]

    # Walk by-elections
    for _, row in by_events.iterrows():
        event_id = str(row["event_id"])
        event_date = date.fromisoformat(str(row["date"]))
        actual_reform = _byelection_actual_reform(event_id, by_results)
        if actual_reform is None:
            # Reform didn't stand — record descriptively, exclude from aggregate.
            per_event_rows.append({
                "event_id": event_id, "type": "by_election",
                "date": event_date.isoformat(),
                "actual_share_pp": None, "actual_source": None,
                "poll_mean_share_pp": None, "bias_pp": None,
                "weight": weights["by_election"],
                "n_polls_in_window": 0, "pollsters_in_window": [],
            })
            continue
        window = _final_week_polls(polls, event_date, final_week_window_days)
        if window.empty:
            per_event_rows.append({
                "event_id": event_id, "type": "by_election",
                "date": event_date.isoformat(),
                "actual_share_pp": actual_reform, "actual_source": None,
                "poll_mean_share_pp": None, "bias_pp": None,
                "weight": weights["by_election"],
                "n_polls_in_window": 0, "pollsters_in_window": [],
            })
            continue
        poll_mean = float(window["reform"].mean())
        bias = actual_reform - poll_mean
        per_event_rows.append({
            "event_id": event_id, "type": "by_election",
            "date": event_date.isoformat(),
            "actual_share_pp": actual_reform, "actual_source": None,
            "poll_mean_share_pp": poll_mean, "bias_pp": bias,
            "weight": weights["by_election"],
            "n_polls_in_window": int(len(window)),
            "pollsters_in_window": sorted({_normalise_pollster(p) for p in window["pollster"]}),
        })
        for pollster_name in window["pollster"].unique():
            pollster_rows = window.loc[window["pollster"] == pollster_name]
            pollster_mean = float(pollster_rows["reform"].mean())
            per_pollster_collect.setdefault(_normalise_pollster(str(pollster_name)), []).append(
                (actual_reform - pollster_mean, weights["by_election"])
            )

    # Walk local elections
    for ev in (local_elections or []):
        event_id = f"{ev.date.isoformat().replace('-', '_')}_local"
        actual_reform, source_method = _local_actual_reform(ev)
        window = _final_week_polls(polls, ev.date, final_week_window_days)
        if window.empty:
            per_event_rows.append({
                "event_id": event_id, "type": "local_election",
                "date": ev.date.isoformat(),
                "actual_share_pp": actual_reform, "actual_source": source_method,
                "poll_mean_share_pp": None, "bias_pp": None,
                "weight": weights["local_election"],
                "n_polls_in_window": 0, "pollsters_in_window": [],
            })
            continue
        poll_mean = float(window["reform"].mean())
        bias = actual_reform - poll_mean
        per_event_rows.append({
            "event_id": event_id, "type": "local_election",
            "date": ev.date.isoformat(),
            "actual_share_pp": actual_reform, "actual_source": source_method,
            "poll_mean_share_pp": poll_mean, "bias_pp": bias,
            "weight": weights["local_election"],
            "n_polls_in_window": int(len(window)),
            "pollsters_in_window": sorted({_normalise_pollster(p) for p in window["pollster"]}),
        })
        for pollster_name in window["pollster"].unique():
            pollster_rows = window.loc[window["pollster"] == pollster_name]
            pollster_mean = float(pollster_rows["reform"].mean())
            per_pollster_collect.setdefault(_normalise_pollster(str(pollster_name)), []).append(
                (actual_reform - pollster_mean, weights["local_election"])
            )

    # Aggregate
    eligible = [(e["bias_pp"], e["weight"]) for e in per_event_rows if e["bias_pp"] is not None]
    if eligible:
        num = sum(b * w for b, w in eligible)
        den = sum(w for _, w in eligible)
        aggregate = num / den if den > 0 else 0.0
    else:
        aggregate = 0.0

    # Per-pollster: weighted mean using the SAME event weights as the overall aggregate.
    # This keeps the per-pollster decomposition semantically consistent with the headline
    # number - both are weighted by event_type.
    per_pollster: dict[str, dict] = {}
    for name, biases_and_weights in per_pollster_collect.items():
        n = len(biases_and_weights)
        num = sum(b * w for b, w in biases_and_weights)
        den = sum(w for _, w in biases_and_weights)
        per_pollster[name] = {
            "mean_bias_pp": float(num / den) if den > 0 else 0.0,
            "n_events_with_polls": n,
            "reliability": "high" if n >= 3 else "low",
        }

    method = {
        "description": "actual_minus_final_week_poll_mean, weighted",
        "final_week_window_days": final_week_window_days,
        "geography": "GB",
        "weights": dict(weights),
    }

    n_events_used = len(per_event_rows)
    n_events_with_polls = sum(1 for e in per_event_rows if e["bias_pp"] is not None)

    logger.info(
        "Reform bias: aggregate=%.2fpp from %d/%d events; %d pollsters",
        aggregate, n_events_with_polls, n_events_used, len(per_pollster),
    )

    return BiasResult(
        aggregate_bias_pp=aggregate,
        recommended_reform_polling_correction_pp=aggregate,
        n_events_used=n_events_used,
        n_events_with_polls=n_events_with_polls,
        per_event=per_event_rows,
        per_pollster=per_pollster,
        method=method,
    )


def write_bias_json(
    result: BiasResult,
    snapshot: Snapshot,
    local_elections_yaml_path: Path | None,
    out_path: Path,
) -> Path:
    """Serialise BiasResult to JSON per spec 5.4 schema. Atomic write via tmp+rename.
    Returns out_path on success."""
    yaml_hash: str | None = None
    if local_elections_yaml_path is not None and local_elections_yaml_path.exists():
        yaml_hash = hashlib.sha256(local_elections_yaml_path.read_bytes()).hexdigest()[:12]

    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "derived_from_snapshot_hash": snapshot.manifest.content_hash,
        "derived_from_snapshot_as_of_date": snapshot.manifest.as_of_date.isoformat(),
        "derived_from_local_elections_yaml_sha256": yaml_hash,
        "method": {
            **result.method,
            "interpretation_note":
                "positive aggregate.bias_pp means pollsters under-state Reform; "
                "the recommended_reform_polling_correction_pp can be passed to "
                "seatpredict-predict via --reform-polling-correction-pp.",
        },
        "events_used": result.n_events_used,
        "events_with_polls": result.n_events_with_polls,
        "aggregate": {
            "bias_pp": result.aggregate_bias_pp,
            "interpretation": "positive value: pollsters under-state Reform on average; "
                              "recommended correction = +bias_pp",
            "recommended_reform_polling_correction_pp": result.recommended_reform_polling_correction_pp,
        },
        "per_pollster": result.per_pollster,
        "per_event": result.per_event,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(out_path)
    logger.info("Wrote bias JSON to %s (aggregate=%+.2fpp)", out_path, result.aggregate_bias_pp)
    return out_path
