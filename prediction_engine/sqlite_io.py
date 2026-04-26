import hashlib
import json
import logging
import re
import sqlite3
from contextlib import closing
from pathlib import Path

import pandas as pd

from data_engine.sqlite_io import open_snapshot_db, write_dataframe
from schema.prediction import RunConfig, ScenarioConfig

logger = logging.getLogger(__name__)


PREDICTION_SCHEMA_VERSION = 1
_LABEL_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def compute_config_hash(scenario: ScenarioConfig) -> str:
    payload = json.dumps(scenario.model_dump(mode="json"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def build_run_id(
    snapshot_content_hash: str, strategy: str, config_hash: str, label: str
) -> str:
    return f"{snapshot_content_hash}__{strategy}__{config_hash}__{label}"


def prediction_filename(
    *,
    out_dir: Path,
    snapshot_content_hash: str,
    strategy: str,
    config_hash: str,
    label: str,
) -> Path:
    if not _LABEL_RE.fullmatch(label):
        raise ValueError(f"invalid label {label!r}: must match {_LABEL_RE.pattern}")
    return out_dir / f"{snapshot_content_hash}__{strategy}__{config_hash}__{label}.sqlite"


def write_prediction_db(
    path: Path,
    *,
    seats: pd.DataFrame,
    national: pd.DataFrame,
    run_config: RunConfig,
) -> None:
    """Write a prediction SQLite file with seats / national / config / notes_index."""
    notes_index = _explode_notes(seats)
    cfg_payload = run_config.model_dump(mode="json")
    cfg_df = pd.DataFrame([cfg_payload])

    path.parent.mkdir(parents=True, exist_ok=True)
    with open_snapshot_db(path) as conn:
        write_dataframe(conn, "seats", seats)
        write_dataframe(conn, "national", national)
        write_dataframe(conn, "config", cfg_df)
        write_dataframe(conn, "notes_index", notes_index)
    logger.debug(
        "Wrote prediction %s (seats=%d, national=%d, notes_index=%d)",
        path.name, len(seats), len(national), len(notes_index),
    )


def _explode_notes(seats: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for _, r in seats.iterrows():
        flags = json.loads(r["notes"]) if r["notes"] else []
        for flag in flags:
            rows.append({"ons_code": r["ons_code"], "flag": flag})
    return pd.DataFrame(rows, columns=pd.Index(["ons_code", "flag"]))


def read_prediction_seats(path: Path) -> pd.DataFrame:
    with closing(sqlite3.connect(str(path))) as conn:
        return pd.read_sql_query("SELECT * FROM seats", conn)


def read_prediction_national(path: Path) -> pd.DataFrame:
    with closing(sqlite3.connect(str(path))) as conn:
        return pd.read_sql_query("SELECT * FROM national", conn)


def read_prediction_notes_index(path: Path) -> pd.DataFrame:
    with closing(sqlite3.connect(str(path))) as conn:
        return pd.read_sql_query("SELECT * FROM notes_index", conn)


def read_prediction_config(path: Path) -> RunConfig:
    with closing(sqlite3.connect(str(path))) as conn:
        df = pd.read_sql_query("SELECT * FROM config", conn)
    if len(df) != 1:
        raise ValueError(f"config table must have exactly 1 row, found {len(df)}")
    return RunConfig.model_validate(df.iloc[0].to_dict())
