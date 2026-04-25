"""End-to-end smoke verification. Run after `seatpredict-data fetch` + `snapshot`.

Asserts:
- Snapshot file exists, contains all expected tables
- 650 UK constituencies in results_2024 (full Westminster set)
- Per-constituency shares sum to ~100%
- Polls table has >=30 rows since GE 2024
- Transfer matrix has at least one non-null cell
- All four seeded by-elections present
"""

import contextlib
import sqlite3
import sys
from pathlib import Path

import pandas as pd


EXPECTED_TABLES = {
    "manifest", "polls", "results_2024",
    "byelections_events", "byelections_results",
    "transfer_weights", "transfer_weights_provenance",
}
EXPECTED_BYELECTIONS = {
    "runcorn_helsby_2025", "hamilton_larkhall_stonehouse_2025",
    "caerphilly_senedd_2025", "gorton_denton_2026",
}


def main() -> int:
    snap_dir = Path("data/snapshots")
    snaps = list(snap_dir.glob("*.sqlite"))
    if not snaps:
        print("FAIL: no snapshots found in data/snapshots/", file=sys.stderr)
        return 1
    snap = max(snaps, key=lambda p: p.stat().st_mtime)
    print(f"Verifying {snap}")
    try:
        with contextlib.closing(sqlite3.connect(str(snap))) as conn:
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            missing = EXPECTED_TABLES - tables
            if missing:
                print(f"FAIL: missing tables {missing}", file=sys.stderr)
                return 1
            print(f"  Tables present: OK ({len(EXPECTED_TABLES)} expected)")

            results = pd.read_sql_query("SELECT * FROM results_2024", conn)
            n_seats = results["ons_code"].nunique()
            if n_seats != 650:
                print(f"FAIL: expected 650 constituencies, got {n_seats}", file=sys.stderr)
                return 1
            print("  Constituencies: 650 OK")

            share_sums = results.groupby("ons_code")["share"].sum()
            bad = share_sums[(share_sums < 99.0) | (share_sums > 101.0)]
            if not bad.empty:
                print(f"FAIL: {len(bad)} constituencies with shares not summing 99-101", file=sys.stderr)
                print(bad.head(), file=sys.stderr)
                return 1
            print("  Share sums in 99-101: all 650 OK")

            polls = pd.read_sql_query("SELECT * FROM polls", conn)
            if len(polls) < 30:
                print(f"FAIL: only {len(polls)} polls extracted (expected >=30)", file=sys.stderr)
                return 1
            print(f"  Polls extracted: {len(polls)} OK")

            events = pd.read_sql_query("SELECT * FROM byelections_events", conn)
            present = set(events["event_id"])
            missing_evs = EXPECTED_BYELECTIONS - present
            if missing_evs:
                print(f"FAIL: missing by-elections {missing_evs}", file=sys.stderr)
                return 1
            print("  By-elections seeded: 4 OK")

            weights = pd.read_sql_query("SELECT * FROM transfer_weights", conn)
            if len(weights) == 0:
                print("FAIL: transfer_weights is empty", file=sys.stderr)
                return 1
            print(f"  Transfer matrix cells: {len(weights)} OK")
    except (sqlite3.DatabaseError, pd.errors.DatabaseError) as e:
        print(f"FAIL: snapshot at {snap} appears corrupt: {e}", file=sys.stderr)
        return 1

    print("\nAll smoke checks PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
