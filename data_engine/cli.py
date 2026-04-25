from datetime import date
from pathlib import Path

import click
import httpx
from data_engine.raw_cache import RawCache
from data_engine.snapshot import BuildSnapshotConfig, build_snapshot
from data_engine.sources.wikipedia_polls import POLLS_URL, fetch_polls_html


HOC_URL = "https://researchbriefings.files.parliament.uk/documents/CBP-10009/HoC-GE2024-results-by-constituency.csv"
USER_AGENT = "seatpredictor/0.0.1 (research)"


def _project_root() -> Path:
    return Path.cwd()


def _raw_cache() -> RawCache:
    return RawCache(root=_project_root() / "data" / "raw_cache")


@click.group()
def main():
    """Data engine: fetch sources, build snapshots."""


@main.command()
@click.option("--refresh", is_flag=True, default=False, help="Force re-fetch even if cached.")
def fetch(refresh: bool):
    """Refresh raw cache for today: download polls + HoC results."""
    today = date.today()
    cache = _raw_cache()

    polls_key = cache.key("wikipedia_polls", today)
    if refresh or not cache.exists(polls_key):
        click.echo(f"Fetching polls from {POLLS_URL}")
        html = fetch_polls_html(POLLS_URL)
        cache.put(polls_key, html.encode("utf-8"), meta={"url": POLLS_URL})
    else:
        click.echo(f"Polls cached for {today}; skipping (use --refresh to force)")

    hoc_key = cache.key("hoc_results", today)
    if refresh or not cache.exists(hoc_key):
        click.echo(f"Fetching HoC results from {HOC_URL}")
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=60.0) as client:
            resp = client.get(HOC_URL)
            resp.raise_for_status()
            cache.put(hoc_key, resp.content, meta={"url": HOC_URL})
    else:
        click.echo(f"HoC results cached for {today}; skipping")


@main.command()
@click.option(
    "--as-of",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="As-of date (YYYY-MM-DD). Defaults to today.",
)
def snapshot(as_of):
    """Build a snapshot from the raw cache + by-elections YAML."""
    as_of_date = (as_of.date() if as_of else date.today())
    cfg = BuildSnapshotConfig(
        as_of_date=as_of_date,
        raw_cache=_raw_cache(),
        out_dir=_project_root() / "data" / "snapshots",
        byelections_yaml=_project_root() / "data" / "hand_curated" / "by_elections.yaml",
    )
    path = build_snapshot(cfg)
    click.echo(f"Snapshot at {path}")


@main.command()
@click.option("--since", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--every-days", type=int, default=7)
def backfill(since, every_days: int):
    """One-time: produce snapshots back to --since, every --every-days."""
    from datetime import timedelta
    start = since.date()
    today = date.today()
    cur = start
    cache = _raw_cache()
    out_dir = _project_root() / "data" / "snapshots"
    yaml_path = _project_root() / "data" / "hand_curated" / "by_elections.yaml"
    while cur <= today:
        cfg = BuildSnapshotConfig(
            as_of_date=cur,
            raw_cache=cache,
            out_dir=out_dir,
            byelections_yaml=yaml_path,
        )
        try:
            path = build_snapshot(cfg)
            click.echo(f"  {cur} -> {path.name}")
        except FileNotFoundError as e:
            click.echo(f"  {cur} -> SKIP (cache miss: {e})")
        cur = cur + timedelta(days=every_days)
