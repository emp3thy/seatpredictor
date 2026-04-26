import json
import logging
from pathlib import Path

import click

from prediction_engine.analysis.drilldown import explain_seat
from prediction_engine.analysis.flips import compute_flips


@click.group()
def main():
    """Analysis CLI for prediction outputs."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)-30s %(message)s",
        datefmt="%H:%M:%S",
    )


@main.command("drilldown")
@click.option("--run", "run_path", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--seat", type=str, required=True, help="ONS code, e.g. E14000123")
@click.option("--explain/--no-explain", default=False)
def drilldown_cmd(run_path: Path, seat: str, explain: bool):
    report = explain_seat(run_path, ons_code=seat)
    if not explain:
        click.echo(json.dumps(report, indent=2, default=str))
        return
    click.echo(f"Seat: {report['ons_code']} {report['constituency_name']} ({report['nation']}/{report['region']})")
    click.echo(f"  Run: {report['run_id']}  Strategy: {report['strategy']}")
    click.echo(f"  Predicted winner: {report['predicted_winner']} (margin {report['predicted_margin']:.2f})")
    click.echo(f"  Leader: {report['leader']}; Consolidator: {report['consolidator']}; Clarity: {report['clarity']}")
    click.echo(f"  Matrix nation: {report['matrix_nation']}; Provenance: {report['matrix_provenance']}")
    click.echo(f"  Notes: {report['notes']}")
    click.echo("  Raw -> Predicted:")
    for party in report["share_raw"]:
        raw = report["share_raw"][party]
        pred = report["share_predicted"][party]
        click.echo(f"    {party:7s}  {raw:5.1f}  ->  {pred:5.1f}  (Δ {pred - raw:+5.2f})")


@main.command("flips")
@click.option("--runs", nargs=2, type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
def flips_cmd(runs: tuple[Path, Path]):
    a, b = runs
    flips = compute_flips(a, b)
    if flips.empty:
        click.echo("no flips between the two runs")
        return
    click.echo(f"{len(flips)} flips between {a.name} and {b.name}:")
    for _, r in flips.iterrows():
        click.echo(f"  {r['ons_code']:11s} {r['constituency_name']:30s} {r['winner_a']} -> {r['winner_b']}")
