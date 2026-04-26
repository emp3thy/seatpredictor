import logging
from pathlib import Path

import click

from prediction_engine.runner import run_prediction
from prediction_engine.strategies.base import STRATEGY_REGISTRY
from prediction_engine import strategies as _strategies  # noqa: F401  populates registry
from schema.prediction import ReformThreatConfig, UniformSwingConfig


@click.group()
def main():
    """Prediction engine: list strategies, run a prediction, sweep configs, diff runs."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)-30s %(message)s",
        datefmt="%H:%M:%S",
    )


@main.command("list-strategies")
def list_strategies_cmd():
    for name in sorted(STRATEGY_REGISTRY):
        click.echo(name)


def _make_config(
    strategy: str,
    *,
    polls_window_days: int,
    multiplier: float | None,
    clarity_threshold: float | None,
):
    if strategy == "uniform_swing":
        return UniformSwingConfig(polls_window_days=polls_window_days)
    if strategy == "reform_threat_consolidation":
        return ReformThreatConfig(
            polls_window_days=polls_window_days,
            multiplier=multiplier if multiplier is not None else 1.0,
            clarity_threshold=clarity_threshold if clarity_threshold is not None else 5.0,
        )
    raise click.ClickException(f"unknown strategy: {strategy}")


@main.command("run")
@click.option("--snapshot", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--strategy", type=str, required=True)
@click.option("--out-dir", type=click.Path(file_okay=False, path_type=Path), required=True)
@click.option("--label", type=str, default="baseline")
@click.option("--multiplier", type=float, default=None)
@click.option("--clarity-threshold", type=float, default=None)
@click.option("--polls-window-days", type=int, default=14)
def run_cmd(
    snapshot: Path,
    strategy: str,
    out_dir: Path,
    label: str,
    multiplier: float | None,
    clarity_threshold: float | None,
    polls_window_days: int,
) -> None:
    cfg = _make_config(
        strategy,
        polls_window_days=polls_window_days,
        multiplier=multiplier,
        clarity_threshold=clarity_threshold,
    )
    out = run_prediction(
        snapshot_path=snapshot,
        strategy_name=strategy,
        scenario=cfg,
        out_dir=out_dir,
        label=label,
    )
    click.echo(f"Prediction at {out}")


@main.command("sweep")
@click.option("--snapshot", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--strategy", type=str, required=True)
@click.option("--out-dir", type=click.Path(file_okay=False, path_type=Path), required=True)
@click.option("--label-prefix", type=str, default="swp")
@click.option("--multiplier", type=str, required=True, help="Comma-separated, e.g. 0.5,1.0,1.5")
@click.option("--clarity-threshold", type=float, default=5.0)
@click.option("--polls-window-days", type=int, default=14)
def sweep_cmd(
    snapshot: Path,
    strategy: str,
    out_dir: Path,
    label_prefix: str,
    multiplier: str,
    clarity_threshold: float,
    polls_window_days: int,
) -> None:
    multipliers = [float(x.strip()) for x in multiplier.split(",")]
    for m in multipliers:
        cfg = _make_config(
            strategy,
            polls_window_days=polls_window_days,
            multiplier=m,
            clarity_threshold=clarity_threshold,
        )
        label = f"{label_prefix}_m{m:.2f}".replace(".", "p")
        out = run_prediction(
            snapshot_path=snapshot,
            strategy_name=strategy,
            scenario=cfg,
            out_dir=out_dir,
            label=label,
        )
        click.echo(f"  m={m:.2f} -> {out.name}")


@main.command("diff")
@click.argument("run_a", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("run_b", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def diff_cmd(run_a: Path, run_b: Path) -> None:
    from prediction_engine.analysis.flips import compute_flips
    flips = compute_flips(run_a, run_b)
    if flips.empty:
        click.echo("no flips between the two runs")
        return
    click.echo(f"{len(flips)} flips between {run_a.name} and {run_b.name}:")
    for _, r in flips.iterrows():
        click.echo(f"  {r['ons_code']:11s} {r['constituency_name']:30s} {r['winner_a']} -> {r['winner_b']}")
