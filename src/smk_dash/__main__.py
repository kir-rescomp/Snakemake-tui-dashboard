"""
smk-dash CLI

Commands:
  smk-dash watch --log <path>            Attach to existing Snakemake log
  smk-dash run -- snakemake ...          Wrap and monitor a new Snakemake run
  smk-dash demo                          Run with simulated workflow (no Slurm needed)
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from typing import Optional

import click


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx: click.Context) -> None:
    """smk-dash — live TUI dashboard for Snakemake workflows on Slurm."""
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# ── watch ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.option(
    "--log", "-l",
    required=True,
    type=click.Path(exists=False),
    help="Path to Snakemake log file (watched live; may not exist yet).",
)
@click.option("--name", "-n", default=None, help="Workflow name shown in header.")
@click.option("--poll", default=5.0, show_default=True, help="Slurm poll interval (s).")
@click.option("--max-cpus",   default=512,  show_default=True, help="Cluster CPU capacity for gauge.")
@click.option("--max-mem-gb", default=2048, show_default=True, help="Cluster memory capacity (GB) for gauge.")
def watch(
    log: str,
    name: Optional[str],
    poll: float,
    max_cpus: int,
    max_mem_gb: int,
) -> None:
    """Attach dashboard to an existing Snakemake log file."""
    from .app import SmkDashApp

    wf_name = name or Path(log).stem or "workflow"
    app = SmkDashApp(
        log_path=log,
        workflow_name=wf_name,
        poll_interval=poll,
        max_cpus=max_cpus,
        max_mem_gb=max_mem_gb,
    )
    app.run()


# ── run ───────────────────────────────────────────────────────────────────────

@cli.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
)
@click.option("--name", "-n", default=None, help="Workflow name shown in header.")
@click.option("--poll", default=5.0, show_default=True, help="Slurm poll interval (s).")
@click.option("--max-cpus",   default=512,  show_default=True)
@click.option("--max-mem-gb", default=2048, show_default=True)
@click.argument("snakemake_args", nargs=-1, type=click.UNPROCESSED)
def run(
    name: Optional[str],
    poll: float,
    max_cpus: int,
    max_mem_gb: int,
    snakemake_args: tuple[str, ...],
) -> None:
    """
    Launch Snakemake and display the dashboard simultaneously.

    Example:

      smk-dash run -- snakemake --profile slurm -j 100 --use-envmodules
    """
    if not snakemake_args:
        raise click.UsageError("Provide Snakemake arguments after '--'.")

    from .app import SmkDashApp

    # Write Snakemake stdout+stderr to a temp log file we can tail
    log_fd, log_path = tempfile.mkstemp(suffix=".smk-dash.log", prefix="smk-")
    os.close(log_fd)

    wf_name = name or _guess_workflow_name(snakemake_args) or "workflow"

    def _run_snakemake() -> None:
        with open(log_path, "w", buffering=1) as fh:
            proc = subprocess.Popen(
                list(snakemake_args),
                stdout=fh,
                stderr=subprocess.STDOUT,
                text=True,
            )
            proc.wait()

    t = threading.Thread(target=_run_snakemake, daemon=True)
    t.start()

    app = SmkDashApp(
        log_path=log_path,
        workflow_name=wf_name,
        poll_interval=poll,
        max_cpus=max_cpus,
        max_mem_gb=max_mem_gb,
    )
    try:
        app.run()
    finally:
        try:
            os.unlink(log_path)
        except OSError:
            pass


# ── demo ──────────────────────────────────────────────────────────────────────

@cli.command()
@click.option(
    "--speed", "-s",
    default=3.0,
    show_default=True,
    help="Simulation speed multiplier (>1 = faster).",
)
@click.option("--max-cpus",   default=512,  show_default=True)
@click.option("--max-mem-gb", default=2048, show_default=True)
def demo(speed: float, max_cpus: int, max_mem_gb: int) -> None:
    """
    Run the dashboard with a simulated RNA-seq pipeline.

    No Snakemake or Slurm installation required — great for testing the UI
    and showing the dashboard to others.

    Example:

      smk-dash demo --speed 5
    """
    from .app import SmkDashApp

    app = SmkDashApp(
        workflow_name="rnaseq_pipeline [DEMO]",
        demo_mode=True,
        demo_speed=speed,
        max_cpus=max_cpus,
        max_mem_gb=max_mem_gb,
    )
    app.run()


# ── helpers ───────────────────────────────────────────────────────────────────

def _guess_workflow_name(args: tuple[str, ...]) -> str:
    """Try to extract a meaningful name from snakemake args."""
    for i, arg in enumerate(args):
        if arg in ("--snakefile", "-s") and i + 1 < len(args):
            return Path(args[i + 1]).stem
        if arg.endswith(".smk") or arg.endswith("Snakefile"):
            return Path(arg).stem
    # Fall back to CWD name
    return Path.cwd().name
