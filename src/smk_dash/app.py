"""
smk-dash Textual application.

Layout (approximate):
  ┌─────────────────────────── smk-dash ─────────────────────────────┐
  │ Header: workflow name + clock                                     │
  ├────────────────────┬─────────────────────────────────────────────┤
  │ OverviewPanel      │ SlurmJobsPanel                              │
  │  progress bar      │  scrollable live job table                  │
  │  counters          ├─────────────────────────────────────────────┤
  ├────────────────────┤ ResourcePanel                               │
  │ RuleTablePanel     │  CPU / Mem gauges                           │
  │  per-rule D/R/P/F  │                                             │
  ├────────────────────┴─────────────────────────────────────────────┤
  │ LogPanel  (scrolling tail, last 200 lines)                       │
  └──────────────────────────────────────────────────────────────────┘
"""
from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Optional

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, ScrollableContainer
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Label,
    Log,
    ProgressBar,
    Rule,
    Static,
)
from textual import work

from .models import WorkflowState
from .watcher import LogWatcher
from .slurm import SlurmPoller

# ── colour constants ──────────────────────────────────────────────────────────

_STATE_STYLE = {
    "RUNNING":    "bold green",
    "COMPLETING": "cyan",
    "PENDING":    "yellow",
    "FAILED":     "bold red",
    "CANCELLED":  "red",
    "TIMEOUT":    "red",
    "COMPLETED":  "dim",
}


# ── individual panels ─────────────────────────────────────────────────────────

class OverviewPanel(Static):
    DEFAULT_CSS = """
    OverviewPanel {
        border: round $primary;
        padding: 1 2;
        height: auto;
        min-height: 12;
    }
    OverviewPanel Label.title {
        text-style: bold;
        color: $accent;
        padding-bottom: 1;
    }
    """

    def compose(self) -> ComposeResult:
        yield Label("◈ OVERVIEW", classes="title")
        yield Static("", id="ov-bar")
        yield Static("", id="ov-stats")

    def refresh_data(self, state: WorkflowState) -> None:
        done    = state.total_done
        running = state.total_running
        pending = state.total_pending
        failed  = state.total_failed
        total   = state.total_expected or (done + running + pending + failed) or 1
        pct     = int(state.progress_pct)

        W = 28
        filled = min(W, int(W * pct / 100))
        bar    = f"[bold green]{'█' * filled}[/][dim]{'░' * (W - filled)}[/]"

        status = ""
        if state.finished:
            status = "  [bold green]✔ Workflow complete[/]"
        elif failed:
            status = f"  [bold red]⚠ {failed} job(s) failed[/]"

        self.query_one("#ov-bar").update(f" {bar}  [bold]{pct}%[/]")
        self.query_one("#ov-stats").update(
            f"\n"
            f"  [bold green]✓[/] Done     [bold]{done:>4}[/]\n"
            f"  [yellow]⟳[/] Running  [bold]{running:>4}[/]\n"
            f"  [dim]○[/] Pending  [bold]{pending:>4}[/]\n"
            f"  [red]✗[/] Failed   [bold]{failed:>4}[/]\n"
            f"\n"
            f"  [dim]Total {total} jobs  ⏱ {state.elapsed_str}[/]"
            + (f"\n{status}" if status else "")
        )


class RuleTablePanel(Static):
    DEFAULT_CSS = """
    RuleTablePanel {
        border: round $primary;
        height: 1fr;
    }
    RuleTablePanel Label.title {
        background: $primary-darken-2;
        color: $text;
        text-align: center;
        width: 100%;
        text-style: bold;
        padding: 0 1;
    }
    RuleTablePanel DataTable {
        height: 1fr;
    }
    """

    def compose(self) -> ComposeResult:
        yield Label("◈ RULE BREAKDOWN", classes="title")
        yield DataTable(id="rule-dt", show_cursor=False, zebra_stripes=True)

    def on_mount(self) -> None:
        dt = self.query_one("#rule-dt", DataTable)
        dt.add_columns("Rule", "✓", "⟳", "○", "✗")
        dt.fixed_columns = 1

    def refresh_data(self, state: WorkflowState) -> None:
        dt = self.query_one("#rule-dt", DataTable)
        dt.clear()
        for rule in sorted(state.rules.values(), key=lambda r: r.name):
            dt.add_row(
                rule.name,
                f"[green]{rule.done}[/]"  if rule.done    else "[dim]·[/]",
                f"[yellow]{rule.running}[/]" if rule.running else "[dim]·[/]",
                str(rule.pending)          if rule.pending else "[dim]·[/]",
                f"[red]{rule.failed}[/]"  if rule.failed  else "[dim]·[/]",
            )


class SlurmJobsPanel(Static):
    DEFAULT_CSS = """
    SlurmJobsPanel {
        border: round $primary;
        height: 1fr;
    }
    SlurmJobsPanel Label.title {
        background: $primary-darken-2;
        color: $text;
        text-align: center;
        width: 100%;
        text-style: bold;
        padding: 0 1;
    }
    SlurmJobsPanel DataTable {
        height: 1fr;
    }
    """

    def compose(self) -> ComposeResult:
        yield Label("◈ SLURM JOBS (live)", classes="title")
        yield DataTable(id="slurm-dt", show_cursor=True, zebra_stripes=True)

    def on_mount(self) -> None:
        dt = self.query_one("#slurm-dt", DataTable)
        dt.add_columns("Slurm ID", "Rule", "State", "Node", "CPUs", "Mem", "Elapsed")

    def refresh_data(self, state: WorkflowState) -> None:
        dt = self.query_one("#slurm-dt", DataTable)
        dt.clear()

        # Sort: running first, then by slurm_id descending
        order = {"RUNNING": 0, "COMPLETING": 1, "PENDING": 2}
        jobs = sorted(
            state.slurm_jobs.values(),
            key=lambda j: (order.get(j.state, 3), -int(j.slurm_id)),
        )

        for job in jobs[:80]:
            style = _STATE_STYLE.get(job.state, "white")
            dt.add_row(
                job.slurm_id,
                job.rule_name,
                f"[{style}]{job.state_short}[/]",
                job.node,
                str(job.cpus) if job.cpus else "-",
                job.mem_str,
                job.elapsed_str,
            )


class ResourcePanel(Static):
    DEFAULT_CSS = """
    ResourcePanel {
        border: round $primary;
        padding: 1 2;
        height: auto;
        min-height: 8;
    }
    ResourcePanel Label.title {
        text-style: bold;
        color: $accent;
        padding-bottom: 1;
    }
    """

    def __init__(self, max_cpus: int = 512, max_mem_gb: int = 2048, **kwargs) -> None:
        super().__init__(**kwargs)
        self.max_cpus = max_cpus
        self.max_mem_gb = max_mem_gb

    def compose(self) -> ComposeResult:
        yield Label("◈ RESOURCES IN USE", classes="title")
        yield Static("", id="res-content")

    def refresh_data(self, state: WorkflowState) -> None:
        cpus   = state.cpus_in_use
        mem_gb = state.mem_gb_in_use
        n_running = sum(
            1 for j in state.slurm_jobs.values()
            if j.state in ("RUNNING", "COMPLETING")
        )

        def _bar(val: float, max_val: float, width: int = 22) -> str:
            if max_val <= 0:
                return "[dim]" + "░" * width + "[/]"
            frac = min(1.0, val / max_val)
            filled = int(width * frac)
            color = "green" if frac < 0.75 else ("yellow" if frac < 0.9 else "red")
            return f"[{color}]{'█' * filled}[/][dim]{'░' * (width - filled)}[/]"

        self.query_one("#res-content").update(
            f"  CPUs  {_bar(cpus, self.max_cpus)}  "
            f"[bold]{cpus}[/][dim]/{self.max_cpus}[/]\n"
            f"  Mem   {_bar(mem_gb, self.max_mem_gb)}  "
            f"[bold]{mem_gb:.1f}[/][dim]/{self.max_mem_gb} GB[/]\n"
            f"\n"
            f"  [dim]Active Slurm jobs: [bold white]{n_running}[/][/]"
        )


class LogPanel(Static):
    DEFAULT_CSS = """
    LogPanel {
        border: round $primary;
        height: 10;
    }
    LogPanel Label.title {
        background: $primary-darken-2;
        color: $text;
        text-align: center;
        width: 100%;
        text-style: bold;
        padding: 0 1;
    }
    LogPanel Log {
        height: 1fr;
    }
    """

    def compose(self) -> ComposeResult:
        yield Label("◈ LOG (tail)", classes="title")
        yield Log(id="log-widget", highlight=True, max_lines=300)

    def push_lines(self, lines: list[str]) -> None:
        log = self.query_one("#log-widget", Log)
        for line in lines:
            log.write_line(line)


# ── main app ──────────────────────────────────────────────────────────────────

class SmkDashApp(App):
    """smk-dash: live Snakemake + Slurm dashboard."""

    CSS = """
    Screen {
        background: $background;
        layers: base;
    }
    #body {
        height: 1fr;
    }
    #left-col {
        width: 32;
        min-width: 30;
        height: 1fr;
    }
    #right-col {
        width: 1fr;
        height: 1fr;
    }
    #right-top {
        height: 1fr;
    }
    """

    BINDINGS = [
        Binding("q",     "quit",         "Quit"),
        Binding("r",     "force_refresh","Refresh"),
        Binding("ctrl+l","clear_log",    "Clear log"),
    ]

    TITLE = "smk-dash"

    def __init__(
        self,
        log_path: Optional[str] = None,
        workflow_name: str = "workflow",
        poll_interval: float = 5.0,
        demo_mode: bool = False,
        demo_speed: float = 3.0,
        max_cpus: int = 512,
        max_mem_gb: int = 2048,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.state = WorkflowState(
            log_path=log_path,
            workflow_name=workflow_name,
        )
        self.poll_interval = poll_interval
        self.demo_mode = demo_mode
        self.demo_speed = demo_speed
        self.max_cpus = max_cpus
        self.max_mem_gb = max_mem_gb

    # ── layout ────────────────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="body"):
            with Vertical(id="left-col"):
                yield OverviewPanel(id="overview")
                yield RuleTablePanel(id="rule-table")
            with Vertical(id="right-col"):
                with Horizontal(id="right-top"):
                    yield SlurmJobsPanel(id="slurm-jobs")
                yield ResourcePanel(
                    max_cpus=self.max_cpus,
                    max_mem_gb=self.max_mem_gb,
                    id="resources",
                )
        yield LogPanel(id="log-panel")
        yield Footer()

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def on_mount(self) -> None:
        self.sub_title = self.state.workflow_name
        self._start_background_tasks()
        self.set_interval(1.5, self._refresh_ui)

    @work(exclusive=False)
    async def _start_background_tasks(self) -> None:
        tasks = []
        if self.demo_mode:
            from .demo import DemoDriver
            driver = DemoDriver(self.state, speed=self.demo_speed)
            tasks.append(driver.run())
        elif self.state.log_path:
            watcher = LogWatcher(self.state)
            poller  = SlurmPoller(self.state, self.poll_interval)
            tasks   = [watcher.run(), poller.run()]

        if tasks:
            await asyncio.gather(*tasks)

    # ── UI refresh ────────────────────────────────────────────────────────────

    def _refresh_ui(self) -> None:
        s = self.state

        self.query_one("#overview",    OverviewPanel).refresh_data(s)
        self.query_one("#rule-table",  RuleTablePanel).refresh_data(s)
        self.query_one("#slurm-jobs",  SlurmJobsPanel).refresh_data(s)
        self.query_one("#resources",   ResourcePanel).refresh_data(s)

        new_lines = s.drain_new_log_lines()
        if new_lines:
            self.query_one("#log-panel", LogPanel).push_lines(new_lines)

        # Update subtitle with elapsed time
        self.sub_title = f"{s.workflow_name}  ⏱ {s.elapsed_str}"

    # ── actions ───────────────────────────────────────────────────────────────

    def action_force_refresh(self) -> None:
        self._refresh_ui()

    def action_clear_log(self) -> None:
        self.query_one("#log-panel", LogPanel).query_one(
            "#log-widget", Log
        ).clear()

    def action_quit(self) -> None:
        self.exit()
