"""
Shared state models. Mutated by background workers (LogWatcher, SlurmPoller),
read by the UI refresh timer. No locking needed — both run in the same asyncio
event loop (Textual's), so updates are cooperative and interleaved safely.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RuleStats:
    name: str
    done: int = 0
    running: int = 0
    pending: int = 0
    failed: int = 0


@dataclass
class SlurmJob:
    slurm_id: str
    smk_jobid: Optional[int] = None
    rule_name: str = "unknown"
    state: str = "PENDING"
    node: str = "-"
    cpus: int = 0
    mem_mb: int = 0
    elapsed_secs: int = 0

    @property
    def elapsed_str(self) -> str:
        h, rem = divmod(self.elapsed_secs, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}" if self.elapsed_secs else "-"

    @property
    def mem_str(self) -> str:
        if not self.mem_mb:
            return "-"
        return f"{self.mem_mb / 1024:.1f}G" if self.mem_mb >= 1024 else f"{self.mem_mb}M"

    @property
    def state_short(self) -> str:
        return {
            "RUNNING": "RUN",
            "COMPLETING": "CMP",
            "PENDING": "PEN",
            "FAILED": "ERR",
            "CANCELLED": "CAN",
            "TIMEOUT": "TMO",
            "COMPLETED": "DONE",
        }.get(self.state, self.state[:4])


@dataclass
class WorkflowState:
    log_path: Optional[str] = None
    workflow_name: str = "workflow"
    start_time: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)

    # rule name → RuleStats
    rules: dict[str, RuleStats] = field(default_factory=dict)

    # Snakemake internal job id → rule name
    jobid_to_rule: dict[int, str] = field(default_factory=dict)

    # Snakemake job id → Slurm job id
    smk_to_slurm: dict[int, str] = field(default_factory=dict)

    # Slurm job id → SlurmJob
    slurm_jobs: dict[str, SlurmJob] = field(default_factory=dict)

    # Slurm IDs we expect to still be in squeue
    active_slurm_ids: set[str] = field(default_factory=set)

    # Ring buffer of raw log lines (capped at 500)
    log_lines: list[str] = field(default_factory=list)

    # How many log_lines the UI has already consumed
    log_cursor: int = 0

    # Total expected jobs (parsed from "N of M steps" lines)
    total_expected: int = 0

    # Set True when workflow finishes cleanly
    finished: bool = False
    exit_code: Optional[int] = None

    # ── derived counters ──────────────────────────────────────────

    @property
    def total_done(self) -> int:
        return sum(r.done for r in self.rules.values())

    @property
    def total_running(self) -> int:
        return sum(r.running for r in self.rules.values())

    @property
    def total_pending(self) -> int:
        return sum(r.pending for r in self.rules.values())

    @property
    def total_failed(self) -> int:
        return sum(r.failed for r in self.rules.values())

    @property
    def progress_pct(self) -> float:
        denom = self.total_expected or (
            self.total_done + self.total_running + self.total_pending + self.total_failed
        )
        return min(100.0, self.total_done / denom * 100) if denom else 0.0

    @property
    def elapsed_str(self) -> str:
        secs = int(time.time() - self.start_time)
        h, rem = divmod(secs, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    @property
    def cpus_in_use(self) -> int:
        return sum(
            j.cpus for j in self.slurm_jobs.values()
            if j.state in ("RUNNING", "COMPLETING")
        )

    @property
    def mem_gb_in_use(self) -> float:
        return sum(
            j.mem_mb for j in self.slurm_jobs.values()
            if j.state in ("RUNNING", "COMPLETING")
        ) / 1024

    # ── helpers ───────────────────────────────────────────────────

    def get_or_create_rule(self, name: str) -> RuleStats:
        if name not in self.rules:
            self.rules[name] = RuleStats(name=name)
        return self.rules[name]

    def push_log(self, line: str) -> None:
        self.log_lines.append(line)
        if len(self.log_lines) > 500:
            # Trim oldest; adjust cursor so UI doesn't re-read them
            trim = len(self.log_lines) - 500
            self.log_lines = self.log_lines[trim:]
            self.log_cursor = max(0, self.log_cursor - trim)
        self.last_update = time.time()

    def drain_new_log_lines(self) -> list[str]:
        """Return lines the UI hasn't seen yet, advance cursor."""
        new = self.log_lines[self.log_cursor:]
        self.log_cursor = len(self.log_lines)
        return new
