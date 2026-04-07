"""
Tails a Snakemake log file and drives WorkflowState transitions.

Snakemake produces subtly different log formats depending on the executor
backend. This table maps the key lines we watch:

  Event            slurm-profile                    drmaa
  ─────────────────────────────────────────────────────────────────────────
  Job submitted    Submitted job N with external    Submitted DRMAA job N
                     jobid 'SLURM_ID'                 with external jobid ID.
  Job finished     Finished job N.                  Finished jobid: N
                                                      (Rule: rulename)
  Job failed       Error in rule name:              Error in rule name:
  Workflow done    Nothing to be done.  (no-op)     15 of 15 steps (100%) done
                                                    Complete log(s): ...

The _RE_SUBMITTED / _RE_FINISHED / _RE_FINISHED_JOBID patterns cover all
the variants above.  _RE_COMPLETE detects a successful run finishing.

The parser is a simple line-at-a-time state machine; no regex back-tracking.
"""
from __future__ import annotations

import asyncio
import re
import time
from pathlib import Path

from .models import SlurmJob, WorkflowState

# ── compiled patterns ─────────────────────────────────────────────────────────

_RE_RULE  = re.compile(r'^rule (\w+):')
_RE_LOCAL = re.compile(r'^localrule (\w+):')
_RE_JOBID = re.compile(r'^\s+jobid:\s+(\d+)')

# Submitted — all executor variants:
#   slurm-profile : "Submitted job 5 with external jobid '15446085'"
#   drmaa         : "Submitted DRMAA job 5 with external jobid 15446085."
#   cluster-generic:"Submitted job 5 with external jobid 15446085"
_RE_SUBMITTED = re.compile(
    r"Submitted(?:\s+\w+)?\s+job\s+(\d+)\s+with\s+external\s+jobid\s+['\"]?(\w+)['\"]?\.?\s*$"
)

# Finished — slurm-profile / cluster-generic / old drmaa:
#   "Finished job 5."   "Finished DRMAA job 5."
_RE_FINISHED = re.compile(r'Finished(?:\s+\w+)?\s+job\s+(\d+)\.')

# Finished — newer drmaa executor:
#   "Finished jobid: 5 (Rule: process)"
#   "Finished jobid: 0 (Rule: all)"   <- the localrule "all" target; skip it
_RE_FINISHED_JOBID = re.compile(r'Finished jobid:\s*(\d+)\s*\(Rule:\s*(\w+)\)')

_RE_ERROR_RULE = re.compile(r'^Error in rule (\w+):')

# Progress: "N of M steps (P%) done"
_RE_PROGRESS = re.compile(r'(\d+) of (\d+) steps(?:\s*\((\d+)%\)\s*done)?')

# Workflow complete signals:
#   - "Nothing to be done."          (nothing was out of date)
#   - "N of N steps (100%) done"     (successful run; also caught by _RE_PROGRESS)
#   - "Complete log(s):"             (Snakemake final summary line, drmaa)
_RE_NOTHING  = re.compile(r'Nothing to be done',  re.IGNORECASE)
_RE_COMPLETE = re.compile(r'Complete log\(s\)',    re.IGNORECASE)


class LogWatcher:
    """
    Async tail-and-parse of a Snakemake log file.
    Runs as an asyncio task inside Textual's event loop.
    """

    def __init__(self, state: WorkflowState, poll_interval: float = 0.3) -> None:
        self.state = state
        self.poll_interval = poll_interval
        self._running = False

        # Parser state machine context
        self._cur_rule: str | None = None
        self._cur_local: bool = False
        self._cur_smk_id: int | None = None
        self._in_error: bool = False
        self._error_rule: str | None = None

    # ── public API ────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Main loop — wait for the log file to appear, then tail it."""
        self._running = True
        path = Path(self.state.log_path)  # type: ignore[arg-type]

        while self._running and not path.exists():
            await asyncio.sleep(0.5)

        with open(path, "r", errors="replace") as fh:
            while self._running:
                line = fh.readline()
                if line:
                    self._process(line.rstrip("\n"))
                else:
                    await asyncio.sleep(self.poll_interval)

    def feed_line(self, line: str) -> None:
        """Feed one line directly (subprocess / demo mode)."""
        self._process(line.rstrip("\n"))

    def stop(self) -> None:
        self._running = False

    # ── internal parser ───────────────────────────────────────────────────────

    def _process(self, line: str) -> None:
        s = self.state
        s.push_log(line)

        # ── rule / localrule header ──────────────────────────────────────────
        m = _RE_RULE.match(line)
        if m:
            self._cur_rule = m.group(1)
            self._cur_local = False
            self._cur_smk_id = None
            self._in_error = False
            s.get_or_create_rule(self._cur_rule).pending += 1
            return

        m = _RE_LOCAL.match(line)
        if m:
            self._cur_rule = m.group(1)
            self._cur_local = True
            self._cur_smk_id = None
            self._in_error = False
            return

        # ── jobid line inside a rule block ───────────────────────────────────
        m = _RE_JOBID.match(line)
        if m and self._cur_rule and not self._in_error:
            self._cur_smk_id = int(m.group(1))
            s.jobid_to_rule[self._cur_smk_id] = self._cur_rule
            return

        # ── job submitted to Slurm ───────────────────────────────────────────
        m = _RE_SUBMITTED.search(line)
        if m:
            smk_id   = int(m.group(1))
            slurm_id = m.group(2)

            s.smk_to_slurm[smk_id] = slurm_id
            s.active_slurm_ids.add(slurm_id)

            rule_name = s.jobid_to_rule.get(smk_id, "unknown")
            rule = s.get_or_create_rule(rule_name)
            rule.pending = max(0, rule.pending - 1)
            rule.running += 1

            if slurm_id not in s.slurm_jobs:
                s.slurm_jobs[slurm_id] = SlurmJob(
                    slurm_id=slurm_id,
                    smk_jobid=smk_id,
                    rule_name=rule_name,
                    state="PENDING",
                )
            return

        # ── job finished (slurm-profile / cluster / old-drmaa) ──────────────
        m = _RE_FINISHED.search(line)
        if m:
            self._finish_smk_job(int(m.group(1)))
            return

        # ── job finished (newer drmaa) ───────────────────────────────────────
        #   "Finished jobid: 5 (Rule: process)"
        m = _RE_FINISHED_JOBID.search(line)
        if m:
            smk_id          = int(m.group(1))
            rule_from_log   = m.group(2)
            # Back-fill rule mapping if we're attaching to an existing log
            if smk_id not in s.jobid_to_rule and rule_from_log != "all":
                s.jobid_to_rule[smk_id] = rule_from_log
            # Skip the "all" localrule target — it's a DAG node, not a job
            if rule_from_log != "all":
                self._finish_smk_job(smk_id)
            return

        # ── job error ────────────────────────────────────────────────────────
        m = _RE_ERROR_RULE.match(line)
        if m:
            self._in_error = True
            self._error_rule = m.group(1)
            rule = s.get_or_create_rule(self._error_rule)
            rule.running = max(0, rule.running - 1)
            rule.failed += 1
            for smk_id, rname in s.jobid_to_rule.items():
                if rname == self._error_rule:
                    sid = s.smk_to_slurm.get(smk_id)
                    if sid and sid in s.slurm_jobs:
                        job = s.slurm_jobs[sid]
                        if job.state not in ("COMPLETED", "FAILED"):
                            job.state = "FAILED"
                            s.active_slurm_ids.discard(sid)
            return

        # ── progress counter "N of M steps (P%) done" ───────────────────────
        m = _RE_PROGRESS.search(line)
        if m:
            done_count = int(m.group(1))
            total      = int(m.group(2))
            pct        = int(m.group(3)) if m.group(3) else None
            s.total_expected = total
            # If Snakemake reports 100% done, the workflow is finished
            if pct == 100 or done_count == total:
                s.finished = True
            return

        # ── workflow completion signals ──────────────────────────────────────
        if _RE_NOTHING.search(line) or _RE_COMPLETE.search(line):
            s.finished = True
            return

    # ── helper ───────────────────────────────────────────────────────────────

    def _finish_smk_job(self, smk_id: int) -> None:
        """Transition a Snakemake job from running→done and sync Slurm state."""
        s = self.state
        rule_name = s.jobid_to_rule.get(smk_id, "unknown")
        rule = s.get_or_create_rule(rule_name)
        rule.running = max(0, rule.running - 1)
        rule.done += 1

        slurm_id = s.smk_to_slurm.get(smk_id)
        if slurm_id:
            s.active_slurm_ids.discard(slurm_id)
            job = s.slurm_jobs.get(slurm_id)
            if job and job.state not in ("FAILED", "CANCELLED", "TIMEOUT"):
                job.state = "COMPLETED"
