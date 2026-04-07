"""
Tails a Snakemake log file and drives WorkflowState transitions.

Key log patterns we watch for (Snakemake ≥ 7 with a Slurm profile):

  [timestamp]
  rule <name>:         → rule block starts; create pending entry
      input: ...
      jobid: N         → maps smk job id to current rule
  localrule <name>:    → local rule (not submitted to Slurm)
      jobid: N

  Submitted job N with external jobid 'SLURM_ID'  → pending→running, store mapping
  Finished job N.                                   → running→done
  Error in rule <name>:                             → running→failed
  N of M steps (P%) done                            → update total_expected
  Nothing to be done.                               → mark finished

The parser is a simple line-at-a-time state machine; no regex back-tracking.
"""
from __future__ import annotations

import asyncio
import re
import time
from pathlib import Path

from .models import SlurmJob, WorkflowState

# ── compiled patterns ─────────────────────────────────────────────────────────

_RE_RULE       = re.compile(r'^rule (\w+):')
_RE_LOCAL      = re.compile(r'^localrule (\w+):')
_RE_JOBID      = re.compile(r'^\s+jobid:\s+(\d+)')
# Handles all executor backends:
#   slurm-profile : Submitted job N with external jobid 'ID'
#   drmaa         : Submitted DRMAA job N with external jobid ID.
#   cluster-generic: Submitted job N with external jobid ID
_RE_SUBMITTED  = re.compile(
    r"Submitted(?:\s+\w+)?\s+job\s+(\d+)\s+with\s+external\s+jobid\s+['\"]?(\w+)['\"]?\.?\s*$"
)
# Handles 'Finished job N.' and 'Finished DRMAA job N.'
_RE_FINISHED   = re.compile(r'Finished(?:\s+\w+)?\s+job\s+(\d+)\.')
_RE_ERROR_RULE = re.compile(r'^Error in rule (\w+):')
_RE_PROGRESS   = re.compile(r'(\d+) of (\d+) steps')
_RE_NOTHING    = re.compile(r'Nothing to be done', re.IGNORECASE)


class LogWatcher:
    """
    Async tail-and-parse of a Snakemake log file.
    Runs as an asyncio task inside Textual's event loop.
    """

    def __init__(self, state: WorkflowState, poll_interval: float = 0.3) -> None:
        self.state = state
        self.poll_interval = poll_interval
        self._running = False

        # Parser state machine
        self._cur_rule: str | None = None
        self._cur_local: bool = False
        self._cur_smk_id: int | None = None
        self._in_error: bool = False
        self._error_rule: str | None = None

    # ── public API ────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Main loop — wait for file, then tail it indefinitely."""
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

        # ── rule header ──────────────────────────────────────────────────────
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

        # ── jobid inside a rule block ────────────────────────────────────────
        m = _RE_JOBID.match(line)
        if m and self._cur_rule and not self._in_error:
            self._cur_smk_id = int(m.group(1))
            s.jobid_to_rule[self._cur_smk_id] = self._cur_rule
            return

        # ── submitted to Slurm ───────────────────────────────────────────────
        m = _RE_SUBMITTED.search(line)
        if m:
            smk_id = int(m.group(1))
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

        # ── finished cleanly ─────────────────────────────────────────────────
        m = _RE_FINISHED.search(line)
        if m:
            smk_id = int(m.group(1))
            rule_name = s.jobid_to_rule.get(smk_id, "unknown")
            rule = s.get_or_create_rule(rule_name)
            rule.running = max(0, rule.running - 1)
            rule.done += 1

            slurm_id = s.smk_to_slurm.get(smk_id)
            if slurm_id:
                s.active_slurm_ids.discard(slurm_id)
                if slurm_id in s.slurm_jobs:
                    s.slurm_jobs[slurm_id].state = "COMPLETED"
            return

        # ── error ────────────────────────────────────────────────────────────
        m = _RE_ERROR_RULE.match(line)
        if m:
            self._in_error = True
            self._error_rule = m.group(1)
            rule = s.get_or_create_rule(self._error_rule)
            rule.running = max(0, rule.running - 1)
            rule.failed += 1

            # Find slurm job for this rule and mark it failed
            for smk_id, rname in s.jobid_to_rule.items():
                if rname == self._error_rule:
                    sid = s.smk_to_slurm.get(smk_id)
                    if sid and sid in s.slurm_jobs:
                        s.slurm_jobs[sid].state = "FAILED"
                        s.active_slurm_ids.discard(sid)
            return

        # ── progress counter ─────────────────────────────────────────────────
        m = _RE_PROGRESS.search(line)
        if m:
            s.total_expected = int(m.group(2))
            return

        # ── finished workflow ────────────────────────────────────────────────
        if _RE_NOTHING.search(line):
            s.finished = True
