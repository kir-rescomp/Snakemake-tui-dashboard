"""
Polls squeue periodically and updates SlurmJob entries in WorkflowState.

Uses `squeue --me --json` (Slurm 21+).  Falls back to a tabular parse if
the JSON flag isn't available (older clusters).

All I/O is async — runs as a task in Textual's event loop.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from .models import WorkflowState


class SlurmPoller:
    def __init__(self, state: WorkflowState, poll_interval: float = 5.0) -> None:
        self.state = state
        self.poll_interval = poll_interval
        self._running = False

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._poll()
            except Exception:
                pass  # never crash the background task
            await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        self._running = False

    # ── internals ─────────────────────────────────────────────────────────────

    async def _poll(self) -> None:
        raw_jobs = await _squeue_json()
        if raw_jobs is None:
            raw_jobs = await _squeue_tabular()

        if not raw_jobs:
            return

        for slurm_id, info in raw_jobs.items():
            if slurm_id in self.state.slurm_jobs:
                job = self.state.slurm_jobs[slurm_id]
                job.state = info["state"]
                job.node = info.get("node") or job.node
                if info.get("cpus"):
                    job.cpus = info["cpus"]
                if info.get("mem_mb"):
                    job.mem_mb = info["mem_mb"]
                job.elapsed_secs = info.get("elapsed_secs", 0)
            elif slurm_id in self.state.active_slurm_ids:
                # We've seen this Slurm ID in the log but not yet in slurm_jobs
                rule_name = _reverse_lookup(self.state, slurm_id)
                from .models import SlurmJob
                self.state.slurm_jobs[slurm_id] = SlurmJob(
                    slurm_id=slurm_id,
                    rule_name=rule_name,
                    state=info["state"],
                    node=info.get("node", "-"),
                    cpus=info.get("cpus", 0),
                    mem_mb=info.get("mem_mb", 0),
                    elapsed_secs=info.get("elapsed_secs", 0),
                )

        # Mark jobs that have disappeared from squeue as COMPLETED
        current_ids = set(raw_jobs.keys())
        for sid in list(self.state.active_slurm_ids):
            if sid not in current_ids:
                # Job is gone from squeue — treat as completed unless log said FAILED
                job = self.state.slurm_jobs.get(sid)
                if job and job.state not in ("FAILED", "CANCELLED", "TIMEOUT", "COMPLETED"):
                    job.state = "COMPLETED"


# ── squeue helpers ────────────────────────────────────────────────────────────

async def _squeue_json() -> dict[str, dict] | None:
    """Run squeue --me --json. Returns None if the flag is unsupported."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "squeue", "--me", "--json",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            return None
        data = json.loads(stdout.decode(errors="replace"))
        return _parse_json_jobs(data)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


async def _squeue_tabular() -> dict[str, dict] | None:
    """Fallback: squeue --me with fixed format string."""
    fmt = "%i|%T|%R|%C|%m|%M|%N"
    try:
        proc = await asyncio.create_subprocess_exec(
            "squeue", "--me", f"--format={fmt}", "--noheader",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            return None
        return _parse_tabular_jobs(stdout.decode(errors="replace"))
    except FileNotFoundError:
        return None


def _parse_json_jobs(data: dict) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for job in data.get("jobs", []):
        slurm_id = str(job.get("job_id", ""))
        if not slurm_id:
            continue

        state_raw = job.get("job_state", "UNKNOWN")
        if isinstance(state_raw, list):
            state_raw = state_raw[0] if state_raw else "UNKNOWN"
        state = str(state_raw).upper()

        cpus = _dig(job, ["cpus", "number"], job.get("cpus", 0))
        mem_mb = _parse_mem(job.get("memory_per_node"))
        node = job.get("nodes", "-") or "-"
        elapsed = _parse_elapsed(job)

        result[slurm_id] = {
            "state": state,
            "node": node,
            "cpus": int(cpus) if cpus else 0,
            "mem_mb": mem_mb,
            "elapsed_secs": elapsed,
        }
    return result


def _parse_tabular_jobs(text: str) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for line in text.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 7:
            continue
        slurm_id, state, _, cpus_s, mem_s, elapsed_s, node = parts[:7]
        try:
            cpus = int(cpus_s)
        except ValueError:
            cpus = 0
        result[slurm_id] = {
            "state": state.upper(),
            "node": node or "-",
            "cpus": cpus,
            "mem_mb": _parse_mem_str(mem_s),
            "elapsed_secs": _hhmmss_to_secs(elapsed_s),
        }
    return result


# ── small utilities ───────────────────────────────────────────────────────────

def _dig(d: Any, path: list, default: Any = 0) -> Any:
    for key in path:
        if isinstance(d, dict):
            d = d.get(key, default)
        else:
            return default
    return d


def _parse_mem(mem: Any) -> int:
    if isinstance(mem, dict):
        val = mem.get("number", 0)
        unit = str(mem.get("unit", "M")).upper()
        try:
            val = int(val)
            if unit == "G":
                return val * 1024
            elif unit == "K":
                return val // 1024
            return val
        except (TypeError, ValueError):
            return 0
    elif isinstance(mem, (int, float)):
        return int(mem)
    return 0


def _parse_mem_str(s: str) -> int:
    """Parse squeue memory strings like '4G', '512M', '1024K'."""
    s = s.strip().upper()
    if not s or s in ("-", "N/A"):
        return 0
    try:
        if s.endswith("G"):
            return int(s[:-1]) * 1024
        elif s.endswith("M"):
            return int(s[:-1])
        elif s.endswith("K"):
            return int(s[:-1]) // 1024
        return int(s)
    except ValueError:
        return 0


def _parse_elapsed(job: dict) -> int:
    start = job.get("start_time", {})
    if isinstance(start, dict):
        start = start.get("number", 0)
    if isinstance(start, int) and start > 0:
        return max(0, int(time.time()) - start)
    return 0


def _hhmmss_to_secs(s: str) -> int:
    s = s.strip()
    if not s or s == "-":
        return 0
    parts = s.split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
    except ValueError:
        pass
    return 0


def _reverse_lookup(state: WorkflowState, slurm_id: str) -> str:
    for smk_id, sid in state.smk_to_slurm.items():
        if sid == slurm_id:
            return state.jobid_to_rule.get(smk_id, "unknown")
    return "unknown"
