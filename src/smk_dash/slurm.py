"""
Polls squeue periodically and updates SlurmJob entries in WorkflowState.

Uses `squeue --me --json` (Slurm 21+).  Falls back to a tabular parse if
the JSON flag isn't available (older clusters).

On startup, queries `scontrol show partitions` once to learn per-partition
default CPUs, memory, and time — used by the UI to flag jobs that appear
to have fallen back to SLURM defaults due to config errors.

All I/O is async — runs as a task in Textual's event loop.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any

from .models import WorkflowState


# ── partition defaults ────────────────────────────────────────────────────────

@dataclass
class PartitionDefaults:
    default_cpus: int = 1
    default_mem_mb: int = 0    # 0 = unknown / not configured
    default_time_secs: int = 0  # 0 = unknown / unlimited


async def fetch_partition_defaults() -> dict[str, PartitionDefaults]:
    """
    Query `scontrol show partitions` once and return per-partition defaults.

    Parses key=value tokens from scontrol output, e.g.:
        PartitionName=short DefaultTime=01:00:00 DefMemPerCPU=4096 ...

    Returns an empty dict if scontrol is unavailable (non-Slurm systems,
    demo mode, etc.).
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "scontrol", "show", "partitions",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            return {}
    except FileNotFoundError:
        return {}

    defaults: dict[str, PartitionDefaults] = {}
    current: PartitionDefaults | None = None
    current_name: str = ""

    for line in stdout.decode(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue

        for token in line.split():
            k, _, v = token.partition("=")
            if k == "PartitionName":
                current_name = v
                current = PartitionDefaults()
                defaults[current_name] = current
                continue

            if current is None:
                continue

            if k == "DefaultTime" and v not in ("NONE", "UNLIMITED", ""):
                current.default_time_secs = _hhmmss_to_secs(v)

            elif k == "DefMemPerCPU" and v not in ("NONE", ""):
                try:
                    # scontrol reports in KB
                    current.default_mem_mb = int(v) // 1024
                except ValueError:
                    pass

            elif k == "DefMemPerNode" and v not in ("NONE", "") and current.default_mem_mb == 0:
                try:
                    current.default_mem_mb = int(v) // 1024
                except ValueError:
                    pass

    return defaults


# ── poller ────────────────────────────────────────────────────────────────────

class SlurmPoller:
    def __init__(self, state: WorkflowState, poll_interval: float = 5.0) -> None:
        self.state = state
        self.poll_interval = poll_interval
        self._running = False
        self.partition_defaults: dict[str, PartitionDefaults] = {}

    async def run(self) -> None:
        self._running = True
        # Fetch partition defaults once before the polling loop starts
        self.partition_defaults = await fetch_partition_defaults()
        self.state.partition_defaults = self.partition_defaults
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
                # Update new fields; preserve existing values if not in response
                if info.get("time_limit_secs"):
                    job.time_limit_secs = info["time_limit_secs"]
                if info.get("partition"):
                    job.partition = info["partition"]

            elif slurm_id in self.state.active_slurm_ids:
                # Seen in the log but not yet in slurm_jobs
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
                    time_limit_secs=info.get("time_limit_secs", 0),
                    partition=info.get("partition", ""),
                )

        # Mark jobs that have disappeared from squeue as COMPLETED
        current_ids = set(raw_jobs.keys())
        for sid in list(self.state.active_slurm_ids):
            if sid not in current_ids:
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
    # %i job id  %T state  %R reason  %C cpus  %m min memory
    # %M elapsed  %N nodes  %l timelimit  %P partition
    fmt = "%i|%T|%R|%C|%m|%M|%N|%l|%P"
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

        cpus    = _dig(job, ["cpus", "number"], job.get("cpus", 0))
        mem_mb  = _parse_mem(job.get("memory_per_node"))
        node    = job.get("nodes", "-") or "-"
        elapsed = _parse_elapsed(job)

        # time_limit arrives as a dict {"set": bool, "number": int (minutes)}
        # in Slurm 22+ JSON; older versions may just be an int.
        tl = job.get("time_limit", {})
        if isinstance(tl, dict):
            tl_mins = tl.get("number", 0) if tl.get("set") else 0
        else:
            try:
                tl_mins = int(tl)
            except (TypeError, ValueError):
                tl_mins = 0

        partition = job.get("partition", "")

        result[slurm_id] = {
            "state":           state,
            "node":            node,
            "cpus":            int(cpus) if cpus else 0,
            "mem_mb":          mem_mb,
            "elapsed_secs":    elapsed,
            "time_limit_secs": tl_mins * 60,
            "partition":       partition,
        }
    return result


def _parse_tabular_jobs(text: str) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for line in text.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 7:
            continue
        slurm_id   = parts[0]
        state      = parts[1].upper()
        cpus_s     = parts[3]
        mem_s      = parts[4]
        elapsed_s  = parts[5]
        node       = parts[6] or "-"
        timelimit_s = parts[7] if len(parts) > 7 else ""
        partition   = parts[8].strip() if len(parts) > 8 else ""
        try:
            cpus = int(cpus_s)
        except ValueError:
            cpus = 0
        result[slurm_id] = {
            "state":           state,
            "node":            node,
            "cpus":            cpus,
            "mem_mb":          _parse_mem_str(mem_s),
            "elapsed_secs":    _hhmmss_to_secs(elapsed_s),
            "time_limit_secs": _hhmmss_to_secs(timelimit_s),
            "partition":       partition,
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
        val  = mem.get("number", 0)
        unit = str(mem.get("unit", "M")).upper()
        try:
            val = int(val)
            if unit == "G":
                return val * 1024
            elif unit == "K":
                return max(1, val // 1024)
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
            return max(1, int(s[:-1]) // 1024)
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
    """Parse D-HH:MM:SS, HH:MM:SS, or MM:SS strings from squeue/scontrol."""
    s = s.strip()
    if not s or s in ("-", "UNLIMITED", "NONE"):
        return 0
    # Handle D-HH:MM:SS format
    days = 0
    if "-" in s:
        day_part, s = s.split("-", 1)
        try:
            days = int(day_part)
        except ValueError:
            pass
    parts = s.split(":")
    try:
        if len(parts) == 3:
            return days * 86400 + int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return days * 86400 + int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 1:
            return days * 86400 + int(parts[0])
    except ValueError:
        pass
    return 0


def _reverse_lookup(state: WorkflowState, slurm_id: str) -> str:
    for smk_id, sid in state.smk_to_slurm.items():
        if sid == slurm_id:
            return state.jobid_to_rule.get(smk_id, "unknown")
    return "unknown"
