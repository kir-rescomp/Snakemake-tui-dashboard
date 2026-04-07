"""
Demo / simulation mode.

Feeds synthetic Snakemake log lines into a LogWatcher so the dashboard can
be evaluated without a real workflow running.  Simulates a typical
RNA-seq pipeline with ~60 jobs across 10 rule types.
"""
from __future__ import annotations

import asyncio
import random
import time

from .models import SlurmJob, WorkflowState

# Simulated pipeline: (rule_name, n_jobs, cpus, mem_gb, avg_runtime_secs)
PIPELINE: list[tuple[str, int, int, int, int]] = [
    ("fastqc",        8,  2,  8,  90),
    ("trim_galore",   8,  4,  8, 180),
    ("star_align",    8, 16, 32, 600),
    ("samtools_sort", 8,  4, 16, 120),
    ("samtools_index",8,  2,  4,  30),
    ("htseq_count",   8,  4,  8, 300),
    ("featurecounts", 8,  4,  8, 120),
    ("deseq2",        1,  8, 32, 240),
    ("multiqc",       1,  2,  8,  60),
    ("report",        1,  2,  4,  20),
]


class DemoDriver:
    """
    Drives WorkflowState with synthetic log events.
    Attach a LogWatcher and call run() as an asyncio task.
    """

    def __init__(self, state: WorkflowState, speed: float = 1.0) -> None:
        self.state = state
        self.speed = speed  # >1 = faster, <1 = slower
        self._slurm_counter = 9_900_000
        self._smk_counter = 0

    async def run(self) -> None:
        from .watcher import LogWatcher
        watcher = LogWatcher(self.state)

        total = sum(n for _, n, *_ in PIPELINE)
        self._emit(watcher, f"Building DAG of jobs...")
        self._emit(watcher, f"Using profile: slurm")
        await asyncio.sleep(self._t(1.5))

        done = 0
        # Walk the pipeline in stage order (sequential dependency simulation)
        for rule, n_jobs, cpus, mem_gb, avg_rt in PIPELINE:
            # Submit all jobs in this stage
            job_slots: list[tuple[int, int, float]] = []  # (smk_id, slurm_id, finish_at)
            for _ in range(n_jobs):
                self._smk_counter += 1
                smk_id = self._smk_counter
                self._slurm_counter += 1
                slurm_id = self._slurm_counter

                rt = avg_rt * random.uniform(0.7, 1.4) / self.speed
                finish_at = time.monotonic() + self._t(rt)

                self._emit(watcher, f"rule {rule}:")
                self._emit(watcher, f"    input: data/sample{smk_id}.fastq.gz")
                self._emit(watcher, f"    output: results/{rule}/sample{smk_id}.out")
                self._emit(watcher, f"    jobid: {smk_id}")
                self._emit(watcher, f"    reason: Missing output files")
                await asyncio.sleep(self._t(0.15))
                self._emit(watcher, f"Submitted job {smk_id} with external jobid '{slurm_id}'")

                # Register in state directly so the poller doesn't need squeue
                self.state.slurm_jobs[str(slurm_id)] = SlurmJob(
                    slurm_id=str(slurm_id),
                    smk_jobid=smk_id,
                    rule_name=rule,
                    state="RUNNING",
                    node=f"bmrc-gpu{random.randint(1,4):02d}" if cpus >= 8 else f"bmrc-cpu{random.randint(1,16):02d}",
                    cpus=cpus,
                    mem_mb=mem_gb * 1024,
                    elapsed_secs=0,
                )
                job_slots.append((smk_id, slurm_id, finish_at))
                await asyncio.sleep(self._t(0.05))

            self._emit(
                watcher,
                f"{done} of {total} steps "
                f"({int(done / total * 100)}%) done",
            )

            # Tick elapsed time and finish jobs one by one
            while job_slots:
                await asyncio.sleep(self._t(0.5))
                now = time.monotonic()
                finished = [(s, sl, f) for s, sl, f in job_slots if now >= f]
                for smk_id, slurm_id, _ in finished:
                    job_slots = [(s, sl, f) for s, sl, f in job_slots if s != smk_id]
                    # Occasional failure for realism
                    if random.random() < 0.04 and done > 0:
                        self._emit(watcher, f"Error in rule {rule}:")
                        self._emit(watcher, f"    jobid: {smk_id}")
                        self._emit(watcher, f"    Failed to execute rule.")
                        if str(slurm_id) in self.state.slurm_jobs:
                            self.state.slurm_jobs[str(slurm_id)].state = "FAILED"
                    else:
                        self._emit(watcher, f"Finished job {smk_id}.")
                        if str(slurm_id) in self.state.slurm_jobs:
                            self.state.slurm_jobs[str(slurm_id)].state = "COMPLETED"
                        done += 1

                # Update elapsed times for still-running jobs
                for smk_id, slurm_id, finish_at in job_slots:
                    job = self.state.slurm_jobs.get(str(slurm_id))
                    if job:
                        # Rough elapsed: time since submission
                        avg_rt_scaled = avg_rt / self.speed
                        remaining = max(0, finish_at - now)
                        job.elapsed_secs = int(avg_rt_scaled - remaining)

        self._emit(watcher, f"{done} of {total} steps (100%) done")
        self._emit(watcher, "Complete log: ...")
        self.state.finished = True

    # ── helpers ───────────────────────────────────────────────────────────────

    def _emit(self, watcher, line: str) -> None:
        import datetime
        ts = datetime.datetime.now().strftime("[%a %b %d %H:%M:%S %Y]")
        # Only timestamp "rule" and "Submitted/Finished/Error" lines
        if line.startswith("rule ") or line.startswith("Submitted") \
                or line.startswith("Finished") or line.startswith("Error"):
            watcher.feed_line(ts)
        watcher.feed_line(line)

    def _t(self, secs: float) -> float:
        return secs / self.speed
