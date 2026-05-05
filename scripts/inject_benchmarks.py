#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
inject_benchmarks.py
────────────────────
Adds a `benchmark:` directive to every rule in a Snakefile that doesn't
already have one.  Writes a modified copy so the original is never touched.

Usage:
  uv run inject_benchmarks.py Snakefile                   # writes Snakefile.benchmarked
  uv run inject_benchmarks.py Snakefile -o MySnakefile    # custom output path
  uv run inject_benchmarks.py Snakefile --in-place        # overwrite (keeps .bak)
  uv run inject_benchmarks.py Snakefile --dry-run         # just show what would change

The injected benchmark line looks like:
  benchmark: "benchmarks/{rule_name}.{wildcards}.benchmark.tsv"
  (wildcards are detected from the rule's output: block automatically)

pipeline_benchmark.py will then scan for *.benchmark.tsv in the workdir
and include per-rule memory/CPU/io stats in the comparison report.
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path


def _detect_wildcards(rule_block: str) -> list[str]:
    """Extract wildcard names from the output: block of a rule."""
    # Find output: section
    m = re.search(r'\boutput:\s*(.*?)(?=\n\s{4}\w|\Z)', rule_block, re.DOTALL)
    if not m:
        return []
    output_text = m.group(1)
    return re.findall(r'\{(\w+)\}', output_text)


def _benchmark_line(rule_name: str, wildcards: list[str], indent: str = "    ") -> str:
    if wildcards:
        wc_part = ".".join(f"{{{w}}}" for w in wildcards)
        path = f'"benchmarks/{rule_name}.{wc_part}.benchmark.tsv"'
    else:
        path = f'"benchmarks/{rule_name}.benchmark.tsv"'
    return f"{indent}benchmark:\n{indent}    {path}\n"


def inject_benchmarks(source: str) -> tuple[str, list[str]]:
    """
    Parse a Snakefile string and inject benchmark directives.
    Returns (modified_source, list_of_modified_rule_names).
    """
    lines = source.splitlines(keepends=True)
    out_lines: list[str] = []
    modified_rules: list[str] = []

    i = 0
    while i < len(lines):
        line = lines[i]
        # Detect rule start
        rule_match = re.match(r'^rule\s+(\w+)\s*:', line)
        if rule_match:
            rule_name = rule_match.group(1)
            # Collect entire rule block (until next top-level keyword or EOF)
            rule_start = i
            block_lines = [line]
            i += 1
            while i < len(lines):
                if lines[i] and not lines[i][0].isspace() and lines[i].strip():
                    break
                block_lines.append(lines[i])
                i += 1
            block = "".join(block_lines)

            # Check if benchmark already present
            if re.search(r'^\s+benchmark\s*:', block, re.MULTILINE):
                out_lines.extend(block_lines)
                continue

            # Detect wildcards from the block
            wildcards = _detect_wildcards(block)
            unique_wc = list(dict.fromkeys(wildcards))  # deduplicated, order-preserved

            # Find the best insertion point: after output:, or after input:, or after rule header
            # Strategy: insert after the last line of the first "section" block
            # We'll insert just before the first keyword after the rule: line
            new_block_lines = list(block_lines)
            inserted = False

            # Find first keyword line in the block
            for j in range(1, len(new_block_lines)):
                kw_match = re.match(r'^(\s+)(\w+)\s*:', new_block_lines[j])
                if kw_match:
                    indent = kw_match.group(1)
                    # Insert benchmark right before first keyword (e.g. input:)
                    bmk = _benchmark_line(rule_name, unique_wc, indent)
                    new_block_lines.insert(j, bmk)
                    inserted = True
                    break

            if not inserted:
                # Fallback: append at end of block with 4-space indent
                new_block_lines.append(_benchmark_line(rule_name, unique_wc))

            out_lines.extend(new_block_lines)
            modified_rules.append(rule_name)
        else:
            out_lines.append(line)
            i += 1

    return "".join(out_lines), modified_rules


def main():
    parser = argparse.ArgumentParser(
        description="Inject Snakemake benchmark: directives into a Snakefile"
    )
    parser.add_argument("snakefile", help="Path to input Snakefile")
    parser.add_argument("-o", "--output", default=None,
                        help="Output path (default: <input>.benchmarked)")
    parser.add_argument("--in-place", action="store_true",
                        help="Overwrite the original (saves .bak backup)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print modified rules without writing files")
    args = parser.parse_args()

    src_path = Path(args.snakefile)
    if not src_path.exists():
        print(f"Error: {src_path} not found", file=sys.stderr)
        sys.exit(1)

    source = src_path.read_text()
    modified, rule_names = inject_benchmarks(source)

    if not rule_names:
        print("No rules needed modification (all already have benchmark: directives).")
        sys.exit(0)

    print(f"Would inject benchmark directives into {len(rule_names)} rule(s):")
    for name in rule_names:
        print(f"  • {name}")

    if args.dry_run:
        print("\n── Modified Snakefile preview ──")
        print(modified[:2000])
        if len(modified) > 2000:
            print(f"... ({len(modified) - 2000} more chars)")
        sys.exit(0)

    if args.in_place:
        bak = src_path.with_suffix(src_path.suffix + ".bak")
        shutil.copy2(src_path, bak)
        print(f"Backup saved to {bak}")
        out_path = src_path
    elif args.output:
        out_path = Path(args.output)
    else:
        out_path = src_path.with_suffix(src_path.suffix + ".benchmarked")

    out_path.write_text(modified)
    print(f"\nWritten to: {out_path}")
    print(f"\nNow run your pipeline with:\n"
          f"  snakemake -s {out_path} --profile slurm ...")
    print(f"\nBenchmark TSV files will appear in ./benchmarks/ and will be\n"
          f"automatically picked up by pipeline_benchmark.py.")


if __name__ == "__main__":
    main()
