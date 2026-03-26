"""Read all log files from both algorithm folders and write a merged CSV.

Output columns:
  run, old_distance_m, old_budget_thb, old_cause, our_distance_m, our_budget_thb, our_cause

Rows are sorted by run number. A cell is empty when that algorithm has no log
for that run number.

Usage:
  python logs_to_csv.py                          # defaults
  python logs_to_csv.py --out results.csv
  python logs_to_csv.py --old-dir logs_old --our-dir logs_our --out results.csv
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional

RUN_RE = re.compile(r"run(\d+)\.txt", re.IGNORECASE)
DISTANCE_RE = re.compile(r"Total distance .*?:\s*([\d.]+)")
BUDGET_RE = re.compile(r"Total budget .*?:\s*([\d.]+)")

@dataclass
class RunRecord:
    run: int
    distance_m: Optional[float]
    budget_thb: Optional[float]
    path: str


def parse_run_number(path: str) -> Optional[int]:
    name = os.path.basename(path)
    m = RUN_RE.search(name)
    return int(m.group(1)) if m else None


def parse_log(path: str) -> Optional[RunRecord]:
    run_num = parse_run_number(path)
    distance: Optional[float] = None
    budget: Optional[float] = None

    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                if run_num is None and line.startswith("Run "):
                    parts = line.split()
                    if len(parts) >= 2:
                        try:
                            run_num = int(parts[1])
                        except ValueError:
                            pass
                if distance is None:
                    m = DISTANCE_RE.search(line)
                    if m:
                        try:
                            distance = float(m.group(1))
                        except ValueError:
                            pass
                if budget is None:
                    m = BUDGET_RE.search(line)
                    if m:
                        try:
                            budget = float(m.group(1))
                        except ValueError:
                            pass
                if all(v is not None for v in (run_num, distance, budget,)):
                    break
    except OSError as exc:
        print(f"  [warn] Cannot read {path}: {exc}", file=sys.stderr)
        return None

    if run_num is None:
        print(f"  [warn] No run number found in {path}", file=sys.stderr)
        return None

    return RunRecord(run=run_num, distance_m=distance, budget_thb=budget, path=path)


def find_all_logs(base_dir: str) -> List[str]:
    pattern = os.path.join(base_dir, "**", "log_*_run*.txt")
    paths = glob.glob(pattern, recursive=True)
    return sorted(paths, key=lambda p: parse_run_number(p) or 0)


def collect(base_dir: str, label: str) -> Dict[int, RunRecord]:
    paths = find_all_logs(base_dir)
    if not paths:
        print(f"  [{label}] No log files found under {base_dir}")
        return {}

    print(f"  [{label}] Found {len(paths)} log files under {base_dir}, parsing...")
    records: Dict[int, RunRecord] = {}
    skipped = 0
    for i, path in enumerate(paths, 1):
        if i % 1000 == 0:
            print(f"    {label}: {i}/{len(paths)}...", flush=True)
        rec = parse_log(path)
        if rec is None:
            skipped += 1
            continue
        # If same run appears more than once (e.g. in both root and big_logs),
        # keep the first occurrence (root files are parsed first when sorted).
        if rec.run not in records:
            records[rec.run] = rec

    print(f"  [{label}] Parsed {len(records)} unique runs ({skipped} skipped)")
    return records


def write_csv(old_data: Dict[int, RunRecord],
              our_data: Dict[int, RunRecord],
              out_path: str) -> None:
    all_runs = sorted(set(old_data) | set(our_data))
    if not all_runs:
        print("No data to write.")
        return

    fieldnames = [
        "run",
        "old_distance_m", "old_budget_thb",
        "our_distance_m", "our_budget_thb",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for run in all_runs:
            row: dict = {"run": run}
            if run in old_data:
                r = old_data[run]
                row["old_distance_m"] = "" if r.distance_m is None else f"{r.distance_m:.2f}"
                row["old_budget_thb"] = "" if r.budget_thb is None else f"{r.budget_thb:.2f}"
            else:
                row["old_distance_m"] = row["old_budget_thb"] = row["old_cause"] = ""
            if run in our_data:
                r = our_data[run]
                row["our_distance_m"] = "" if r.distance_m is None else f"{r.distance_m:.2f}"
                row["our_budget_thb"] = "" if r.budget_thb is None else f"{r.budget_thb:.2f}"
            else:
                row["our_distance_m"] = row["our_budget_thb"] = ""
            writer.writerow(row)

    print(f"\nWrote {len(all_runs)} rows -> {out_path}")


def print_summary(label: str, data: Dict[int, RunRecord]) -> None:
    if not data:
        return
    distances = [r.distance_m for r in data.values() if r.distance_m is not None]
    budgets = [r.budget_thb for r in data.values() if r.budget_thb is not None]

    def stats(arr: List[float]) -> str:
        if not arr:
            return "n/a"
        mean = sum(arr) / len(arr)
        arr_s = sorted(arr)
        median = arr_s[len(arr_s) // 2]
        return f"mean={mean:.2f}, median={median:.2f}, min={arr_s[0]:.2f}, max={arr_s[-1]:.2f}"

    print(f"\n[{label}] {len(data)} runs")
    print(f"  distance_m : {stats(distances)}")
    print(f"  budget_thb : {stats(budgets)}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export all log files to a merged CSV.")
    p.add_argument("--old-dir", default="logs_old",
                   help="Root directory for old algorithm logs (default: logs_old)")
    p.add_argument("--our-dir", default="logs_our",
                   help="Root directory for new algorithm logs (default: logs_our)")
    p.add_argument("--out", default="run_results.csv",
                   help="Output CSV file path (default: run_results.csv)")
    p.add_argument("--sources", choices=["old", "our", "both"], default="both",
                   help="Which algorithm logs to include (default: both)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print("Collecting logs...")
    old_data: Dict[int, RunRecord] = {}
    our_data: Dict[int, RunRecord] = {}

    if args.sources in ("old", "both"):
        if os.path.isdir(args.old_dir):
            old_data = collect(args.old_dir, "old")
        else:
            print(f"  [old] Directory not found: {args.old_dir}")

    if args.sources in ("our", "both"):
        if os.path.isdir(args.our_dir):
            our_data = collect(args.our_dir, "our")
        else:
            print(f"  [our] Directory not found: {args.our_dir}")

    print_summary("old", old_data)
    print_summary("our", our_data)

    write_csv(old_data, our_data, args.out)


if __name__ == "__main__":
    main()
