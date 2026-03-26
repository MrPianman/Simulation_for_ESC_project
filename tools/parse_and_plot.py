from __future__ import annotations

import argparse
import glob
import os
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List

import matplotlib.pyplot as plt

RUN_RE = re.compile(r"run(\d+)\.txt")
DISTANCE_RE = re.compile(r"Total distance .*?:\s*([\d.]+)")
BUDGET_RE = re.compile(r"Total budget .*?:\s*([\d.]+)")
CAUSE_RE = re.compile(r"Overall Cause:\s*(.*)")

@dataclass
class LogRecord:
    run: int
    distance_m: float | None
    budget_thb: float | None
    cause: str | None
    source: str
    path: str


def parse_run_from_name(path: str) -> int | None:
    name = os.path.basename(path)
    m = RUN_RE.search(name)
    if m:
        return int(m.group(1))
    return None


def parse_log_file(path: str, source: str) -> LogRecord | None:
    run_num = parse_run_from_name(path)
    distance = None
    budget = None
    cause = None

    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                if run_num is None and line.startswith("Run"):
                    try:
                        run_num = int(line.split()[1])
                    except (IndexError, ValueError):
                        pass
                if distance is None:
                    dm = DISTANCE_RE.search(line)
                    if dm:
                        try:
                            distance = float(dm.group(1))
                        except ValueError:
                            distance = None
                if budget is None:
                    bm = BUDGET_RE.search(line)
                    if bm:
                        try:
                            budget = float(bm.group(1))
                        except ValueError:
                            budget = None
                if cause is None:
                    cm = CAUSE_RE.search(line)
                    if cm:
                        cause = cm.group(1).strip()
                if distance is not None and budget is not None and cause is not None and run_num is not None:
                    break
    except OSError as exc:
        print(f"Failed to read {path}: {exc}")
        return None

    if run_num is None:
        print(f"Skipping {path}: missing run number")
        return None

    return LogRecord(run=run_num, distance_m=distance, budget_thb=budget, cause=cause, source=source, path=path)


def find_logs(root: str, subdir: str = "big_logs") -> List[str]:
    target_dir = os.path.join(root, subdir)
    pattern = os.path.join(target_dir, "log_*_run*.txt")
    return sorted(glob.glob(pattern), key=lambda p: parse_run_from_name(p) or float("inf"))


def collect_records(paths: Iterable[str], source: str) -> List[LogRecord]:
    records: List[LogRecord] = []
    for path in paths:
        rec = parse_log_file(path, source)
        if rec:
            records.append(rec)
    return records


def _describe(arr: List[float]) -> Dict[str, float] | None:
    if not arr:
        return None
    vals = sorted(arr)
    n = len(vals)
    mean = sum(vals) / n
    var = sum((v - mean) ** 2 for v in vals) / n
    sd = var ** 0.5

    def quantile(q: float) -> float:
        if n == 1:
            return vals[0]
        idx = (n - 1) * q
        lo = int(idx)
        hi = min(lo + 1, n - 1)
        frac = idx - lo
        return vals[lo] * (1 - frac) + vals[hi] * frac

    p10 = quantile(0.10)
    p25 = quantile(0.25)
    p50 = quantile(0.50)
    p75 = quantile(0.75)
    p90 = quantile(0.90)

    return {
        "count": n,
        "mean": mean,
        "sd": sd,
        "min": vals[0],
        "max": vals[-1],
        "median": p50,
        "p10": p10,
        "p25": p25,
        "p75": p75,
        "p90": p90,
        "iqr": p75 - p25,
    }


def summarize(records: List[LogRecord]) -> Dict[str, Dict[str, float] | None]:
    distances = [r.distance_m for r in records if r.distance_m is not None]
    budgets = [r.budget_thb for r in records if r.budget_thb is not None]

    return {
        "runs": {"count": len(records)},
        "distance": _describe(distances),
        "budget": _describe(budgets),
    }


def plot_histograms(data: Dict[str, List[LogRecord]], save_path: str | None, show: bool) -> None:
    """Separate window: frequency histograms of per-run distance and budget."""
    if not data:
        return

    fig, (ax_dist, ax_budget) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("Histogram of per-run totals (frequency)")

    colors = ["steelblue", "darkorange", "green", "red"]
    alpha = 0.6

    for i, (source, records) in enumerate(data.items()):
        if not records:
            continue
        distances = [r.distance_m for r in records if r.distance_m is not None]
        budgets = [r.budget_thb for r in records if r.budget_thb is not None]
        color = colors[i % len(colors)]
        label = f"{source} algorithm"

        ax_dist.hist(distances, bins=30, color=color, alpha=alpha, label=label, edgecolor="white")
        ax_budget.hist(budgets, bins=30, color=color, alpha=alpha, label=label, edgecolor="white")

    ax_dist.set_xlabel("Total distance (m)")
    ax_dist.set_ylabel("Frequency")
    ax_dist.set_title("Distance per run")
    ax_dist.legend()
    ax_dist.grid(True, linestyle=":", alpha=0.5)

    ax_budget.set_xlabel("Total budget (THB)")
    ax_budget.set_ylabel("Frequency")
    ax_budget.set_title("Budget per run")
    ax_budget.legend()
    ax_budget.grid(True, linestyle=":", alpha=0.5)

    fig.tight_layout()

    if save_path:
        hist_path = save_path.rsplit(".", 1)
        hist_file = hist_path[0] + "_hist." + hist_path[1] if len(hist_path) == 2 else save_path + "_hist.png"
        fig.savefig(hist_file, dpi=150)
        print(f"Saved histogram to {hist_file}")
    if show:
        plt.show()
    plt.close(fig)


def plot_metrics(data: Dict[str, List[LogRecord]], save_path: str | None, show: bool) -> None:
    if not data:
        print("No data to plot.")
        return

    fig, (ax_dist, ax_budget) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)

    for source, records in data.items():
        if not records:
            continue
        records_sorted = sorted(records, key=lambda r: r.run)
        runs = [r.run for r in records_sorted]
        distances = [r.distance_m for r in records_sorted]
        budgets = [r.budget_thb for r in records_sorted]

        ax_dist.plot(runs, distances, linestyle="-", label=f"{source} algorithm")
        ax_budget.plot(runs, budgets, linestyle="-", label=f"{source} algorithm")

    ax_dist.set_ylabel("Total distance (m)")
    ax_budget.set_ylabel("Total budget (THB)")
    ax_budget.set_xlabel("Run number")
    ax_dist.grid(True, linestyle=":", alpha=0.6)
    ax_budget.grid(True, linestyle=":", alpha=0.6)
    ax_dist.legend()
    ax_budget.legend()
    fig.suptitle("Simulation big-log totals")
    fig.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150)
        print(f"Saved plot to {save_path}")
    if show:
        plt.show()
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse big logs and plot run totals.")
    parser.add_argument("--old-dir", default="logs_old", help="Base directory for old algorithm logs (default: logs_old)")
    parser.add_argument("--our-dir", default="logs_our", help="Base directory for new algorithm logs (default: logs_our)")
    parser.add_argument("--sources", choices=["old", "our", "both"], default="both", help="Which sources to parse")
    parser.add_argument("--subdir", default="big_logs", help="Subdirectory containing big log snapshots (default: big_logs)")
    parser.add_argument("--save", dest="save_path", default=None, help="Optional path to save PNG output")
    parser.add_argument("--show", action="store_true", help="Show the matplotlib window")
    parser.add_argument("--start-run", type=int, default=None, help="Only include runs >= this number")
    parser.add_argument("--end-run", type=int, default=None, help="Only include runs <= this number")
    return parser.parse_args()


def filter_runs(records: List[LogRecord], start_run: int | None, end_run: int | None) -> List[LogRecord]:
    result: List[LogRecord] = []
    for rec in records:
        if start_run is not None and rec.run < start_run:
            continue
        if end_run is not None and rec.run > end_run:
            continue
        result.append(rec)
    return result


def main() -> None:
    args = parse_args()
    sources_to_use = []
    if args.sources in ("old", "both"):
        sources_to_use.append(("old", args.old_dir))
    if args.sources in ("our", "both"):
        sources_to_use.append(("our", args.our_dir))

    all_data: Dict[str, List[LogRecord]] = {}

    for source_name, base_dir in sources_to_use:
        if not os.path.isdir(base_dir):
            print(f"Skipping {source_name}: directory {base_dir} not found")
            continue
        paths = find_logs(base_dir, args.subdir)
        if not paths:
            print(f"No logs found for {source_name} in {base_dir}/{args.subdir}")
            continue
        records = collect_records(paths, source_name)
        records = filter_runs(records, args.start_run, args.end_run)
        all_data[source_name] = records
        stats = summarize(records)
        print(f"{source_name}: {stats['runs']['count']} runs")

        def print_block(label: str, desc: Dict[str, float] | None) -> None:
            if not desc:
                print(f"  {label}: no data")
                return
            print(f"  {label}:")
            print(f"    mean+/-sd: {desc['mean']:.2f} +/- {desc['sd']:.2f}")
            print(f"    median/IQR: {desc['median']:.2f} / {desc['iqr']:.2f}")
            print(f"    min..max: {desc['min']:.2f} .. {desc['max']:.2f}")
            print(
                "    p10/p25/p50/p75/p90: "
                f"{desc['p10']:.2f}, {desc['p25']:.2f}, {desc['median']:.2f}, {desc['p75']:.2f}, {desc['p90']:.2f}"
            )
            print("Smaller Are Better")

        print_block("Distance m", stats["distance"])
        print_block("Budget THB", stats["budget"])

    if not any(all_data.values()):
        print("No records parsed; nothing to plot.")
        return

    plot_histograms(all_data, args.save_path, args.show)
    plot_metrics(all_data, args.save_path, args.show)


if __name__ == "__main__":
    main()