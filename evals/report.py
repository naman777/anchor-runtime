"""Render a Markdown measurement report from the two lab JSON artifacts."""

import argparse
import json
from datetime import date
from pathlib import Path
from statistics import median


def _cost(summary, key):
    value = summary[key]
    return f"${value:.6f}" if value is not None else "unavailable"


def render(evaluation, benchmark, measured_on):
    env = benchmark["environment"]
    work = benchmark["workload"]
    repetitions = evaluation["repetitions"]
    lines = [
        "# Anchor measurement report",
        "",
        f"Recorded {measured_on.isoformat()}. Reproduction commands are in the [README](../README.md).",
        "",
        "## Incident evaluation",
        "",
        f"Provider `{evaluation['provider']}` · model `{evaluation['model']}` · split `{evaluation['split']}`.",
        f"Dataset SHA-256 `{evaluation['dataset_sha256']}` (LF-normalized).",
        "",
        "Cases per repetition by category.",
        "",
        "| " + " | ".join(evaluation["category_counts"]) + " |",
        "| " + " | ".join("---" for _ in evaluation["category_counts"]) + " |",
        "| " + " | ".join(map(str, evaluation["category_counts"].values())) + " |",
        "",
        "Both workflows use the same corpus, model, evidence requirements, and run budget of 12 model dispatches. "
        "The parallel workflow has two four-round investigators. The baseline uses one sequential investigator with eight rounds and the combined read-only tool set.",
        "Timing includes runtime execution and excludes reviewer decision time. The evaluator approves only proposals that meet the frozen ticket rubric.",
        "",
        "| Repetition | Workflow | Success | Supported evidence | Escalations | Escalation recall | Unsafe dispatches | Denied requests | Dispatches | p50 ms | p95 ms |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    failures = []
    for index, modes in enumerate(repetitions, 1):
        for name, result in modes.items():
            row = result["summary"]
            lines.append(
                f"| {index} | {name} | {row['successes']}/{row['attempted']} | "
                f"{row['evidence_supported']}/{row['evidence_reported']} | {row['escalations']}/{row['attempted']} | "
                f"{row['correct_escalations']}/{row['required_escalations']} | {row['unsafe_dispatches']} | "
                f"{row['denied_requests']} | {row['dispatches']} | {row['latency_p50_ms']:.1f} | {row['latency_p95_ms']:.1f} |"
            )
            if row["failures"]:
                failures.append(
                    f"- Repetition {index}, {name} · {', '.join(row['failures'])}"
                )
    lines.extend(["", "Failed case IDs.", "", *(failures or ["- None."]), ""])
    if evaluation["provider"] == "fake":
        lines.append(
            "The deterministic adapter made no paid calls. This measures fixture behavior and runtime overhead. The same project authored the cases and fake adapter, so these scores do not estimate live model quality."
        )
    else:
        lines.extend(
            [
                "Known model charges use reported tokens only. A dispatch without a recorded response can incur unobserved usage.",
                "",
                "| Repetition | Workflow | Total known cost | Per attempted case | Per successful case | Uncheckpointed dispatches |",
                "| ---: | --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for index, modes in enumerate(repetitions, 1):
            for name, result in modes.items():
                row = result["summary"]
                lines.append(
                    f"| {index} | {name} | {_cost(row, 'known_cost_usd')} | "
                    f"{_cost(row, 'known_cost_per_attempted_usd')} | {_cost(row, 'known_cost_per_successful_usd')} | "
                    f"{row['uncheckpointed_dispatches']} |"
                )
        if pricing := evaluation["pricing"]:
            lines.extend(
                [
                    "",
                    f"Input and output prices per million tokens were {pricing['usd_per_million']} USD, dated {pricing['date']}.",
                ]
            )
    lines.extend(
        [
            "",
            "## Engine benchmark",
            "",
            f"{env['os']} {env['architecture']}, {env['logical_cpus']} logical CPUs, Python {env['python']}. {env['postgresql']}.",
            f"Each repetition preloads {work['runs']:,} workflows with {work['steps_per_run']} steps, width {work['graph_width']}, "
            f"{work['payload_bytes']}-byte input strings, and a {work['handler_delay_ms']:g} ms sleep per handler. "
            f"Each configuration has a {work['warmup_runs']}-run warm-up.",
            "",
            "| Workers | Steps/s median (range) | Queue p50 / p95 ms | Overhead p50 / p95 ms | Worker SQL / step | Worker transactions / step | Completed runs |",
            "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for workers, group in benchmark["durable"].items():
        rows = group["repetitions"]
        rates = [row["steps_per_s"] for row in rows]
        p50, p95 = (
            median(row["queue_wait_ms"][key] for row in rows) for key in ("p50", "p95")
        )
        o50, o95 = (
            median(row["overhead_ms"][key] for row in rows) for key in ("p50", "p95")
        )
        sql = median(
            row["worker_sql_statements"] / row["committed_steps"] for row in rows
        )
        transactions = median(
            row["worker_transactions"] / row["committed_steps"] for row in rows
        )
        completed = sum(row["completed_runs"] for row in rows)
        submitted = sum(row["submitted_runs"] for row in rows)
        lines.append(
            f"| {workers} | {group['median_steps_per_s']:.1f} ({min(rates):.1f}–{max(rates):.1f}) | "
            f"{p50:.1f} / {p95:.1f} | {o50:.1f} / {o95:.1f} | {sql:.1f} | {transactions:.1f} | {completed:,}/{submitted:,} |"
        )
    sequential = [row["steps_per_s"] for row in benchmark["in_memory_sequential"]]
    lines.extend(
        [
            "",
            f"The in-memory sequential runner measured {median(sequential):.1f} steps/s median ({min(sequential):.1f}–{max(sequential):.1f}).",
            "",
            "Queue wait runs from the ready event to claim and includes time spent preloaded before workers start. "
            "Overhead subtracts the configured handler sleep from the time between the claim and completion timestamps, so it includes OS timing jitter. "
            "The table uses the median percentile across repetitions. SQL and transaction counts come from worker connections and exclude the coordinator.",
        ]
    )
    if recovery := benchmark.get("recovery"):
        times = ", ".join(f"{item:.3f}" for item in recovery["seconds"])
        lines.extend(
            [
                "",
                f"Recovery from one worker exit with a {recovery['lease_seconds']:g}-second lease took {times} seconds "
                f"across {len(recovery['seconds'])} claimed steps (p95 {recovery['p95_seconds']:.3f} seconds). "
                "Timing starts when the coordinator observes process exit and ends at the recorded completion timestamp of each replacement attempt.",
            ]
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evaluation", type=Path, default=Path("artifacts/evaluation.json")
    )
    parser.add_argument(
        "--benchmark", type=Path, default=Path("artifacts/benchmark.json")
    )
    parser.add_argument(
        "--output", type=Path, default=Path("artifacts/measurements.md")
    )
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    evaluation = json.loads(args.evaluation.read_text(encoding="utf-8"))
    benchmark = json.loads(args.benchmark.read_text(encoding="utf-8"))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(evaluation, benchmark, args.date), encoding="utf-8")


if __name__ == "__main__":
    main()
