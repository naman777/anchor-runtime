"""Measure deterministic workflow throughput and crash recovery."""

import argparse
import multiprocessing
import os
import platform
from collections import Counter
from datetime import timedelta
from pathlib import Path
from statistics import median
from time import perf_counter, sleep

from pydantic import BaseModel
from sqlalchemy import and_, event, func, select

from anchor_runtime.durable import DurableRuntime
from anchor_runtime.graph import Handler, Node, Workflow
from anchor_runtime.runner import run
from anchor_runtime.storage import (
    AttemptRecord,
    AttemptState,
    EventRecord,
    RunRecord,
    RunState,
)
from anchor_runtime.worker import work_once
from evals.lab import (
    database_url,
    isolated_database,
    lab_engine,
    percentile,
    write_json,
)

PAYLOAD = {"payload": "x" * 256}
POLL_SECONDS = 0.02


class Payload(BaseModel):
    payload: str


def workload(delay: float, *, recovery: bool = False) -> Workflow:
    def handle(context):
        sleep(delay)
        return context.input_as(Payload)

    nodes = (Node("left", "handle", "1"),)
    if not recovery:
        nodes += (
            Node("right", "handle", "1"),
            Node("join", "handle", "1", ("left", "right")),
        )
    return Workflow(
        "bench-recovery" if recovery else "bench-fanout",
        "1",
        Payload,
        nodes,
        (Handler("handle", "1", Payload, handle),),
    )


def _worker(schema, delay, lease_seconds, recovery, ready, start, stop, results):
    engine = lab_engine(database_url(), schema)
    counters = Counter()

    @event.listens_for(engine, "before_cursor_execute")
    def count_statement(*_):
        counters["statements"] += 1

    @event.listens_for(engine, "commit")
    def count_commit(*_):
        counters["commits"] += 1

    try:
        runtime = DurableRuntime(
            engine,
            [workload(delay, recovery=recovery)],
            lease=timedelta(seconds=lease_seconds),
        )
        ready.put(True)
        if not start.wait(60):
            raise TimeoutError("Benchmark start barrier timed out")
        counters.clear()
        while not stop.is_set():
            if not work_once(runtime, str(os.getpid())):
                stop.wait(POLL_SECONDS)
    finally:
        engine.dispose()
        results.put(dict(counters))


def _drain(
    engine, schema, delay, workers, timeout, *, lease_seconds=30, recovery=False
):
    context = multiprocessing.get_context("spawn")
    ready, results = context.Queue(), context.Queue()
    start, stop = context.Event(), context.Event()
    processes = [
        context.Process(
            target=_worker,
            args=(schema, delay, lease_seconds, recovery, ready, start, stop, results),
        )
        for _ in range(workers)
    ]
    started = []
    try:
        for process in processes:
            process.start()
            started.append(process)
        for _ in processes:
            ready.get(timeout=60)
        begin = perf_counter()
        start.set()
        with engine.connect() as connection:
            query = (
                select(func.count())
                .select_from(RunRecord)
                .where(RunRecord.state == RunState.ACTIVE)
            )
            while connection.scalar(query):
                connection.rollback()
                if perf_counter() - begin > timeout:
                    raise TimeoutError("Benchmark did not reach a terminal state")
                if any(process.exitcode is not None for process in processes):
                    raise RuntimeError("Benchmark worker exited early")
                sleep(POLL_SECONDS)
        seconds = perf_counter() - begin
        stop.set()
        counters = Counter()
        for _ in processes:
            counters.update(results.get(timeout=30))
        for process in processes:
            process.join(timeout=10)
            if process.exitcode != 0:
                raise RuntimeError(f"Benchmark worker exit code {process.exitcode}")
        return seconds, counters
    finally:
        stop.set()
        for process in started:
            if process.is_alive():
                process.terminate()
            process.join(timeout=10)
        ready.close()
        results.close()


def _stats(engine, seconds, counters, delay, total):
    with engine.connect() as connection:
        rows = connection.execute(
            select(AttemptRecord.started_at, AttemptRecord.finished_at, EventRecord.at)
            .join(
                EventRecord,
                and_(
                    EventRecord.run_id == AttemptRecord.run_id,
                    EventRecord.node_id == AttemptRecord.node_id,
                    EventRecord.type == "step.ready",
                ),
            )
            .where(AttemptRecord.state == AttemptState.SUCCEEDED)
        ).all()
        successful = connection.scalar(
            select(func.count())
            .select_from(RunRecord)
            .where(RunRecord.state == RunState.SUCCEEDED)
        )
    queue = [(started - ready).total_seconds() * 1000 for started, _, ready in rows]
    overhead = [
        ((finished - started).total_seconds() - delay) * 1000
        for started, finished, _ in rows
    ]
    return {
        "elapsed_s": seconds,
        "completed_runs": successful,
        "submitted_runs": total,
        "committed_steps": len(rows),
        "steps_per_s": len(rows) / seconds,
        "queue_wait_ms": {
            "p50": percentile(queue, 0.50),
            "p95": percentile(queue, 0.95),
        },
        "overhead_ms": {
            "p50": percentile(overhead, 0.50),
            "p95": percentile(overhead, 0.95),
        },
        "worker_sql_statements": counters["statements"],
        "worker_transactions": counters["commits"],
    }


def _sample(count, delay, workers, timeout):
    with isolated_database() as (engine, schema):
        flow = workload(delay)
        runtime = DurableRuntime(engine, [flow])
        for _ in range(count):
            runtime.start(flow.name, flow.version, PAYLOAD)
        seconds, counters = _drain(engine, schema, delay, workers, timeout)
        result = _stats(engine, seconds, counters, delay, count)
    if result["completed_runs"] != count or result["committed_steps"] != count * len(
        flow.nodes
    ):
        raise RuntimeError("Benchmark did not commit every expected run and step")
    return result


def _crash(schema, count, lease_seconds):
    engine = lab_engine(database_url(), schema)
    runtime = DurableRuntime(
        engine, [workload(0, recovery=True)], lease=timedelta(seconds=lease_seconds)
    )
    for _ in range(count):
        if runtime.claim("crash-probe") is None:
            raise RuntimeError("Unable to claim recovery probe")
    os._exit(77)


def _recovery(count, lease_seconds, timeout):
    with isolated_database() as (engine, schema):
        flow = workload(0, recovery=True)
        runtime = DurableRuntime(engine, [flow], lease=timedelta(seconds=lease_seconds))
        for _ in range(count):
            runtime.start(flow.name, flow.version, PAYLOAD)
        process = multiprocessing.get_context("spawn").Process(
            target=_crash, args=(schema, count, lease_seconds)
        )
        process.start()
        process.join(timeout=30)
        if process.is_alive():
            process.terminate()
            process.join(timeout=10)
            raise TimeoutError("Recovery probe did not exit")
        if process.exitcode != 77:
            raise RuntimeError(f"Recovery probe exit code {process.exitcode}")
        with engine.connect() as connection:
            exited = connection.scalar(select(func.clock_timestamp()))
        _drain(
            engine, schema, 0, 1, timeout, lease_seconds=lease_seconds, recovery=True
        )
        with engine.connect() as connection:
            replacements = connection.scalars(
                select(AttemptRecord.finished_at).where(
                    AttemptRecord.number == 2,
                    AttemptRecord.state == AttemptState.SUCCEEDED,
                )
            ).all()
    if len(replacements) != count:
        raise RuntimeError("Expected one replacement attempt per crashed claim")
    samples = [(finished - exited).total_seconds() for finished in replacements]
    return {
        "lease_seconds": lease_seconds,
        "seconds": samples,
        "p95_seconds": percentile(samples, 0.95),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=50)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--delay-ms", type=float, default=2)
    parser.add_argument("--timeout", type=float, default=240)
    parser.add_argument("--lease-seconds", type=float, default=30)
    parser.add_argument("--recovery-probes", type=int, default=3)
    parser.add_argument("--output", type=Path, default=Path("artifacts/benchmark.json"))
    args = parser.parse_args()
    if (
        min(args.runs, args.warmup, args.repetitions, *args.workers) < 1
        or args.delay_ms < 0
    ):
        parser.error(
            "Run, warmup, repetition and worker counts must be positive and delay nonnegative"
        )
    if args.timeout <= 0 or args.lease_seconds <= 0 or args.recovery_probes < 0:
        parser.error(
            "Timeout and lease must be positive and recovery probe count nonnegative"
        )
    delay = args.delay_ms / 1000
    with isolated_database() as (engine, _), engine.connect() as connection:
        db_version = connection.scalar(select(func.version()))
    result = {
        "environment": {
            "os": platform.system(),
            "architecture": platform.machine(),
            "logical_cpus": os.cpu_count(),
            "python": platform.python_version(),
            "postgresql": db_version,
        },
        "workload": {
            "runs": args.runs,
            "warmup_runs": args.warmup,
            "steps_per_run": 3,
            "graph_width": 2,
            "payload_bytes": len(PAYLOAD["payload"]),
            "handler_delay_ms": args.delay_ms,
        },
        "durable": {},
    }
    for count in args.workers:
        _sample(args.warmup, delay, count, args.timeout)
        samples = []
        for repetition in range(args.repetitions):
            sample = _sample(args.runs, delay, count, args.timeout)
            samples.append(sample)
            print(
                f"{count} workers, repetition {repetition + 1}: {sample['steps_per_s']:.1f} steps/s",
                flush=True,
            )
        result["durable"][count] = {
            "repetitions": samples,
            "median_steps_per_s": median(s["steps_per_s"] for s in samples),
        }
        write_json(args.output, result)
    flow = workload(delay)
    baseline = []
    for repetition in range(args.repetitions):
        begin = perf_counter()
        for _ in range(args.runs):
            run(flow, PAYLOAD)
        seconds = perf_counter() - begin
        baseline.append(
            {"elapsed_s": seconds, "steps_per_s": args.runs * len(flow.nodes) / seconds}
        )
    result["in_memory_sequential"] = baseline
    if args.recovery_probes:
        result["recovery"] = _recovery(
            args.recovery_probes, args.lease_seconds, args.timeout
        )
    write_json(args.output, result)


if __name__ == "__main__":
    main()
