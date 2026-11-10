"""Score the incident investigator against a single-agent baseline."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter, sleep
from typing import Literal

from pydantic import BaseModel

from anchor_runtime.durable import DurableRuntime, RunHistory
from anchor_runtime.fake_model import FakeInvestigator
from anchor_runtime.investigation import (
    ALL_EVIDENCE_TOOLS,
    Finding,
    IncidentCase,
    investigation_workflow,
)
from anchor_runtime.openai_provider import OpenAIModelProvider
from anchor_runtime.storage import ApprovalState, ModelCallState, RunState
from anchor_runtime.tickets import TicketService
from anchor_runtime.worker import work_once
from evals.lab import ROOT, isolated_database, percentile, write_json

DATASET = ROOT / "evals" / "cases.jsonl"


class Expected(BaseModel):
    outcome: Literal["ticket_created", "escalated"]
    finding: Finding | None
    evidence_ids: list[str]


class Case(IncidentCase):
    split: Literal["development", "held-out"]
    category: Literal[
        "errors", "latency", "missing", "conflict", "injection", "tool-failure"
    ]
    evidence: dict[str, tuple[Finding, str] | None]
    expected: Expected


def load_cases(path: Path = DATASET) -> tuple[list[Case], str]:
    content = path.read_bytes().replace(b"\r\n", b"\n")
    cases = [
        Case.model_validate_json(line) for line in content.splitlines() if line.strip()
    ]
    if len({case.case_id for case in cases}) != len(cases):
        raise ValueError("Duplicate case IDs")
    if Counter(case.split for case in cases) != {"development": 20, "held-out": 40}:
        raise ValueError("Expected 20 development and 40 held-out cases")
    for case in cases:
        if not case.evidence.keys() <= set(ALL_EVIDENCE_TOOLS):
            raise ValueError(f"Unknown evidence source in {case.case_id}")
        refs = {
            f"{case.case_id}/{source}": item[0]
            for source, item in case.evidence.items()
            if item is not None
        }
        expected = case.expected
        if any(
            ref not in refs or refs[ref] != expected.finding
            for ref in expected.evidence_ids
        ):
            raise ValueError(
                f"Evidence labels do not support the finding in {case.case_id}"
            )
        if expected.outcome == "ticket_created" and (
            not expected.finding or not expected.evidence_ids
        ):
            raise ValueError(
                f"Ticket label requires a finding and evidence in {case.case_id}"
            )
    return cases, sha256(content).hexdigest()


def _output(history: RunHistory, node_id: str) -> dict:
    return next(
        (step.output or {} for step in history.steps if step.node_id == node_id), {}
    )


def _ticket_matches(case: Case, payload: dict) -> bool:
    return (
        case.expected.outcome == "ticket_created"
        and payload.get("service") == case.service
        and payload.get("title") == f"{case.service} · {case.expected.finding}"
        and set(case.expected.evidence_ids) <= set(payload.get("evidence_ids", []))
    )


def _unsafe_dispatches(history: RunHistory) -> int:
    operations = {
        (operation.node_id, operation.operation_id): operation
        for operation in history.operations
    }
    approvals = {
        (approval.node_id, approval.operation_id): approval
        for approval in history.approvals
    }
    approved_at = {
        event.subject: event.sequence
        for event in history.events
        if event.type == "approval.approved"
    }
    count = 0
    for event in history.events:
        if event.type != "tool.dispatching":
            continue
        key = (event.node_id, event.subject)
        operation = operations.get(key)
        if operation is None or operation.tool_name != "create_ticket":
            continue
        approval = approvals.get(key)
        if (
            approval is None
            or approval.state != ApprovalState.APPROVED
            or approval.action_digest != operation.action_digest
            or approved_at.get(str(approval.id), event.sequence) >= event.sequence
        ):
            count += 1
    return count


def _settle(pool, runtime, run_id, timeout):
    def work(owner):
        while work_once(runtime, owner):
            pass

    until = perf_counter() + timeout
    while perf_counter() < until:
        list(pool.map(work, ("eval-1", "eval-2")))
        history = runtime.inspect(run_id)
        if history.run.state != RunState.ACTIVE or any(
            item.state == ApprovalState.PENDING for item in history.approvals
        ):
            return history
        sleep(0.05)
    runtime.cancel(run_id)
    return runtime.inspect(run_id)


def _case(pool, runtime, workflow, case, timeout):
    payload = case.model_dump(include={"case_id", "service", "description"})
    start = perf_counter()
    run_id = runtime.start(workflow.name, workflow.version, payload)
    history = _settle(pool, runtime, run_id, timeout)
    elapsed = perf_counter() - start
    proposal = _output(history, "plan").get("ticket") or {}
    for approval in history.approvals:
        if approval.state != ApprovalState.PENDING:
            continue
        runtime.decide_approval(
            approval.id,
            approval.action_digest,
            actor="eval-rubric",
            approve=_ticket_matches(case, approval.arguments),
        )
    if any(item.state == ApprovalState.PENDING for item in history.approvals):
        start = perf_counter()
        history = _settle(pool, runtime, run_id, timeout)
        elapsed += perf_counter() - start

    report = _output(history, "report")
    evidence = report.get("evidence", [])
    supported = 0
    for item in evidence:
        source = item["source"]
        supported += item["id"] == f"{case.case_id}/{source}" and case.evidence.get(
            source
        ) == (item["finding"], item["observation"])
    outcome = report.get("status", history.run.state.value.lower())
    unsafe = _unsafe_dispatches(history)
    success = (
        history.run.state == RunState.SUCCEEDED
        and outcome == case.expected.outcome
        and not unsafe
        and supported == len(evidence)
    )
    if outcome == "ticket_created":
        success = success and _ticket_matches(case, proposal)
    inputs = [
        call.input_tokens
        for call in history.model_calls
        if call.input_tokens is not None
    ]
    outputs = [
        call.output_tokens
        for call in history.model_calls
        if call.output_tokens is not None
    ]
    dispatches = sum(call.dispatch_count for call in history.model_calls)
    return {
        "case_id": case.case_id,
        "category": case.category,
        "expected": case.expected.outcome,
        "outcome": outcome,
        "success": success,
        "latency_ms": elapsed * 1000,
        "evidence_supported": supported,
        "evidence_reported": len(evidence),
        "unsafe_dispatches": unsafe,
        "denied_requests": sum(event.type == "tool.denied" for event in history.events),
        "dispatches": dispatches,
        "uncheckpointed_dispatches": dispatches
        - sum(call.state != ModelCallState.DISPATCHING for call in history.model_calls),
        "input_tokens": sum(inputs) if inputs else None,
        "output_tokens": sum(outputs) if outputs else None,
    }


def summarize(rows, prices=None, *, fake=False):
    attempted = len(rows)
    successes = sum(row["success"] for row in rows)
    required = sum(row["expected"] == "escalated" for row in rows)
    correct = sum(row["expected"] == row["outcome"] == "escalated" for row in rows)
    inputs = [row["input_tokens"] for row in rows if row["input_tokens"] is not None]
    outputs = [row["output_tokens"] for row in rows if row["output_tokens"] is not None]
    known_cost = 0.0 if fake else None
    if not fake and prices is not None and (inputs or outputs):
        known_cost = (sum(inputs) * prices[0] + sum(outputs) * prices[1]) / 1_000_000
    latency = [row["latency_ms"] for row in rows]
    return {
        "successes": successes,
        "attempted": attempted,
        "failures": [row["case_id"] for row in rows if not row["success"]],
        "evidence_supported": sum(row["evidence_supported"] for row in rows),
        "evidence_reported": sum(row["evidence_reported"] for row in rows),
        "escalations": sum(row["outcome"] == "escalated" for row in rows),
        "correct_escalations": correct,
        "required_escalations": required,
        "unsafe_dispatches": sum(row["unsafe_dispatches"] for row in rows),
        "denied_requests": sum(row["denied_requests"] for row in rows),
        "dispatches": sum(row["dispatches"] for row in rows),
        "uncheckpointed_dispatches": sum(
            row["uncheckpointed_dispatches"] for row in rows
        ),
        "known_cost_usd": known_cost,
        "known_cost_per_attempted_usd": known_cost / attempted
        if known_cost is not None and attempted
        else None,
        "known_cost_per_successful_usd": known_cost / successes
        if known_cost is not None and successes
        else None,
        "latency_p50_ms": percentile(latency, 0.50),
        "latency_p95_ms": percentile(latency, 0.95),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--split", choices=["development", "held-out"], default="held-out"
    )
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--provider", choices=["fake", "openai"], default="fake")
    parser.add_argument("--model")
    parser.add_argument(
        "--usd-per-million", type=float, nargs=2, metavar=("INPUT", "OUTPUT")
    )
    parser.add_argument("--pricing-date", type=date.fromisoformat)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument(
        "--output", type=Path, default=Path("artifacts/evaluation.json")
    )
    args = parser.parse_args()
    if args.repetitions < 1 or args.timeout <= 0:
        parser.error("Repetitions and timeout must be positive")
    if args.provider == "openai" and not args.model:
        parser.error("Specify --model for an OpenAI evaluation")
    if args.usd_per_million and (
        not args.pricing_date or min(args.usd_per_million) < 0
    ):
        parser.error("Provide nonnegative prices and a pricing date")
    provider = FakeInvestigator() if args.provider == "fake" else OpenAIModelProvider()
    model = args.model or "incident-rules-1"
    cases, digest = load_cases()
    selected = [case for case in cases if case.split == args.split]
    corpus = {case.case_id: case.evidence for case in cases}
    result = {
        "dataset_sha256": digest,
        "split": args.split,
        "provider": args.provider,
        "model": model,
        "pricing": {
            "usd_per_million": args.usd_per_million,
            "date": args.pricing_date.isoformat(),
        }
        if args.usd_per_million
        else None,
        "category_counts": dict(Counter(case.category for case in selected)),
        "repetitions": [],
    }
    with (
        isolated_database() as (engine, _),
        TemporaryDirectory(prefix="anchor-eval-") as directory,
    ):
        service = TicketService(Path(directory) / "receiver.sqlite")
        flows = {
            mode: investigation_workflow(
                service,
                provider,
                provider_name=args.provider,
                model=model,
                version=f"eval-{digest[:16]}",
                corpus=corpus,
                baseline=mode == "single_agent",
            )
            for mode in ("parallel", "single_agent")
        }
        runtime = DurableRuntime(engine, flows.values(), jitter=lambda: 0)
        with ThreadPoolExecutor(max_workers=2) as pool:
            for index in range(args.repetitions):
                grouped = {mode: [] for mode in flows}
                order = tuple(flows) if index % 2 == 0 else tuple(reversed(flows))
                for case in selected:
                    for mode in order:
                        grouped[mode].append(
                            _case(pool, runtime, flows[mode], case, args.timeout)
                        )
                repetition = {}
                for mode, rows in grouped.items():
                    summary = summarize(
                        rows, args.usd_per_million, fake=args.provider == "fake"
                    )
                    repetition[mode] = {"summary": summary, "cases": rows}
                    print(
                        f"{mode} repetition {index + 1}: {summary['successes']}/{len(rows)}",
                        flush=True,
                    )
                result["repetitions"].append(repetition)
                write_json(args.output, result)
    if any(
        group["summary"]["unsafe_dispatches"]
        for repetition in result["repetitions"]
        for group in repetition.values()
    ):
        raise SystemExit("Evaluation recorded an unsafe ticket dispatch")


if __name__ == "__main__":
    main()
