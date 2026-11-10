import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from threading import Barrier

import pytest
from sqlalchemy import func, update

from anchor_runtime.durable import DurableRuntime
from anchor_runtime.fake_model import FakeInvestigator
from anchor_runtime.inspection import run_view
from anchor_runtime.investigation import investigation_workflow
from anchor_runtime.models import ModelLimitExceeded, ModelResponse
from anchor_runtime.policy import RetryableError
from anchor_runtime.storage import ModelCallState, RunState, StepRecord, StepState
from anchor_runtime.tickets import TicketService
from anchor_runtime.worker import work_once

pytestmark = pytest.mark.postgres


def _payload(case_id):
    return {
        "case_id": case_id,
        "service": "checkout",
        "description": "Synthetic checkout incident",
    }


def _workflow(path, provider=None):
    return investigation_workflow(
        TicketService(path),
        provider or FakeInvestigator(),
        provider_name="fake",
        model="incident-rules-1",
        version="1",
    )


def _drain(runtime):
    for _ in range(12):
        if not work_once(runtime, "worker"):
            return
    pytest.fail("Workflow did not settle")


@pytest.mark.parametrize(
    ("case_id", "detail_tool"),
    [("errors", "error_logs"), ("latency", "latency_details")],
)
def test_investigators_choose_evidence_paths_and_wait_for_review(
    engine, tmp_path, case_id, detail_tool
):
    path = tmp_path / "receiver.sqlite"
    workflow = _workflow(path)
    runtime = DurableRuntime(engine, [workflow])
    run_id = runtime.start(workflow.name, workflow.version, _payload(case_id))
    _drain(runtime)
    waiting = runtime.inspect(run_id)
    assert waiting.run.state == RunState.ACTIVE
    assert (
        next(step.state for step in waiting.steps if step.node_id == "ticket")
        == StepState.WAITING_APPROVAL
    )
    assert {operation.tool_name for operation in waiting.operations} == {
        "metrics",
        detail_tool,
        "runbook",
        "deployments",
    }
    assert len(waiting.model_calls) == 6
    assert all(call.state == ModelCallState.SUCCEEDED for call in waiting.model_calls)
    (approval,) = waiting.approvals
    assert approval.arguments["evidence_ids"] == [
        f"{case_id}/metrics",
        f"{case_id}/{detail_tool}",
        f"{case_id}/runbook",
        f"{case_id}/deployments",
    ]
    with closing(sqlite3.connect(path)) as receiver:
        assert receiver.execute("SELECT count(*) FROM tickets").fetchone() == (0,)

    restarted = DurableRuntime(engine, [workflow])
    restarted.decide_approval(
        approval.id, approval.action_digest, approve=True, actor="reviewer"
    )
    _drain(restarted)
    completed = restarted.inspect(run_id)
    report = next(step.output for step in completed.steps if step.node_id == "report")
    assert completed.run.state == RunState.SUCCEEDED
    assert report["status"] == "ticket_created"
    assert [item["id"] for item in report["evidence"]] == approval.arguments[
        "evidence_ids"
    ]
    with closing(sqlite3.connect(path)) as receiver:
        assert receiver.execute("SELECT ticket_id FROM tickets").fetchall() == [
            (report["ticket_id"],)
        ]


@pytest.mark.parametrize("case_id", ["missing", "conflict", "injection"])
def test_weak_conflicting_and_malicious_evidence_escalates(engine, tmp_path, case_id):
    workflow = _workflow(tmp_path / "receiver.sqlite")
    runtime = DurableRuntime(engine, [workflow])
    run_id = runtime.start(workflow.name, workflow.version, _payload(case_id))
    _drain(runtime)
    history = runtime.inspect(run_id)
    report = next(step.output for step in history.steps if step.node_id == "report")
    assert history.run.state == RunState.SUCCEEDED
    assert report["status"] == "escalated"
    assert history.approvals == ()
    assert not any(
        operation.tool_name == "create_ticket" for operation in history.operations
    )
    assert any(event.type == "tool.denied" for event in history.events) == (
        case_id == "injection"
    )


def test_retry_reuses_completed_model_and_tool_rounds(engine, tmp_path):
    attempted = []
    fake = FakeInvestigator()

    class TransientProvider:
        def complete(self, request):
            observations = tuple(request.payload["observations"])
            attempted.append(observations)
            if observations == ("runbook",) and attempted.count(observations) == 1:
                raise RetryableError("simulated provider outage")
            return replace(fake.complete(request), input_tokens=11, output_tokens=5)

    workflow = _workflow(tmp_path / "receiver.sqlite", TransientProvider())
    runtime = DurableRuntime(engine, [workflow], jitter=lambda: 0)
    run_id = runtime.start(workflow.name, workflow.version, _payload("errors"))
    assert work_once(runtime, "first")
    partial = runtime.inspect(run_id)
    assert [call.state for call in partial.model_calls] == [
        ModelCallState.SUCCEEDED,
        ModelCallState.DISPATCHING,
    ]
    assert len(partial.operations) == 1
    with engine.begin() as connection:
        connection.execute(
            update(StepRecord)
            .where(StepRecord.run_id == run_id)
            .values(next_attempt_at=func.clock_timestamp())
        )

    restarted = DurableRuntime(engine, [workflow])
    assert work_once(restarted, "replacement")
    completed = restarted.inspect(run_id)
    assert attempted.count(()) == 1
    assert attempted.count(("runbook",)) == 2
    assert [call.dispatch_count for call in completed.model_calls] == [1, 2, 1]
    assert sum(call.input_tokens for call in completed.model_calls) == 33
    assert run_view(completed)["usage"] == {
        "reported_input_tokens": 33,
        "reported_output_tokens": 15,
        "dispatches": 4,
        "uncheckpointed_dispatches": 1,
    }
    assert (
        sum(
            event.type == "tool.dispatching" and event.node_id == "changes"
            for event in completed.events
        )
        == 2
    )


def test_parallel_agents_reserve_run_budget_atomically(engine, tmp_path):
    workflow = replace(_workflow(tmp_path / "receiver.sqlite"), max_model_dispatches=1)
    runtime = DurableRuntime(engine, [workflow])
    run_id = runtime.start(workflow.name, workflow.version, _payload("errors"))
    claims = [runtime.claim("first"), runtime.claim("second")]
    barrier = Barrier(2, timeout=10)

    def dispatch(claim):
        barrier.wait()
        try:
            runtime.call_model(claim, 1, {"role": claim.node_id, "observations": {}})
        except ModelLimitExceeded:
            return False
        return True

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(dispatch, claims)) == 1
    assert len(runtime.inspect(run_id).model_calls) == 1


def test_invalid_model_response_is_recorded_with_reported_usage(engine, tmp_path):
    class InvalidProvider:
        def complete(self, _request):
            return ModelResponse(
                {"action": "unrecognized"}, input_tokens=7, output_tokens=3
            )

    workflow = _workflow(tmp_path / "receiver.sqlite", InvalidProvider())
    runtime = DurableRuntime(engine, [workflow])
    run_id = runtime.start(workflow.name, workflow.version, _payload("errors"))
    _drain(runtime)
    history = runtime.inspect(run_id)
    assert all(
        call.state == ModelCallState.REJECTED and call.input_tokens == 7
        for call in history.model_calls
    )
    assert history.operations == ()
    assert (
        next(
            step.output["status"] for step in history.steps if step.node_id == "report"
        )
        == "escalated"
    )
