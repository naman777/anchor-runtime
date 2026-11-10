import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, event, func, select, text, update

from anchor_runtime.durable import (
    Claim,
    DurableRuntime,
    RequestConflict,
    VersionConflict,
)
from anchor_runtime.example import FakeProvider, incident_workflow
from anchor_runtime.graph import Handler, Node, Workflow
from anchor_runtime.storage import AttemptState, RunState, StepRecord, StepState
from anchor_runtime.worker import work_once

pytestmark = pytest.mark.postgres
PAYLOAD = {"service": "checkout", "error_rate": 0.08}


@pytest.fixture
def engine():
    url = os.environ.get("ANCHOR_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set ANCHOR_TEST_DATABASE_URL to run PostgreSQL integration tests")
    schema = f"anchor_test_{uuid4().hex}"
    admin = create_engine(url)
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with admin.begin() as connection:
            connection.execute(text(f"CREATE SCHEMA {schema}"))
        config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()


@pytest.fixture
def workflow():
    return incident_workflow(FakeProvider())


def test_restart_uses_checkpoint_and_original_version(engine, workflow):
    runtime = DurableRuntime(engine, [workflow])
    run_id = runtime.start(
        "incident", "1", PAYLOAD, idempotency_key="checkout-incident"
    )
    assert work_once(runtime, "first")

    newer = replace(workflow, version="2")
    newer_runtime = DurableRuntime(engine, [newer])
    assert newer_runtime.claim("only-v2") is None
    restarted = DurableRuntime(engine, [workflow, newer])
    assert (
        restarted.start("incident", "1", PAYLOAD, idempotency_key="checkout-incident")
        == run_id
    )
    with pytest.raises(RequestConflict):
        restarted.start("incident", "2", PAYLOAD, idempotency_key="checkout-incident")
    changed = replace(workflow, nodes=(Node("new", "collect", "1"),))
    with pytest.raises(VersionConflict):
        DurableRuntime(engine, [changed])

    assert work_once(restarted, "second")
    assert restarted.claim("second") is None
    history = restarted.inspect(run_id)
    assert history.run.state == RunState.SUCCEEDED
    assert history.run.workflow_version == "1"
    assert {step.node_id: step.output for step in history.steps} == {
        "evidence": PAYLOAD,
        "assessment": {"service": "checkout", "severity": "elevated"},
    }
    assert {attempt.node_id: attempt.owner for attempt in history.attempts} == {
        "evidence": "first",
        "assessment": "second",
    }
    assert [event.sequence for event in history.events] == list(range(1, 9))


def test_recovers_after_process_death_and_rejects_expired_attempt(engine, workflow):
    runtime = DurableRuntime(engine, [workflow])
    run_id = runtime.start("incident", "1", PAYLOAD)
    with engine.connect() as connection:
        schema = connection.scalar(select(func.current_schema()))
    env = {**os.environ, "PGOPTIONS": f"-csearch_path={schema}"}
    # The child commits a claim, then waits without committing a result.
    child_code = """
import os
import sys
from sqlalchemy import create_engine
from anchor_runtime.durable import DurableRuntime
from anchor_runtime.example import FakeProvider, incident_workflow
runtime = DurableRuntime(
    create_engine(os.environ["ANCHOR_TEST_DATABASE_URL"]), [incident_workflow(FakeProvider())]
)
claim = runtime.claim("crashed")
print(claim.run_id, flush=True)
sys.stdin.read()
"""
    with subprocess.Popen(
        [sys.executable, "-c", child_code],
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            assert UUID(process.stdout.readline().strip()) == run_id
        finally:
            process.kill()
            process.wait(timeout=10)

    snapshot = runtime.inspect(run_id)
    assert {step.node_id: step.state for step in snapshot.steps} == {
        "evidence": StepState.RUNNING,
        "assessment": StepState.PENDING,
    }
    assert all(step.output is None for step in snapshot.steps)

    # Move the persisted timer into the past to avoid sleeping through a full lease.
    with engine.begin() as connection:
        connection.execute(
            update(StepRecord)
            .where(StepRecord.run_id == run_id, StepRecord.node_id == "evidence")
            .values(lease_expires_at=func.clock_timestamp())
        )
    restarted = DurableRuntime(engine, [workflow])
    stale = Claim(
        run_id,
        "evidence",
        1,
        "crashed",
        workflow,
        '{"service":"checkout","error_rate":0.08}',
        (),
    )
    assert not restarted.renew(stale)
    assert not restarted.succeed(stale, stale.execute())
    current = restarted.claim("crashed")
    assert current.number == 2
    assert not restarted.succeed(stale, current.execute())
    assert not restarted.fail(stale, "LateError")
    assert restarted.renew(current)
    assert restarted.succeed(current, current.execute())
    assert work_once(restarted, "replacement")
    history = restarted.inspect(run_id)
    assert history.run.state == RunState.SUCCEEDED
    assert [
        attempt.state for attempt in history.attempts if attempt.node_id == "evidence"
    ] == [
        AttemptState.LEASE_EXPIRED,
        AttemptState.SUCCEEDED,
    ]


def test_concurrent_workers_claim_once(engine, workflow):
    runtime = DurableRuntime(engine, [workflow])
    runtime.start("incident", "1", PAYLOAD)
    barrier = Barrier(2, timeout=10)

    def claim(owner):
        barrier.wait()
        return runtime.claim(owner)

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(claim, ["first", "second"]))
    assert sum(item is not None for item in claims) == 1


def test_completion_rollback_keeps_checkpoint_and_downstream_uncommitted(
    engine, workflow
):
    runtime = DurableRuntime(engine, [workflow])
    run_id = runtime.start("incident", "1", PAYLOAD)
    claim = runtime.claim("worker")
    output = claim.execute()

    def fail_before_commit(conn):
        raise RuntimeError("injected commit failure")

    event.listen(engine, "commit", fail_before_commit)
    try:
        with pytest.raises(RuntimeError, match="injected commit failure"):
            runtime.succeed(claim, output)
    finally:
        event.remove(engine, "commit", fail_before_commit)

    history = runtime.inspect(run_id)
    assert [event.type for event in history.events] == [
        "run.started",
        "step.ready",
        "step.started",
    ]
    assert history.attempts[0].state == AttemptState.RUNNING
    assert all(step.output is None for step in history.steps)
    assert runtime.claim("other-worker") is None
    assert runtime.succeed(claim, output)


def test_join_schedules_once_after_concurrent_parent_completion(engine, workflow):
    def join(context):
        assert tuple(context.parents) == ("right", "left")
        return context.parents["right"]

    evidence = workflow.handlers[0]
    fanout = Workflow(
        "fanout",
        "1",
        workflow.input_schema,
        (
            Node("left", "collect", "1"),
            Node("right", "collect", "1"),
            Node("join", "join", "1", ("right", "left")),
        ),
        (evidence, Handler("join", "1", evidence.output_schema, join)),
    )
    runtime = DurableRuntime(engine, [fanout])
    run_id = runtime.start("fanout", "1", PAYLOAD)
    claims = [runtime.claim("first"), runtime.claim("second")]
    assert runtime.claim("third") is None
    barrier = Barrier(2, timeout=10)

    def complete(claim):
        output = claim.execute()
        barrier.wait()
        return runtime.succeed(claim, output)

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert all(pool.map(complete, claims))
    assert work_once(runtime, "third")
    history = runtime.inspect(run_id)
    assert history.run.state == RunState.SUCCEEDED
    assert (
        sum(
            event.type == "step.ready" and event.node_id == "join"
            for event in history.events
        )
        == 1
    )


def test_failure_blocks_downstream_work(engine, workflow):
    def broken(context):
        raise RuntimeError("sensitive details")

    handlers = (replace(workflow.handlers[0], execute=broken), workflow.handlers[1])
    runtime = DurableRuntime(engine, [replace(workflow, handlers=handlers)])
    run_id = runtime.start("incident", "1", PAYLOAD)
    assert work_once(runtime, "worker")
    assert not work_once(runtime, "worker")
    history = runtime.inspect(run_id)
    assert history.run.state == RunState.FAILED
    assert history.attempts[0].error_type == "RuntimeError"
    assert {step.node_id: step.state for step in history.steps} == {
        "evidence": StepState.FAILED,
        "assessment": StepState.CANCELLED,
    }
