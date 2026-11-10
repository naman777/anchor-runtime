"""Transactional checkpoints and worker leases for registered DAGs."""

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from random import random
from types import MappingProxyType
from uuid import UUID, uuid4

from sqlalchemy import Engine, and_, func, or_, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from anchor_runtime.graph import Workflow
from anchor_runtime.runner import encode_checkpoint, encode_input, execute_node
from anchor_runtime.storage import (
    AttemptRecord,
    AttemptState,
    EventRecord,
    OperationState,
    RunRecord,
    RunState,
    StepRecord,
    StepState,
    ToolOperation,
    WorkflowVersion,
)
from anchor_runtime.tools import (
    LeaseLost,
    ReconciliationRequired,
    ToolCaller,
    ToolConflict,
)


class VersionConflict(ValueError):
    """A registered workflow version has a different definition."""


class RequestConflict(ValueError):
    """An idempotency key already belongs to a different request."""


@dataclass(frozen=True, slots=True)
class Claim:
    run_id: UUID
    node_id: str
    number: int
    owner: str
    workflow: Workflow
    run_input: str
    parent_outputs: tuple[tuple[str, str], ...]

    def execute(self, call_tool: ToolCaller | None = None) -> str:
        return execute_node(
            self.workflow,
            self.node_id,
            self.run_input,
            dict(self.parent_outputs),
            call_tool,
        )


@dataclass(frozen=True, slots=True)
class RunHistory:
    run: RunRecord
    steps: tuple[StepRecord, ...]
    attempts: tuple[AttemptRecord, ...]
    events: tuple[EventRecord, ...]
    operations: tuple[ToolOperation, ...]


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: object) -> str:
    return sha256(_json(value).encode()).hexdigest()


def _definition(workflow: Workflow) -> dict:
    definition = {
        "input_schema": workflow.input_schema.model_json_schema(),
        "nodes": [
            {
                "id": node_id,
                "parents": workflow.nodes_by_id[node_id].parents,
                "handler": list(workflow.nodes_by_id[node_id].handler_key),
                "output_schema": workflow.handlers_by_key[
                    workflow.nodes_by_id[node_id].handler_key
                ].output_schema.model_json_schema(mode="serialization"),
            }
            for node_id in workflow.order
        ],
    }
    # Keep the digest for pre-policy workflow definitions stable across migrations.
    if workflow.timeout is not None:
        definition["timeout"] = workflow.timeout.total_seconds()
    for entry in definition["nodes"]:
        node = workflow.nodes_by_id[entry["id"]]
        if node.timeout is not None:
            entry["timeout"] = node.timeout.total_seconds()
        if node.retry is not None:
            entry["retry"] = {
                "max_attempts": node.retry.max_attempts,
                "base_delay": node.retry.base_delay.total_seconds(),
                "max_delay": node.retry.max_delay.total_seconds(),
            }
        if node.tools:
            entry["tools"] = [
                {
                    "name": tool.name,
                    "version": tool.version,
                    "schema": tool.input_schema.model_json_schema(),
                    "idempotent": tool.idempotent,
                    "requires_approval": tool.requires_approval,
                }
                for tool in (
                    workflow.tools_by_name[name] for name in sorted(node.tools)
                )
            ]
    return definition


def _now(session: Session) -> datetime:
    return session.scalar(select(func.clock_timestamp()))


def _event(
    session: Session,
    run: RunRecord,
    event_type: str,
    now: datetime,
    step: StepRecord | None = None,
    attempt: int | None = None,
) -> None:
    run.event_sequence += 1
    session.add(
        EventRecord(
            run_id=run.id,
            sequence=run.event_sequence,
            type=event_type,
            at=now,
            node_id=step.node_id if step is not None else None,
            attempt_number=attempt,
        )
    )


def _clear_lease(step: StepRecord) -> None:
    step.lease_owner = None
    step.lease_expires_at = None
    step.deadline = None


class DurableRuntime:
    """Workers share one database and load the versions they can execute."""

    def __init__(
        self,
        engine: Engine,
        workflows: Iterable[Workflow],
        *,
        lease: timedelta = timedelta(seconds=30),
        jitter: Callable[[], float] = random,
    ) -> None:
        if engine.dialect.name != "postgresql":
            raise ValueError("The durable runtime requires PostgreSQL")
        if lease <= timedelta(0):
            raise ValueError("Lease duration must be positive")
        self._sessions = sessionmaker(engine, expire_on_commit=False)
        self.lease = lease
        self._jitter = jitter
        definitions = {}
        for workflow in workflows:
            key = workflow.name, workflow.version
            if key in definitions:
                raise ValueError(f"Duplicate workflow version {key!r}")
            definitions[key] = workflow
        self.workflows = MappingProxyType(definitions)
        with self._sessions.begin() as session:
            # Consistent order when multiple workers register overlapping versions.
            for key, workflow in sorted(self.workflows.items()):
                definition = _definition(workflow)
                digest = _digest(definition)
                session.execute(
                    insert(WorkflowVersion)
                    .values(
                        name=key[0],
                        version=key[1],
                        definition=definition,
                        digest=digest,
                    )
                    .on_conflict_do_nothing()
                )
                if session.get(WorkflowVersion, key).digest != digest:
                    raise VersionConflict(
                        f"Definition changed for workflow version {key!r}"
                    )

    def start(
        self,
        name: str,
        version: str,
        payload: object,
        *,
        idempotency_key: str | None = None,
    ) -> UUID:
        workflow = self.workflows[name, version]
        run_input = json.loads(encode_input(workflow, payload))
        request_digest = _digest([name, version, run_input])
        with self._sessions.begin() as session:
            now = _now(session)
            run_id = session.scalar(
                insert(RunRecord)
                .values(
                    id=uuid4(),
                    workflow_name=name,
                    workflow_version=version,
                    state=RunState.ACTIVE,
                    input=run_input,
                    idempotency_key=idempotency_key,
                    request_digest=request_digest,
                    created_at=now,
                    deadline=now + workflow.timeout if workflow.timeout else None,
                )
                .on_conflict_do_nothing(index_elements=[RunRecord.idempotency_key])
                .returning(RunRecord.id)
            )
            if run_id is None:
                existing = session.scalar(
                    select(RunRecord).where(
                        RunRecord.idempotency_key == idempotency_key
                    )
                )
                if existing.request_digest != request_digest:
                    raise RequestConflict(
                        "Idempotency key already used for a different run request"
                    )
                return existing.id

            run = session.get(RunRecord, run_id)
            steps = {
                node_id: StepRecord(
                    run_id=run_id, node_id=node_id, state=StepState.PENDING
                )
                for node_id in workflow.order
            }
            session.add_all(steps.values())
            _event(session, run, "run.started", now)
            self._schedule(session, workflow, run, steps, now)
            return run_id

    def claim(self, owner: str) -> Claim | None:
        if not owner:
            raise ValueError("Worker owner is required")
        if not self.workflows:
            return None
        self.advance_timers()
        with self._sessions.begin() as session:
            ready = StepRecord.state == StepState.READY
            run = self._next_run(
                session,
                and_(
                    or_(
                        RunRecord.deadline.is_(None),
                        RunRecord.deadline > func.statement_timestamp(),
                    ),
                    self._has_step(ready),
                ),
            )
            if run is None:
                return None
            step = session.scalar(
                select(StepRecord)
                .where(StepRecord.run_id == run.id, ready)
                .order_by(StepRecord.node_id)
                .limit(1)
            )
            if step is None:
                return None

            now = _now(session)
            if run.deadline is not None and run.deadline <= now:
                return None

            workflow = self.workflows[run.workflow_name, run.workflow_version]
            node = workflow.nodes_by_id[step.node_id]
            step.state = StepState.RUNNING
            step.attempt_number += 1
            step.lease_owner = owner
            step.lease_expires_at = now + self.lease
            step.deadline = now + node.timeout if node.timeout else None
            session.add(
                AttemptRecord(
                    run_id=run.id,
                    node_id=step.node_id,
                    number=step.attempt_number,
                    owner=owner,
                    state=AttemptState.RUNNING,
                    started_at=now,
                )
            )
            _event(session, run, "step.started", now, step, step.attempt_number)
            parent_outputs = tuple(
                (parent, _json(session.get(StepRecord, (run.id, parent)).output))
                for parent in workflow.nodes_by_id[step.node_id].parents
            )
            return Claim(
                run.id,
                step.node_id,
                step.attempt_number,
                owner,
                workflow,
                _json(run.input),
                parent_outputs,
            )

    @staticmethod
    def _has_step(predicate):
        return (
            select(StepRecord.node_id)
            .where(StepRecord.run_id == RunRecord.id, predicate)
            .exists()
        )

    def _next_run(self, session: Session, predicate, *, registered_only: bool = True):
        # All mutations of steps, attempts, operations and events lock the run first.
        query = select(RunRecord).where(RunRecord.state == RunState.ACTIVE, predicate)
        if registered_only:
            query = query.where(
                tuple_(RunRecord.workflow_name, RunRecord.workflow_version).in_(
                    self.workflows
                )
            )
        return session.scalar(
            query.order_by(RunRecord.created_at, RunRecord.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )

    def advance_timers(self) -> bool:
        """Resolve due timers for one run. Poll from workers or a scheduler."""
        time = func.statement_timestamp()
        due = or_(
            (StepRecord.state == StepState.RETRY_WAIT)
            & (StepRecord.next_attempt_at <= time),
            (StepRecord.state == StepState.RUNNING)
            & or_(StepRecord.deadline <= time, StepRecord.lease_expires_at <= time),
        )
        with self._sessions.begin() as session:
            run = self._next_run(
                session, or_(RunRecord.deadline <= time, self._has_step(due))
            )
            if run is None:
                return False
            now = _now(session)
            if run.deadline is not None and run.deadline <= now:
                _event(session, run, "run.deadline_exceeded", now)
                self._terminate(session, run, now)
                return True

            for step in self._steps(session, run.id).values():
                if step.state == StepState.RETRY_WAIT and step.next_attempt_at <= now:
                    self._make_ready(session, run, step, now)
                elif step.state == StepState.RUNNING:
                    if step.deadline is not None and step.deadline <= now:
                        self._finish_failure(
                            session, run, step, now, "StepTimeout", retryable=True
                        )
                    elif step.lease_expires_at <= now:
                        self._finish_failure(
                            session,
                            run,
                            step,
                            now,
                            "LeaseExpired",
                            retryable=True,
                            expired=True,
                        )
                if run.state != RunState.ACTIVE:
                    break
            return True

    def _lock_claim(
        self, session: Session, claim: Claim
    ) -> tuple[RunRecord, StepRecord, datetime] | None:
        run = session.get(RunRecord, claim.run_id, with_for_update=True)
        if (
            run is None
            or run.state != RunState.ACTIVE
            or (run.workflow_name, run.workflow_version)
            != (claim.workflow.name, claim.workflow.version)
        ):
            return None
        step = session.get(StepRecord, (claim.run_id, claim.node_id))
        now = _now(session)
        if (
            step is None
            or step.state != StepState.RUNNING
            or step.attempt_number != claim.number
            or step.lease_owner != claim.owner
            or step.lease_expires_at <= now
            or (step.deadline is not None and step.deadline <= now)
            or (run.deadline is not None and run.deadline <= now)
        ):
            return None
        return run, step, now

    def renew(self, claim: Claim) -> bool:
        with self._sessions.begin() as session:
            current = self._lock_claim(session, claim)
            if current is None:
                return False
            _, step, now = current
            step.lease_expires_at = now + self.lease
            return True

    def execute(self, claim: Claim) -> str:
        return claim.execute(
            lambda name, operation_id, arguments: self.call_tool(
                claim, name, operation_id, arguments
            )
        )

    def call_tool(
        self, claim: Claim, name: str, operation_id: str, arguments: object
    ) -> object:
        workflow = self.workflows[claim.workflow.name, claim.workflow.version]
        node = workflow.nodes_by_id[claim.node_id]
        if name not in node.tools:
            raise PermissionError(f"Tool {name!r} is not allowed for this step")
        if not operation_id or len(operation_id) > 200:
            raise ValueError("Operation ID must contain 1 to 200 characters")
        tool = workflow.tools_by_name[name]
        if tool.requires_approval:
            raise PermissionError("Protected tool requires a durable approval")
        encoded = encode_checkpoint(tool.input_schema, arguments)
        payload = json.loads(encoded)
        validated = tool.input_schema.model_validate_json(encoded, by_name=True)
        digest = _digest([tool.name, tool.version, payload])
        identity = (claim.run_id, claim.node_id, operation_id)
        key = _digest([str(claim.run_id), claim.node_id, operation_id])
        needs_reconciliation = False
        with self._sessions.begin() as session:
            current = self._lock_claim(session, claim)
            if current is None:
                raise LeaseLost("Cannot dispatch without a current lease")
            run, step, now = current
            operation = session.get(ToolOperation, identity)
            if operation is None:
                session.add(
                    ToolOperation(
                        run_id=claim.run_id,
                        node_id=claim.node_id,
                        operation_id=operation_id,
                        key=key,
                        tool_name=name,
                        action_digest=digest,
                        arguments=payload,
                        idempotent=tool.idempotent,
                        state=OperationState.DISPATCHING,
                    )
                )
            else:
                if operation.action_digest != digest:
                    raise ToolConflict(
                        "Operation ID already belongs to a different action"
                    )
                if operation.state == OperationState.SUCCEEDED:
                    return operation.result
                if not operation.idempotent:
                    needs_reconciliation = self._flag_ambiguous_operations(
                        session, run, step, now
                    )
            if not needs_reconciliation:
                _event(session, run, "tool.dispatching", now, step, claim.number)
        if needs_reconciliation:
            raise ReconciliationRequired(
                "Check the external system before repeating this operation"
            )

        try:
            result = json.loads(_json(tool.dispatch(key, validated)))
        except Exception as error:
            if tool.idempotent:
                raise
            with self._sessions.begin() as session:
                current = self._lock_claim(session, claim)
                if current is not None:
                    self._flag_ambiguous_operations(session, *current)
            raise ReconciliationRequired(
                "External operation outcome is unknown"
            ) from error

        with self._sessions.begin() as session:
            current = self._lock_claim(session, claim)
            if current is None:
                raise LeaseLost("Cannot record a tool result without a current lease")
            run, step, now = current
            operation = session.get(ToolOperation, identity)
            if operation.state == OperationState.NEEDS_RECONCILIATION:
                raise ReconciliationRequired(
                    "External operation outcome requires review"
                )
            if operation.state == OperationState.SUCCEEDED:
                return operation.result
            operation.result = result
            operation.state = OperationState.SUCCEEDED
            _event(session, run, "tool.succeeded", now, step, claim.number)
            return result

    def succeed(self, claim: Claim, output: str) -> bool:
        workflow = self.workflows[claim.workflow.name, claim.workflow.version]
        handler = workflow.handlers_by_key[
            workflow.nodes_by_id[claim.node_id].handler_key
        ]
        handler.output_schema.model_validate_json(output, by_name=True)
        checkpoint = json.loads(output)
        with self._sessions.begin() as session:
            current = self._lock_claim(session, claim)
            if current is None:
                return False
            run, step, now = current
            if self._flag_ambiguous_operations(session, run, step, now):
                self._finish_failure(session, run, step, now, "ReconciliationRequired")
                return True
            step.output = checkpoint
            step.state = StepState.SUCCEEDED
            _clear_lease(step)
            attempt = session.get(AttemptRecord, (run.id, step.node_id, claim.number))
            attempt.state = AttemptState.SUCCEEDED
            attempt.finished_at = now
            _event(session, run, "step.succeeded", now, step, claim.number)
            steps = self._steps(session, run.id)
            self._schedule(session, workflow, run, steps, now)
            if all(step.state == StepState.SUCCEEDED for step in steps.values()):
                run.state = RunState.SUCCEEDED
                _event(session, run, "run.succeeded", now)
            return True

    def fail(
        self,
        claim: Claim,
        error_type: str,
        *,
        retryable: bool = False,
        retry_after: timedelta = timedelta(0),
    ) -> bool:
        with self._sessions.begin() as session:
            current = self._lock_claim(session, claim)
            if current is None:
                return False
            run, step, now = current
            self._finish_failure(
                session,
                run,
                step,
                now,
                error_type,
                retryable=retryable,
                retry_after=retry_after,
            )
            return True

    def _finish_failure(
        self,
        session: Session,
        run: RunRecord,
        step: StepRecord,
        now: datetime,
        error_type: str,
        *,
        retryable: bool = False,
        expired: bool = False,
        retry_after: timedelta = timedelta(0),
    ) -> None:
        workflow = self.workflows[run.workflow_name, run.workflow_version]
        policy = workflow.nodes_by_id[step.node_id].retry
        if self._flag_ambiguous_operations(session, run, step, now):
            retryable = False
            error_type = "ReconciliationRequired"
        number = step.attempt_number
        attempt = session.get(AttemptRecord, (run.id, step.node_id, number))
        attempt.state = AttemptState.LEASE_EXPIRED if expired else AttemptState.FAILED
        attempt.finished_at = now
        attempt.error_type = error_type
        _clear_lease(step)
        _event(
            session,
            run,
            "step.lease_expired" if expired else "step.failed",
            now,
            step,
            number,
        )
        if retryable and expired and policy is None:
            self._make_ready(session, run, step, now)
        elif retryable and policy is not None and number < policy.max_attempts:
            step.state = StepState.RETRY_WAIT
            step.next_attempt_at = now + max(
                retry_after, policy.delay(number, self._jitter())
            )
            _event(session, run, "step.retry_scheduled", now, step, number)
        else:
            step.state = StepState.FAILED
            self._terminate(session, run, now)

    def _terminate(self, session: Session, run: RunRecord, now: datetime) -> None:
        for step in self._steps(session, run.id).values():
            if step.state in {StepState.SUCCEEDED, StepState.FAILED}:
                continue
            number = None
            if step.state == StepState.RUNNING:
                number = step.attempt_number
                attempt = session.get(AttemptRecord, (run.id, step.node_id, number))
                attempt.state = AttemptState.CANCELLED
                attempt.finished_at = now
                self._flag_ambiguous_operations(session, run, step, now)
            step.state = StepState.CANCELLED
            step.next_attempt_at = None
            _clear_lease(step)
            _event(session, run, "step.cancelled", now, step, number)
        run.state = RunState.FAILED
        _event(session, run, "run.failed", now)

    @staticmethod
    def _make_ready(
        session: Session, run: RunRecord, step: StepRecord, now: datetime
    ) -> None:
        step.state = StepState.READY
        step.next_attempt_at = None
        _event(session, run, "step.ready", now, step)

    @staticmethod
    def _flag_ambiguous_operations(
        session: Session,
        run: RunRecord,
        step: StepRecord,
        now: datetime,
    ) -> bool:
        operations = session.scalars(
            select(ToolOperation)
            .where(
                ToolOperation.run_id == run.id,
                ToolOperation.node_id == step.node_id,
                ToolOperation.state.in_(
                    [OperationState.DISPATCHING, OperationState.NEEDS_RECONCILIATION]
                ),
                ToolOperation.idempotent.is_(False),
            )
            .order_by(ToolOperation.operation_id)
        ).all()
        for operation in operations:
            if operation.state == OperationState.DISPATCHING:
                operation.state = OperationState.NEEDS_RECONCILIATION
                _event(
                    session,
                    run,
                    "tool.needs_reconciliation",
                    now,
                    step,
                    step.attempt_number,
                )
        return bool(operations)

    @staticmethod
    def _steps(session: Session, run_id: UUID) -> dict[str, StepRecord]:
        rows = session.scalars(
            select(StepRecord)
            .where(StepRecord.run_id == run_id)
            .order_by(StepRecord.node_id)
        )
        return {step.node_id: step for step in rows}

    @staticmethod
    def _schedule(
        session: Session,
        workflow: Workflow,
        run: RunRecord,
        steps: dict[str, StepRecord],
        now: datetime,
    ) -> None:
        for node_id in workflow.order:
            step = steps[node_id]
            if step.state == StepState.PENDING and all(
                steps[parent].state == StepState.SUCCEEDED
                for parent in workflow.nodes_by_id[node_id].parents
            ):
                DurableRuntime._make_ready(session, run, step, now)

    def inspect(self, run_id: UUID) -> RunHistory | None:
        with self._sessions.begin() as session:
            run = session.get(RunRecord, run_id, with_for_update={"read": True})
            if run is None:
                return None
            attempts = session.scalars(
                select(AttemptRecord)
                .where(AttemptRecord.run_id == run_id)
                .order_by(AttemptRecord.node_id, AttemptRecord.number)
            )
            events = session.scalars(
                select(EventRecord)
                .where(EventRecord.run_id == run_id)
                .order_by(EventRecord.sequence)
            )
            operations = session.scalars(
                select(ToolOperation)
                .where(ToolOperation.run_id == run_id)
                .order_by(ToolOperation.node_id, ToolOperation.operation_id)
            )
            return RunHistory(
                run,
                tuple(self._steps(session, run_id).values()),
                tuple(attempts),
                tuple(events),
                tuple(operations),
            )
