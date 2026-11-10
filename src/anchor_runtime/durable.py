"""Transactional checkpoints and worker leases for registered DAGs."""

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from types import MappingProxyType
from uuid import UUID, uuid4

from sqlalchemy import Engine, func, or_, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from anchor_runtime.graph import Workflow
from anchor_runtime.runner import encode_input, execute_node
from anchor_runtime.storage import (
    AttemptRecord,
    AttemptState,
    EventRecord,
    RunRecord,
    RunState,
    StepRecord,
    StepState,
    WorkflowVersion,
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

    def execute(self) -> str:
        return execute_node(
            self.workflow, self.node_id, self.run_input, dict(self.parent_outputs)
        )


@dataclass(frozen=True, slots=True)
class RunHistory:
    run: RunRecord
    steps: tuple[StepRecord, ...]
    attempts: tuple[AttemptRecord, ...]
    events: tuple[EventRecord, ...]


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: object) -> str:
    return sha256(_json(value).encode()).hexdigest()


def _definition(workflow: Workflow) -> dict:
    return {
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


class DurableRuntime:
    """Workers share one database and load the versions they can execute."""

    def __init__(
        self,
        engine: Engine,
        workflows: Iterable[Workflow],
        *,
        lease: timedelta = timedelta(seconds=30),
    ) -> None:
        if engine.dialect.name != "postgresql":
            raise ValueError("The durable runtime requires PostgreSQL")
        if lease <= timedelta(0):
            raise ValueError("Lease duration must be positive")
        self._sessions = sessionmaker(engine, expire_on_commit=False)
        self.lease = lease
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
        with self._sessions.begin() as session:
            ready = StepRecord.state == StepState.READY
            expired = (StepRecord.state == StepState.RUNNING) & (
                StepRecord.lease_expires_at <= func.statement_timestamp()
            )
            has_work = (
                select(StepRecord.node_id)
                .where(StepRecord.run_id == RunRecord.id, or_(ready, expired))
                .exists()
            )
            # Every mutation of a run's steps, attempts, and events locks the run first.
            run = session.scalar(
                select(RunRecord)
                .where(
                    RunRecord.state == RunState.ACTIVE,
                    tuple_(RunRecord.workflow_name, RunRecord.workflow_version).in_(
                        self.workflows
                    ),
                    has_work,
                )
                .order_by(RunRecord.created_at, RunRecord.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if run is None:
                return None
            step = session.scalar(
                select(StepRecord)
                .where(StepRecord.run_id == run.id, or_(ready, expired))
                .order_by(ready.desc(), StepRecord.node_id)
                .limit(1)
            )
            if step is None:
                return None

            now = _now(session)
            if step.state == StepState.RUNNING:
                previous = session.get(
                    AttemptRecord, (run.id, step.node_id, step.attempt_number)
                )
                previous.state = AttemptState.LEASE_EXPIRED
                previous.finished_at = now
                _event(session, run, "step.lease_expired", now, step, previous.number)

            step.state = StepState.RUNNING
            step.attempt_number += 1
            step.lease_owner = owner
            step.lease_expires_at = now + self.lease
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
            workflow = self.workflows[run.workflow_name, run.workflow_version]
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

    def fail(self, claim: Claim, error_type: str) -> bool:
        with self._sessions.begin() as session:
            current = self._lock_claim(session, claim)
            if current is None:
                return False
            run, failed, now = current
            failed.state = StepState.FAILED
            _clear_lease(failed)
            attempt = session.get(AttemptRecord, (run.id, failed.node_id, claim.number))
            attempt.state = AttemptState.FAILED
            attempt.finished_at = now
            attempt.error_type = error_type
            _event(session, run, "step.failed", now, failed, claim.number)

            for step in self._steps(session, run.id).values():
                if step.state in {StepState.SUCCEEDED, StepState.FAILED}:
                    continue
                number = None
                if step.state == StepState.RUNNING:
                    number = step.attempt_number
                    attempt = session.get(AttemptRecord, (run.id, step.node_id, number))
                    attempt.state = AttemptState.CANCELLED
                    attempt.finished_at = now
                step.state = StepState.CANCELLED
                _clear_lease(step)
                _event(session, run, "step.cancelled", now, step, number)
            run.state = RunState.FAILED
            _event(session, run, "run.failed", now)
            return True

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
                step.state = StepState.READY
                _event(session, run, "step.ready", now, step)

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
            return RunHistory(
                run,
                tuple(self._steps(session, run_id).values()),
                tuple(attempts),
                tuple(events),
            )
