"""Transactional checkpoints and worker leases for registered DAGs."""

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from random import random
from types import MappingProxyType
from uuid import UUID, uuid4

from pydantic import BaseModel
from sqlalchemy import Engine, and_, func, or_, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from anchor_runtime.branches import Readiness, select_parents, selected_route
from anchor_runtime.graph import Workflow
from anchor_runtime.models import (
    AgentSpec,
    ModelCaller,
    ModelLimitExceeded,
    ModelOutputRejected,
    ModelRequest,
)
from anchor_runtime.runner import encode_checkpoint, encode_input, execute_node
from anchor_runtime.storage import (
    ApprovalRecord,
    ApprovalState,
    AttemptRecord,
    AttemptState,
    EventRecord,
    ModelCallRecord,
    ModelCallState,
    OperationState,
    RunRecord,
    RunState,
    StepRecord,
    StepState,
    ToolOperation,
    WorkflowVersion,
)
from anchor_runtime.tools import (
    ApprovalWait,
    LeaseLost,
    ReconciliationRequired,
    Tool,
    ToolCaller,
    ToolConflict,
)


class VersionConflict(ValueError):
    """A registered workflow version has a different definition."""


class RequestConflict(ValueError):
    """An idempotency key already belongs to a different request."""


class ApprovalConflict(ValueError):
    """An approval decision conflicts with the action or recorded decision."""


@dataclass(frozen=True, slots=True)
class Claim:
    run_id: UUID
    node_id: str
    number: int
    owner: str
    workflow: Workflow
    run_input: str
    parent_outputs: tuple[tuple[str, str], ...]

    def execute(
        self,
        call_tool: ToolCaller | None = None,
        should_stop: Callable[[], bool] | None = None,
        call_model: ModelCaller | None = None,
    ) -> str:
        return execute_node(
            self.workflow,
            self.node_id,
            self.run_input,
            dict(self.parent_outputs),
            call_tool,
            should_stop,
            call_model,
        )


@dataclass(frozen=True, slots=True)
class RunHistory:
    run: RunRecord
    steps: tuple[StepRecord, ...]
    attempts: tuple[AttemptRecord, ...]
    events: tuple[EventRecord, ...]
    operations: tuple[ToolOperation, ...]
    approvals: tuple[ApprovalRecord, ...]
    model_calls: tuple[ModelCallRecord, ...]


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
    if workflow.max_model_dispatches is not None:
        definition["max_model_dispatches"] = workflow.max_model_dispatches
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
        if node.routes:
            entry["routes"] = sorted(node.routes)
        if node.when:
            entry["when"] = dict(node.when)
        if node.join_selected:
            entry["join_selected"] = True
        if agent := node.agent:
            entry["agent"] = {
                "provider": agent.provider_name,
                "model": agent.model,
                "prompt_version": agent.prompt_version,
                "instructions": agent.instructions,
                "schema": agent.response_schema.model_json_schema(),
                "max_rounds": agent.max_rounds,
                "max_input_bytes": agent.max_input_bytes,
                "max_output_tokens": agent.max_output_tokens,
                "max_dispatches_per_round": agent.max_dispatches_per_round,
            }
        if node.tools:
            entry["tools"] = [
                {
                    "name": tool.name,
                    "version": tool.version,
                    "schema": tool.input_schema.model_json_schema(),
                    "idempotent": tool.idempotent,
                    "requires_approval": tool.requires_approval,
                    **(
                        {
                            "destination": tool.destination,
                            "approval_timeout": tool.approval_timeout.total_seconds(),
                        }
                        if tool.requires_approval
                        else {}
                    ),
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


def _completed_routes(
    workflow: Workflow, steps: dict[str, StepRecord]
) -> dict[str, str | None]:
    return {
        node_id: selected_route(workflow.nodes_by_id[node_id], step.output)
        for node_id, step in steps.items()
        if step.state == StepState.SUCCEEDED
    }


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
            parents = {
                parent: session.get(StepRecord, (run.id, parent))
                for parent in node.parents
            }
            readiness, selected = select_parents(
                node,
                _completed_routes(workflow, parents),
                {
                    parent
                    for parent, record in parents.items()
                    if record.state == StepState.SKIPPED
                },
            )
            if readiness != Readiness.READY:
                raise RuntimeError(
                    f"Ready step {step.node_id!r} has unsatisfied dependencies"
                )
            parent_outputs = tuple(
                (parent, _json(parents[parent].output)) for parent in selected
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
            expired_approval = (
                select(ApprovalRecord.id)
                .where(
                    ApprovalRecord.run_id == RunRecord.id,
                    ApprovalRecord.state == ApprovalState.PENDING,
                    ApprovalRecord.deadline <= time,
                )
                .exists()
            )
            run = self._next_run(
                session,
                or_(RunRecord.deadline <= time, self._has_step(due), expired_approval),
            )
            if run is None:
                return False
            now = _now(session)
            if run.deadline is not None and run.deadline <= now:
                _event(session, run, "run.deadline_exceeded", now)
                self._terminate(session, run, now)
                return True

            approval = session.scalar(
                select(ApprovalRecord)
                .where(
                    ApprovalRecord.run_id == run.id,
                    ApprovalRecord.state == ApprovalState.PENDING,
                    ApprovalRecord.deadline <= now,
                )
                .order_by(ApprovalRecord.deadline, ApprovalRecord.id)
                .limit(1)
            )
            if approval is not None:
                self._record_approval(
                    session, run, approval, ApprovalState.EXPIRED, "system", None, now
                )
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
            ),
            lambda: not self._claim_active(claim),
            lambda round_number, payload: self.call_model(claim, round_number, payload),
        )

    def _claim_active(self, claim: Claim) -> bool:
        with self._sessions.begin() as session:
            return self._lock_claim(session, claim) is not None

    def call_model(self, claim: Claim, round_number: int, payload: dict) -> BaseModel:
        workflow = self.workflows[claim.workflow.name, claim.workflow.version]
        agent = workflow.nodes_by_id[claim.node_id].agent
        if agent is None:
            raise PermissionError("This step has no registered model")
        if not 1 <= round_number <= agent.max_rounds:
            raise ModelLimitExceeded("Agent round limit exceeded")
        encoded = _json(payload).encode()
        if len(encoded) > agent.max_input_bytes:
            raise ModelLimitExceeded("Agent input byte limit exceeded")
        canonical_payload = json.loads(encoded)
        digest = sha256(encoded).hexdigest()
        cached = self._reserve_model_call(
            claim,
            round_number,
            canonical_payload,
            digest,
            agent,
            workflow.max_model_dispatches,
        )
        if cached is not None:
            if cached.state == ModelCallState.REJECTED:
                raise ModelOutputRejected("Recorded model response was rejected")
            return agent.response_schema.model_validate_json(
                _json(cached.response), by_name=True
            )

        response = agent.provider.complete(
            ModelRequest(
                agent.model,
                agent.instructions,
                canonical_payload,
                agent.response_schema,
                agent.max_output_tokens,
            )
        )
        try:
            checkpoint = encode_checkpoint(agent.response_schema, response.output)
        except (ValueError, TypeError):
            checkpoint = None
        with self._sessions.begin() as session:
            current = self._lock_claim(session, claim)
            if current is None:
                raise LeaseLost(
                    "Cannot record a model response without a current lease"
                )
            run, step, now = current
            call = session.get(
                ModelCallRecord, (claim.run_id, claim.node_id, round_number)
            )
            call.state = (
                ModelCallState.SUCCEEDED
                if checkpoint is not None
                else ModelCallState.REJECTED
            )
            call.response = json.loads(checkpoint) if checkpoint is not None else None
            call.input_tokens = response.input_tokens
            call.output_tokens = response.output_tokens
            call.provider_reference = response.reference
            call.finished_at = now
            _event(
                session,
                run,
                f"model.{call.state.value.lower()}",
                now,
                step,
                claim.number,
            )
        if checkpoint is None:
            raise ModelOutputRejected(
                "Model response did not match the registered schema"
            )
        return agent.response_schema.model_validate_json(checkpoint, by_name=True)

    def _reserve_model_call(
        self,
        claim: Claim,
        round_number: int,
        payload: dict,
        digest: str,
        agent: AgentSpec,
        run_budget: int | None,
    ) -> ModelCallRecord | None:
        identity = (claim.run_id, claim.node_id, round_number)
        with self._sessions.begin() as session:
            current = self._lock_claim(session, claim)
            if current is None:
                raise LeaseLost("Cannot dispatch a model call without a current lease")
            run, step, now = current
            call = session.get(ModelCallRecord, identity)
            if call is not None:
                if call.request_digest != digest:
                    raise ValueError(
                        "Model round already belongs to a different request"
                    )
                if call.state != ModelCallState.DISPATCHING:
                    return call
                if (
                    call.last_attempt == claim.number
                    or call.dispatch_count >= agent.max_dispatches_per_round
                ):
                    raise ModelLimitExceeded("Model round dispatch limit exceeded")
            elif round_number > 1:
                prior = session.get(
                    ModelCallRecord, (claim.run_id, claim.node_id, round_number - 1)
                )
                if prior is None or prior.state != ModelCallState.SUCCEEDED:
                    raise ValueError("Model rounds must complete in order")
            if run_budget is not None:
                reserved = session.scalar(
                    select(
                        func.coalesce(func.sum(ModelCallRecord.dispatch_count), 0)
                    ).where(ModelCallRecord.run_id == run.id)
                )
                if reserved >= run_budget:
                    raise ModelLimitExceeded("Workflow model dispatch budget exceeded")
            if call is None:
                call = ModelCallRecord(
                    run_id=claim.run_id,
                    node_id=claim.node_id,
                    round_number=round_number,
                    provider=agent.provider_name,
                    model=agent.model,
                    prompt_version=agent.prompt_version,
                    request_digest=digest,
                    request=payload,
                    state=ModelCallState.DISPATCHING,
                    dispatch_count=0,
                )
                session.add(call)
            call.dispatch_count += 1
            call.last_attempt = claim.number
            _event(session, run, "model.dispatching", now, step, claim.number)
            return None

    def _require_approval(
        self,
        claim: Claim,
        tool: Tool,
        operation_id: str,
        arguments: object,
        digest: str,
    ) -> None:
        with self._sessions.begin() as session:
            current = self._lock_claim(session, claim)
            if current is None:
                raise LeaseLost("Cannot request approval without a current lease")
            run, step, now = current
            approval = session.get(ApprovalRecord, (run.id, step.node_id, operation_id))
            if approval is not None:
                if approval.action_digest != digest:
                    raise ToolConflict("Approval belongs to a different action")
                if approval.state != ApprovalState.APPROVED:
                    raise ApprovalConflict("Protected action has no valid approval")
                return
            session.add(
                ApprovalRecord(
                    run_id=run.id,
                    node_id=step.node_id,
                    operation_id=operation_id,
                    id=uuid4(),
                    tool_name=tool.name,
                    destination=tool.destination,
                    action_digest=digest,
                    arguments=arguments,
                    deadline=now + tool.approval_timeout,
                    state=ApprovalState.PENDING,
                )
            )
            step.state = StepState.WAITING_APPROVAL
            _clear_lease(step)
            attempt = session.get(AttemptRecord, (run.id, step.node_id, claim.number))
            attempt.state = AttemptState.WAITING_APPROVAL
            attempt.finished_at = now
            _event(session, run, "approval.requested", now, step, claim.number)
        raise ApprovalWait("The protected action is waiting for approval")

    def pending_approvals(self, *, limit: int = 100) -> tuple[ApprovalRecord, ...]:
        with self._sessions() as session:
            time = func.statement_timestamp()
            return tuple(
                session.scalars(
                    select(ApprovalRecord)
                    .join(RunRecord, RunRecord.id == ApprovalRecord.run_id)
                    .where(
                        ApprovalRecord.state == ApprovalState.PENDING,
                        ApprovalRecord.deadline > time,
                        RunRecord.state == RunState.ACTIVE,
                        or_(RunRecord.deadline.is_(None), RunRecord.deadline > time),
                    )
                    .order_by(ApprovalRecord.deadline, ApprovalRecord.id)
                    .limit(limit)
                )
            )

    def decide_approval(
        self,
        approval_id: UUID,
        action_digest: str,
        *,
        approve: bool,
        actor: str,
        reason: str | None = None,
    ) -> ApprovalRecord | None:
        if not actor:
            raise ValueError("Reviewer identity is required")
        decision = ApprovalState.APPROVED if approve else ApprovalState.REJECTED
        with self._sessions.begin() as session:
            run_id = session.scalar(
                select(ApprovalRecord.run_id).where(ApprovalRecord.id == approval_id)
            )
            if run_id is None:
                return None
            run = session.get(RunRecord, run_id, with_for_update=True)
            approval = session.scalar(
                select(ApprovalRecord).where(ApprovalRecord.id == approval_id)
            )
            if approval.action_digest != action_digest:
                raise ApprovalConflict("Action digest does not match the request")
            if approval.state in {ApprovalState.EXPIRED, ApprovalState.CANCELLED}:
                return approval
            if approval.state != ApprovalState.PENDING:
                if (approval.state, approval.actor, approval.reason) != (
                    decision,
                    actor,
                    reason,
                ):
                    raise ApprovalConflict("Approval already has a different decision")
                return approval
            now = _now(session)
            if run.deadline is not None and run.deadline <= now:
                _event(session, run, "run.deadline_exceeded", now)
                self._terminate(session, run, now)
            elif approval.deadline <= now:
                self._record_approval(
                    session, run, approval, ApprovalState.EXPIRED, "system", None, now
                )
            else:
                self._record_approval(
                    session, run, approval, decision, actor, reason, now
                )
            return approval

    def _record_approval(
        self,
        session: Session,
        run: RunRecord,
        approval: ApprovalRecord,
        decision: ApprovalState,
        actor: str,
        reason: str | None,
        now: datetime,
    ) -> None:
        approval.state = decision
        approval.actor = actor
        approval.reason = reason
        approval.decided_at = now
        step = session.get(StepRecord, (run.id, approval.node_id))
        _event(
            session,
            run,
            f"approval.{decision.value.lower()}",
            now,
            step,
            step.attempt_number,
        )
        if decision == ApprovalState.APPROVED:
            self._make_ready(session, run, step, now)
        else:
            self._terminate(session, run, now)

    def cancel(self, run_id: UUID) -> RunState | None:
        with self._sessions.begin() as session:
            run = session.get(RunRecord, run_id, with_for_update=True)
            if run is None:
                return None
            if run.state == RunState.ACTIVE:
                now = _now(session)
                if run.deadline is not None and run.deadline <= now:
                    _event(session, run, "run.deadline_exceeded", now)
                    self._terminate(session, run, now)
                else:
                    self._terminate(session, run, now, state=RunState.CANCELLED)
            return run.state

    def call_tool(
        self, claim: Claim, name: str, operation_id: str, arguments: object
    ) -> object:
        workflow = self.workflows[claim.workflow.name, claim.workflow.version]
        node = workflow.nodes_by_id[claim.node_id]
        if name not in node.tools:
            with self._sessions.begin() as session:
                current = self._lock_claim(session, claim)
                if current is None:
                    raise LeaseLost("Cannot request a tool without a current lease")
                run, step, now = current
                _event(session, run, "tool.denied", now, step, claim.number)
            raise PermissionError(f"Tool {name!r} is not allowed for this step")
        if not operation_id or len(operation_id) > 200:
            raise ValueError("Operation ID must contain 1 to 200 characters")
        tool = workflow.tools_by_name[name]
        encoded = encode_checkpoint(tool.input_schema, arguments)
        payload = json.loads(encoded)
        validated = tool.input_schema.model_validate_json(encoded, by_name=True)
        digest = _digest([tool.name, tool.version, payload])
        if tool.requires_approval:
            digest = _digest(
                [
                    workflow.name,
                    workflow.version,
                    tool.destination,
                    tool.name,
                    tool.version,
                    payload,
                ]
            )
            self._require_approval(claim, tool, operation_id, payload, digest)
        identity = (claim.run_id, claim.node_id, operation_id)
        key = _digest([str(claim.run_id), claim.node_id, operation_id])
        needs_reconciliation = False
        with self._sessions.begin() as session:
            current = self._lock_claim(session, claim)
            if current is None:
                raise LeaseLost("Cannot dispatch without a current lease")
            run, step, now = current
            if (
                not tool.requires_approval
                and session.get(ApprovalRecord, identity) is not None
            ):
                raise ToolConflict("Operation ID already belongs to a protected action")
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
        selected_route(workflow.nodes_by_id[claim.node_id], checkpoint)
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
            if all(
                step.state in {StepState.SUCCEEDED, StepState.SKIPPED}
                for step in steps.values()
            ):
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
        charged_attempts = number
        if retryable and policy is not None:
            charged_attempts -= session.scalar(
                select(func.count())
                .select_from(AttemptRecord)
                .where(
                    AttemptRecord.run_id == run.id,
                    AttemptRecord.node_id == step.node_id,
                    AttemptRecord.state == AttemptState.WAITING_APPROVAL,
                )
            )
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
        elif (
            retryable and policy is not None and charged_attempts < policy.max_attempts
        ):
            step.state = StepState.RETRY_WAIT
            step.next_attempt_at = now + max(
                retry_after, policy.delay(charged_attempts, self._jitter())
            )
            _event(session, run, "step.retry_scheduled", now, step, number)
        else:
            step.state = StepState.FAILED
            self._terminate(session, run, now)

    def _terminate(
        self,
        session: Session,
        run: RunRecord,
        now: datetime,
        *,
        state: RunState = RunState.FAILED,
    ) -> None:
        for approval in session.scalars(
            select(ApprovalRecord)
            .where(
                ApprovalRecord.run_id == run.id,
                ApprovalRecord.state == ApprovalState.PENDING,
            )
            .order_by(ApprovalRecord.node_id, ApprovalRecord.operation_id)
        ):
            approval.state = ApprovalState.CANCELLED
            approval.actor = "system"
            approval.decided_at = now
            step = session.get(StepRecord, (run.id, approval.node_id))
            _event(session, run, "approval.cancelled", now, step, step.attempt_number)
        for step in self._steps(session, run.id).values():
            if step.state in {StepState.SUCCEEDED, StepState.SKIPPED, StepState.FAILED}:
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
        run.state = state
        _event(session, run, f"run.{state.value.lower()}", now)

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
        completed = _completed_routes(workflow, steps)
        skipped = {
            node_id
            for node_id, step in steps.items()
            if step.state == StepState.SKIPPED
        }
        for node_id in workflow.order:
            step = steps[node_id]
            if step.state != StepState.PENDING:
                continue
            readiness, _ = select_parents(
                workflow.nodes_by_id[node_id], completed, skipped
            )
            if readiness == Readiness.READY:
                DurableRuntime._make_ready(session, run, step, now)
            elif readiness == Readiness.SKIP:
                step.state = StepState.SKIPPED
                skipped.add(node_id)
                _event(session, run, "step.skipped", now, step)

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
            approvals = session.scalars(
                select(ApprovalRecord)
                .where(ApprovalRecord.run_id == run_id)
                .order_by(ApprovalRecord.node_id, ApprovalRecord.operation_id)
            )
            model_calls = session.scalars(
                select(ModelCallRecord)
                .where(ModelCallRecord.run_id == run_id)
                .order_by(ModelCallRecord.node_id, ModelCallRecord.round_number)
            )
            return RunHistory(
                run,
                tuple(self._steps(session, run_id).values()),
                tuple(attempts),
                tuple(events),
                tuple(operations),
                tuple(approvals),
                tuple(model_calls),
            )
