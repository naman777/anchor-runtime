"""Authenticated HTTP entry points for runs and approval decisions."""

import json
import os
from datetime import datetime
from secrets import compare_digest
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import create_engine

from anchor_runtime.durable import ApprovalConflict, DurableRuntime, RequestConflict
from anchor_runtime.example import demo_workflows
from anchor_runtime.storage import ApprovalState, RunState

bearer = HTTPBearer()
Credentials = Annotated[HTTPAuthorizationCredentials, Depends(bearer)]


class StartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    input: Any
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)


class DecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    action_digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    approve: bool = Field(strict=True)
    reason: str | None = Field(default=None, max_length=2000)


class ApprovalView(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    run_id: UUID
    node_id: str
    operation_id: str
    tool_name: str
    destination: str
    action_digest: str
    arguments: Any
    deadline: datetime
    state: ApprovalState
    actor: str | None
    reason: str | None
    decided_at: datetime | None


def create_app(
    runtime: DurableRuntime, *, operator_token: str, reviewers: dict[str, str]
) -> FastAPI:
    reviewer_tokens = {actor: token.encode() for actor, token in reviewers.items()}
    operator_secret = operator_token.encode()
    secrets = [operator_secret, *reviewer_tokens.values()]
    if (
        not all(secrets)
        or not all(reviewer_tokens)
        or len(set(secrets)) != len(secrets)
    ):
        raise ValueError(
            "Configure distinct nonempty tokens and nonempty reviewer identities"
        )
    app = FastAPI(title="Anchor Runtime")

    def operator(credentials: Credentials) -> None:
        if not compare_digest(credentials.credentials.encode(), operator_secret):
            raise HTTPException(403, "Operator credentials required")

    def reviewer(credentials: Credentials) -> str:
        presented = credentials.credentials.encode()
        for actor, token in reviewer_tokens.items():
            if compare_digest(presented, token):
                return actor
        raise HTTPException(403, "Reviewer credentials required")

    @app.get("/workflows", dependencies=[Depends(operator)])
    def workflows():
        return [
            {"name": name, "version": version}
            for name, version in sorted(runtime.workflows)
        ]

    @app.post(
        "/workflows/{name}/{version}/runs",
        status_code=202,
        dependencies=[Depends(operator)],
    )
    def start(name: str, version: str, request: StartRequest):
        if (name, version) not in runtime.workflows:
            raise HTTPException(404, "Workflow version unavailable")
        try:
            run_id = runtime.start(
                name, version, request.input, idempotency_key=request.idempotency_key
            )
        except ValidationError as error:
            raise HTTPException(
                422, "Input does not match the workflow schema"
            ) from error
        except RequestConflict as error:
            raise HTTPException(409, str(error)) from error
        return {"id": run_id}

    @app.get("/runs/{run_id}", dependencies=[Depends(operator)])
    def inspect(run_id: UUID):
        history = runtime.inspect(run_id)
        if history is None:
            raise HTTPException(404, "Run not found")
        return {
            "id": history.run.id,
            "state": history.run.state,
            "workflow": history.run.workflow_name,
            "version": history.run.workflow_version,
            "steps": [
                {"id": step.node_id, "state": step.state, "output": step.output}
                for step in history.steps
            ],
            "events": [
                {
                    "sequence": event.sequence,
                    "type": event.type,
                    "at": event.at,
                    "node_id": event.node_id,
                    "attempt": event.attempt_number,
                }
                for event in history.events
            ],
            "operations": [
                {
                    "node_id": operation.node_id,
                    "id": operation.operation_id,
                    "state": operation.state,
                }
                for operation in history.operations
            ],
        }

    @app.post("/runs/{run_id}/cancel", dependencies=[Depends(operator)])
    def cancel(run_id: UUID):
        state = runtime.cancel(run_id)
        if state is None:
            raise HTTPException(404, "Run not found")
        if state != RunState.CANCELLED:
            raise HTTPException(409, f"Run is already {state.value.lower()}")
        return {"id": run_id, "state": state}

    @app.get(
        "/approvals",
        response_model=list[ApprovalView],
        dependencies=[Depends(reviewer)],
    )
    def pending(limit: Annotated[int, Query(ge=1, le=100)] = 100):
        return runtime.pending_approvals(limit=limit)

    @app.post("/approvals/{approval_id}/decision", response_model=ApprovalView)
    def decide(
        approval_id: UUID,
        request: DecisionRequest,
        actor: Annotated[str, Depends(reviewer)],
    ):
        try:
            approval = runtime.decide_approval(
                approval_id,
                request.action_digest,
                approve=request.approve,
                actor=actor,
                reason=request.reason,
            )
        except ApprovalConflict as error:
            raise HTTPException(409, str(error)) from error
        if approval is None:
            raise HTTPException(404, "Approval not found")
        if approval.state in {ApprovalState.EXPIRED, ApprovalState.CANCELLED}:
            raise HTTPException(409, f"Approval is {approval.state.value.lower()}")
        return approval

    return app


def app_from_env() -> FastAPI:
    engine = create_engine(os.environ["ANCHOR_DATABASE_URL"], pool_pre_ping=True)
    runtime = DurableRuntime(engine, demo_workflows(os.environ.get("ANCHOR_TICKET_DB")))
    return create_app(
        runtime,
        operator_token=os.environ["ANCHOR_OPERATOR_TOKEN"],
        reviewers=json.loads(os.environ["ANCHOR_REVIEWER_TOKENS"]),
    )
