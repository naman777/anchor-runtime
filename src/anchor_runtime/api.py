"""Authenticated HTTP entry points for runs and approval decisions."""

import json
import os
from datetime import datetime
from pathlib import Path
from secrets import compare_digest
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import create_engine

from anchor_runtime.durable import ApprovalConflict, DurableRuntime, RequestConflict
from anchor_runtime.example import demo_workflows
from anchor_runtime.inspection import run_view
from anchor_runtime.storage import ApprovalState, RunState
from anchor_runtime.telemetry import span, tag_run

bearer = HTTPBearer()
Credentials = Annotated[HTTPAuthorizationCredentials, Depends(bearer)]
CONSOLE = Path(__file__).with_name("console")


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

    @app.middleware("http")
    async def trace_request(request: Request, call_next):
        with span(
            "anchor.http", attributes={"http.request.method": request.method}
        ) as request_span:
            response = await call_next(request)
            route = getattr(request.scope.get("route"), "path", None)
            if route:
                request_span.set_attribute("http.route", route)
            request_span.set_attribute(
                "http.response.status_code", response.status_code
            )
            response.headers["Cache-Control"] = "no-store"
            return response

    @app.get("/console", include_in_schema=False)
    def console():
        return FileResponse(
            CONSOLE / "index.html",
            headers={
                "Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
                "Referrer-Policy": "no-referrer",
            },
        )

    @app.get("/console.js", include_in_schema=False)
    def console_script():
        return FileResponse(CONSOLE / "console.js", media_type="text/javascript")

    @app.get("/console.css", include_in_schema=False)
    def console_style():
        return FileResponse(CONSOLE / "console.css", media_type="text/css")

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
        tag_run(run_id)
        return {"id": run_id}

    @app.get("/runs/{run_id}", dependencies=[Depends(operator)])
    def inspect(run_id: UUID):
        history = runtime.inspect(run_id)
        if history is None:
            raise HTTPException(404, "Run not found")
        tag_run(run_id)
        return run_view(history)

    @app.post("/runs/{run_id}/cancel", dependencies=[Depends(operator)])
    def cancel(run_id: UUID):
        state = runtime.cancel(run_id)
        if state is None:
            raise HTTPException(404, "Run not found")
        tag_run(run_id)
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
        tag_run(approval.run_id)
        if approval.state in {ApprovalState.EXPIRED, ApprovalState.CANCELLED}:
            raise HTTPException(409, f"Approval is {approval.state.value.lower()}")
        return approval

    return app


def app_from_env() -> FastAPI:
    engine = create_engine(
        os.environ["ANCHOR_DATABASE_URL"], pool_pre_ping=True, hide_parameters=True
    )
    runtime = DurableRuntime(engine, demo_workflows(os.environ.get("ANCHOR_TICKET_DB")))
    return create_app(
        runtime,
        operator_token=os.environ["ANCHOR_OPERATOR_TOKEN"],
        reviewers=json.loads(os.environ["ANCHOR_REVIEWER_TOKENS"]),
    )
