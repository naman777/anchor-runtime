"""Public run views that omit model payloads and redact credential fields."""

import re
from typing import Any

from anchor_runtime.durable import RunHistory
from anchor_runtime.storage import ModelCallState
from anchor_runtime.telemetry import trace_id

SECRET_KEYS = {
    "apikey",
    "authorization",
    "cookie",
    "credential",
    "credentials",
    "password",
    "privatekey",
    "secret",
    "setcookie",
    "token",
}


def _secret_field(key: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", key.lower())
    return normalized in SECRET_KEYS or normalized.endswith(
        ("password", "secret", "token", "apikey")
    )


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "[redacted]" if _secret_field(key) else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def run_view(history: RunHistory) -> dict:
    calls = history.model_calls
    input_counts = [
        call.input_tokens for call in calls if call.input_tokens is not None
    ]
    output_counts = [
        call.output_tokens for call in calls if call.output_tokens is not None
    ]
    dispatches = sum(call.dispatch_count for call in calls)
    return {
        "id": history.run.id,
        "state": history.run.state,
        "workflow": history.run.workflow_name,
        "version": history.run.workflow_version,
        "trace_id": trace_id(history.run.traceparent),
        "steps": [
            {"id": step.node_id, "state": step.state, "output": redact(step.output)}
            for step in history.steps
        ],
        "graph": history.graph,
        "events": [
            {
                "sequence": event.sequence,
                "type": event.type,
                "at": event.at,
                "node_id": event.node_id,
                "attempt": event.attempt_number,
                "subject": event.subject,
            }
            for event in history.events
        ],
        "attempts": [
            {
                "node_id": attempt.node_id,
                "number": attempt.number,
                "state": attempt.state,
                "started_at": attempt.started_at,
                "finished_at": attempt.finished_at,
                "error_type": attempt.error_type,
                "trace_id": trace_id(attempt.traceparent),
            }
            for attempt in history.attempts
        ],
        "approvals": [
            {
                "id": approval.id,
                "node_id": approval.node_id,
                "operation_id": approval.operation_id,
                "tool_name": approval.tool_name,
                "state": approval.state,
                "deadline": approval.deadline,
                "decided_at": approval.decided_at,
            }
            for approval in history.approvals
        ],
        "operations": [
            {
                "node_id": operation.node_id,
                "id": operation.operation_id,
                "name": operation.tool_name,
                "state": operation.state,
            }
            for operation in history.operations
        ],
        "model_calls": [
            {
                "node_id": call.node_id,
                "round": call.round_number,
                "provider": call.provider,
                "model": call.model,
                "prompt_version": call.prompt_version,
                "state": call.state,
                "dispatches": call.dispatch_count,
                "input_tokens": call.input_tokens,
                "output_tokens": call.output_tokens,
            }
            for call in calls
        ],
        "usage": {
            "reported_input_tokens": sum(input_counts) if input_counts else None,
            "reported_output_tokens": sum(output_counts) if output_counts else None,
            "dispatches": dispatches,
            "uncheckpointed_dispatches": dispatches
            - sum(call.state != ModelCallState.DISPATCHING for call in calls),
        },
    }
