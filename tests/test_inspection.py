import json
import logging

import pytest
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from anchor_runtime import telemetry
from anchor_runtime.api import create_app
from anchor_runtime.durable import DurableRuntime
from anchor_runtime.example import incident_workflow
from anchor_runtime.inspection import redact
from anchor_runtime.tickets import TicketService, ticket_workflow
from anchor_runtime.worker import work_once


def test_redacts_nested_credential_fields():
    payload = {
        "safe": "visible",
        "parent": [
            {"API-Key": "secret", "input_tokens": 21},
            {"accessToken": "private"},
        ],
        "headers": {"Authorization": "Bearer value", "Set-Cookie": "cookie"},
    }
    assert redact(payload) == {
        "safe": "visible",
        "parent": [
            {"API-Key": "[redacted]", "input_tokens": 21},
            {"accessToken": "[redacted]"},
        ],
        "headers": {"Authorization": "[redacted]", "Set-Cookie": "[redacted]"},
    }


@pytest.mark.postgres
def test_console_api_correlates_request_worker_approval_and_tool(
    engine, tmp_path, monkeypatch, caplog
):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(
        telemetry, "_tracer", lambda: provider.get_tracer("anchor-test")
    )
    workflow = ticket_workflow(
        TicketService(tmp_path / "receiver.sqlite"), protected=True
    )
    runtime = DurableRuntime(engine, [workflow])
    app = create_app(
        runtime, operator_token="operator-token", reviewers={"oncall": "reviewer-token"}
    )
    operator = {"Authorization": "Bearer operator-token"}
    reviewer = {"Authorization": "Bearer reviewer-token"}

    with (
        TestClient(app) as client,
        caplog.at_level(logging.INFO, logger="anchor_runtime.worker"),
    ):
        page = client.get("/console")
        assert page.status_code == 200
        assert "frame-ancestors 'none'" in page.headers["content-security-policy"]
        assert "Approval inbox" in page.text
        assert client.get("/console.js").status_code == 200
        assert client.get("/console.css").status_code == 200
        run_id = client.post(
            "/workflows/approved-ticket/1/runs",
            headers=operator,
            json={"input": {"service": "checkout", "title": "Unique payload marker"}},
        ).json()["id"]
        assert work_once(runtime, "first")
        (pending,) = client.get("/approvals", headers=reviewer).json()
        assert pending["arguments"]["title"] == "Unique payload marker"
        assert (
            client.post(
                f"/approvals/{pending['id']}/decision",
                headers=reviewer,
                json={"action_digest": pending["action_digest"], "approve": True},
            ).status_code
            == 200
        )
        assert work_once(runtime, "second")
        response = client.get(f"/runs/{run_id}", headers=operator)
        assert response.headers["cache-control"] == "no-store"
        view = response.json()
        assert view["state"] == "SUCCEEDED"
        assert view["graph"] == [{"id": "ticket", "parents": []}]
        assert len(view["trace_id"]) == 32
        assert {attempt["trace_id"] for attempt in view["attempts"]} == {
            view["trace_id"]
        }
        assert [
            event["subject"]
            for event in view["events"]
            if event["type"].startswith("approval.")
        ] == [
            pending["id"],
            pending["id"],
        ]
        assert [
            event["subject"]
            for event in view["events"]
            if event["type"].startswith("tool.")
        ] == [
            "incident-ticket",
            "incident-ticket",
        ]
        assert "Unique payload marker" not in response.text

    spans = [
        span
        for span in exporter.get_finished_spans()
        if f"{span.context.trace_id:032x}" == view["trace_id"]
    ]
    assert {span.name for span in spans} == {
        "anchor.http",
        "anchor.step",
        "anchor.tool",
    }
    root = next(span for span in spans if span.name == "anchor.http")
    assert all(
        span.parent.span_id == root.context.span_id
        for span in spans
        if span.name == "anchor.step"
    )
    logs = [
        json.loads(record.message)
        for record in caplog.records
        if record.name == "anchor_runtime.worker"
    ]
    assert {log["trace_id"] for log in logs} == {view["trace_id"]}
    assert "Unique payload marker" not in str(spans)
    assert "operator-token" not in caplog.text
    provider.shutdown()


@pytest.mark.postgres
def test_run_output_redaction_does_not_modify_checkpoint(engine):
    from dataclasses import replace

    from pydantic import BaseModel

    from anchor_runtime.example import FakeProvider

    class Output(BaseModel):
        access_token: str

    sample = incident_workflow(FakeProvider())
    handler = replace(
        sample.handlers[0],
        output_schema=Output,
        execute=lambda _: {"access_token": "private"},
    )
    workflow = replace(sample, nodes=(sample.nodes[0],), handlers=(handler,))
    runtime = DurableRuntime(engine, [workflow])
    run_id = runtime.start("incident", "1", {"service": "checkout", "error_rate": 0.08})
    assert work_once(runtime, "worker")
    with TestClient(create_app(runtime, operator_token="op", reviewers={})) as client:
        view = client.get(
            f"/runs/{run_id}", headers={"Authorization": "Bearer op"}
        ).json()
    assert view["steps"][0]["output"] == {"access_token": "[redacted]"}
    assert runtime.inspect(run_id).steps[0].output == {"access_token": "private"}
