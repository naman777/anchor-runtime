"""A deterministic incident workflow for local development."""

import json
from dataclasses import dataclass
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from anchor_runtime.graph import Handler, Node, StepContext, Workflow
from anchor_runtime.runner import run
from anchor_runtime.tickets import TicketService, ticket_workflow


class Incident(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service: str = Field(min_length=1)
    error_rate: float = Field(ge=0, le=1)


class Evidence(BaseModel):
    service: str
    error_rate: float


class Assessment(BaseModel):
    service: str
    severity: Literal["elevated", "normal"]


class AssessmentProvider(Protocol):
    def assess(self, evidence: Evidence) -> Assessment: ...


@dataclass(frozen=True, slots=True)
class FakeProvider:
    def assess(self, evidence: Evidence) -> Assessment:
        severity = "elevated" if evidence.error_rate >= 0.05 else "normal"
        return Assessment(service=evidence.service, severity=severity)


def collect(context: StepContext) -> Evidence:
    incident = context.input_as(Incident)
    return Evidence(service=incident.service, error_rate=incident.error_rate)


def incident_workflow(provider: AssessmentProvider) -> Workflow:
    def assess(context: StepContext) -> Assessment:
        return provider.assess(context.parent_as("evidence", Evidence))

    return Workflow(
        name="incident",
        version="1",
        input_schema=Incident,
        nodes=(
            Node("evidence", "collect", "1"),
            Node("assessment", "assess", "1", ("evidence",)),
        ),
        handlers=(
            Handler("collect", "1", Evidence, collect),
            Handler("assess", "1", Assessment, assess),
        ),
    )


def main() -> None:
    result = run(
        incident_workflow(FakeProvider()), {"service": "checkout", "error_rate": 0.08}
    )
    for event in result.events:
        print(event.type, event.node_id or "")
    print(json.dumps(json.loads(result.outputs["assessment"]), indent=2))


def demo_workflows(ticket_db: str | None = None) -> list[Workflow]:
    workflows = [incident_workflow(FakeProvider())]
    if ticket_db:
        service = TicketService(ticket_db)
        workflows.extend(
            [ticket_workflow(service), ticket_workflow(service, protected=True)]
        )
    return workflows


if __name__ == "__main__":
    main()
