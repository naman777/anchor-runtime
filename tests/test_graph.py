import pytest
from pydantic import BaseModel, Field, ValidationError

from anchor_runtime.example import Assessment, FakeProvider, incident_workflow
from anchor_runtime.graph import Handler, Node, Route, Workflow
from anchor_runtime.runner import Event, run


@pytest.mark.parametrize(
    ("nodes", "message"),
    [
        ((Node("a", "collect", "2"),), "Missing handler"),
        ((Node("a", "collect", "1", ("b",)),), "Unknown dependencies"),
        ((Node("a", "collect", "1"), Node("a", "collect", "1")), "Duplicate node"),
        ((Node("a", "collect", "1", ("a",)),), "cycle"),
        (
            (Node("a", "collect", "1", ("b",)), Node("b", "collect", "1", ("a",))),
            "cycle",
        ),
    ],
)
def test_rejects_invalid_graph(nodes, message):
    sample = incident_workflow(FakeProvider())
    with pytest.raises(ValueError, match=message):
        Workflow("invalid", "1", sample.input_schema, nodes, sample.handlers)


def test_runs_typed_workflow_in_dependency_order():
    result = run(
        incident_workflow(FakeProvider()), {"service": "checkout", "error_rate": 0.1}
    )
    assert Assessment.model_validate_json(result.outputs["assessment"]) == Assessment(
        service="checkout", severity="elevated"
    )
    assert result.events == (
        Event("run.started"),
        Event("step.started", "evidence"),
        Event("step.succeeded", "evidence"),
        Event("step.started", "assessment"),
        Event("step.succeeded", "assessment"),
        Event("run.succeeded"),
    )


def test_rejects_invalid_input():
    with pytest.raises(ValidationError):
        run(incident_workflow(FakeProvider()), {"service": "checkout", "error_rate": 2})


def test_rejects_invalid_handler_output():
    class InvalidProvider:
        def assess(self, evidence):
            return Assessment.model_construct(
                service=evidence.service, severity="critical"
            )

    with pytest.raises(ValidationError):
        run(
            incident_workflow(InvalidProvider()),
            {"service": "checkout", "error_rate": 0.1},
        )


def test_checkpoints_use_field_names_when_validation_and_serialization_aliases_differ():
    class Input(BaseModel):
        service: str = Field(
            validation_alias="from_user", serialization_alias="for_display"
        )

    def collect(context):
        return context.input_as(Input)

    def summarize(context):
        return context.parent_as("collect", Input)

    workflow = Workflow(
        "aliases",
        "1",
        Input,
        (
            Node("collect", "collect", "1"),
            Node("summary", "summarize", "1", ("collect",)),
        ),
        (
            Handler("collect", "1", Input, collect),
            Handler("summarize", "1", Input, summarize),
        ),
    )
    assert (
        run(workflow, {"from_user": "checkout"}).outputs["summary"]
        == '{"service":"checkout"}'
    )


def test_condition_requires_a_route_schema():
    sample = incident_workflow(FakeProvider())
    with pytest.raises(ValueError, match="must return a Route model"):
        Workflow(
            "invalid",
            "1",
            sample.input_schema,
            (Node("pick", "collect", "1", routes=("one",)),),
            sample.handlers,
        )


def test_rejects_unknown_route_dependency_and_undeclared_selection():
    sample = incident_workflow(FakeProvider())
    condition = Node("pick", "pick", "1", routes=("one",))
    handlers = (
        *sample.handlers,
        Handler("pick", "1", Route, lambda _: Route(selected="unknown")),
    )
    with pytest.raises(ValueError, match="Invalid route"):
        Workflow(
            "invalid",
            "1",
            sample.input_schema,
            (
                condition,
                Node("collect", "collect", "1", ("pick",), when=(("pick", "two"),)),
            ),
            handlers,
        )
    workflow = Workflow(
        "invalid-result", "1", sample.input_schema, (condition,), handlers
    )
    with pytest.raises(ValueError, match="undeclared route"):
        run(workflow, {"service": "checkout", "error_rate": 0.1})
