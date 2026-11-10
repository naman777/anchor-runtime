"""Sequential reference runner and shared handler invocation."""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from anchor_runtime.graph import StepContext, Workflow


@dataclass(frozen=True, slots=True)
class Event:
    type: str
    node_id: str | None = None


@dataclass(frozen=True, slots=True)
class Result:
    outputs: dict[str, str]
    events: tuple[Event, ...]


def execute_node(
    workflow: Workflow, node_id: str, run_input: str, parent_outputs: Mapping[str, str]
) -> str:
    node = workflow.nodes_by_id[node_id]
    parents = {}
    for parent_id in node.parents:
        parent = workflow.nodes_by_id[parent_id]
        schema = workflow.handlers_by_key[parent.handler_key].output_schema
        parents[parent_id] = schema.model_validate_json(parent_outputs[parent_id])

    handler = workflow.handlers_by_key[node.handler_key]
    context = StepContext(
        run_input=workflow.input_schema.model_validate_json(run_input),
        parents=MappingProxyType(parents),
    )
    output = handler.execute(context)
    if isinstance(output, handler.output_schema):
        # Revalidate model instances even if a handler mutated or constructed one directly.
        output = output.model_dump(mode="python", round_trip=True, by_alias=True)
    validated = handler.output_schema.model_validate(output)
    return validated.model_dump_json(round_trip=True, by_alias=True)


def run(workflow: Workflow, payload: object) -> Result:
    validated = workflow.input_schema.model_validate(payload)
    run_input = validated.model_dump_json(round_trip=True, by_alias=True)
    outputs: dict[str, str] = {}
    events = [Event("run.started")]
    for node_id in workflow.order:
        events.append(Event("step.started", node_id))
        outputs[node_id] = execute_node(workflow, node_id, run_input, outputs)
        events.append(Event("step.succeeded", node_id))
    events.append(Event("run.succeeded"))
    return Result(outputs, tuple(events))
