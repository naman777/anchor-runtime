"""Sequential reference runner and shared handler invocation."""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from pydantic import BaseModel

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
        parents[parent_id] = schema.model_validate_json(
            parent_outputs[parent_id], by_name=True
        )

    handler = workflow.handlers_by_key[node.handler_key]
    context = StepContext(
        run_input=workflow.input_schema.model_validate_json(run_input, by_name=True),
        parents=MappingProxyType(parents),
    )
    return _encode_checkpoint(handler.output_schema, handler.execute(context))


def run(workflow: Workflow, payload: object) -> Result:
    run_input = encode_input(workflow, payload)
    outputs: dict[str, str] = {}
    events = [Event("run.started")]
    for node_id in workflow.order:
        events.append(Event("step.started", node_id))
        outputs[node_id] = execute_node(workflow, node_id, run_input, outputs)
        events.append(Event("step.succeeded", node_id))
    events.append(Event("run.succeeded"))
    return Result(outputs, tuple(events))


def encode_input(workflow: Workflow, payload: object) -> str:
    return _encode_checkpoint(workflow.input_schema, payload)


def _encode_checkpoint(schema: type[BaseModel], value: object) -> str:
    if isinstance(value, schema):
        # Revalidate model instances even if they were mutated or constructed directly.
        value = value.model_dump(mode="python", round_trip=True, by_alias=False)
    validated = schema.model_validate(value, by_name=True)
    encoded = validated.model_dump_json(round_trip=True, by_alias=False)
    schema.model_validate_json(encoded, by_name=True)
    return encoded
