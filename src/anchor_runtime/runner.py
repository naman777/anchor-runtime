"""Sequential reference runner and shared handler invocation."""

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from pydantic import BaseModel

from anchor_runtime.branches import Readiness, select_parents, selected_route
from anchor_runtime.graph import StepContext, Workflow
from anchor_runtime.models import ModelCaller
from anchor_runtime.tools import ToolCaller


@dataclass(frozen=True, slots=True)
class Event:
    type: str
    node_id: str | None = None


@dataclass(frozen=True, slots=True)
class Result:
    outputs: dict[str, str]
    events: tuple[Event, ...]


def execute_node(
    workflow: Workflow,
    node_id: str,
    run_input: str,
    parent_outputs: Mapping[str, str],
    call_tool: ToolCaller | None = None,
    should_stop: Callable[[], bool] | None = None,
    call_model: ModelCaller | None = None,
) -> str:
    node = workflow.nodes_by_id[node_id]
    parents = {}
    for parent_id in node.parents:
        if node.join_selected and parent_id not in parent_outputs:
            continue
        parent = workflow.nodes_by_id[parent_id]
        schema = workflow.handlers_by_key[parent.handler_key].output_schema
        parents[parent_id] = schema.model_validate_json(
            parent_outputs[parent_id], by_name=True
        )

    handler = workflow.handlers_by_key[node.handler_key]
    context = StepContext(
        run_input=workflow.input_schema.model_validate_json(run_input, by_name=True),
        parents=MappingProxyType(parents),
        _call_tool=call_tool,
        _should_stop=should_stop,
        _call_model=call_model,
    )
    output = encode_checkpoint(handler.output_schema, handler.execute(context))
    if node.routes:
        selected_route(node, json.loads(output))
    return output


def run(workflow: Workflow, payload: object) -> Result:
    run_input = encode_input(workflow, payload)
    outputs: dict[str, str] = {}
    completed: dict[str, str | None] = {}
    skipped: set[str] = set()
    events = [Event("run.started")]
    for node_id in workflow.order:
        node = workflow.nodes_by_id[node_id]
        readiness, parents = select_parents(node, completed, skipped)
        if readiness == Readiness.SKIP:
            skipped.add(node_id)
            events.append(Event("step.skipped", node_id))
            continue
        events.append(Event("step.started", node_id))
        output = execute_node(
            workflow,
            node_id,
            run_input,
            {parent: outputs[parent] for parent in parents},
        )
        outputs[node_id] = output
        completed[node_id] = (
            selected_route(node, json.loads(output)) if node.routes else None
        )
        events.append(Event("step.succeeded", node_id))
    events.append(Event("run.succeeded"))
    return Result(outputs, tuple(events))


def encode_input(workflow: Workflow, payload: object) -> str:
    return encode_checkpoint(workflow.input_schema, payload)


def encode_checkpoint(schema: type[BaseModel], value: object) -> str:
    if isinstance(value, schema):
        # Revalidate model instances even if they were mutated or constructed directly.
        value = value.model_dump(mode="python", round_trip=True, by_alias=False)
    validated = schema.model_validate(value, by_name=True)
    encoded = validated.model_dump_json(round_trip=True, by_alias=False)
    schema.model_validate_json(encoded, by_name=True)
    return encoded
