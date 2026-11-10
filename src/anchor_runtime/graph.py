"""Definitions for trusted, versioned workflow code."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from graphlib import CycleError, TopologicalSorter
from types import MappingProxyType
from typing import TypeVar

from pydantic import BaseModel

from anchor_runtime.policy import RetryPolicy
from anchor_runtime.tools import Tool, ToolCaller

Model = TypeVar("Model", bound=BaseModel)


@dataclass(frozen=True, slots=True)
class StepContext:
    run_input: BaseModel
    parents: Mapping[str, BaseModel]
    _call_tool: ToolCaller | None = field(default=None, repr=False)
    _should_stop: Callable[[], bool] | None = field(default=None, repr=False)

    def call_tool(self, name: str, operation_id: str, arguments: object) -> object:
        if self._call_tool is None:
            raise RuntimeError("Tool calls require a durable worker")
        return self._call_tool(name, operation_id, arguments)

    def should_stop(self) -> bool:
        """Check whether cancellation, a deadline, or lease loss ended this attempt."""
        return self._should_stop is not None and self._should_stop()

    def input_as(self, schema: type[Model]) -> Model:
        if not isinstance(self.run_input, schema):
            raise TypeError(f"Expected workflow input of type {schema.__name__}")
        return self.run_input

    def parent_as(self, node_id: str, schema: type[Model]) -> Model:
        value = self.parents[node_id]
        if not isinstance(value, schema):
            raise TypeError(
                f"Expected output from {node_id!r} of type {schema.__name__}"
            )
        return value


@dataclass(frozen=True, slots=True)
class Handler:
    name: str
    version: str
    output_schema: type[BaseModel]
    execute: Callable[[StepContext], object]

    @property
    def key(self) -> tuple[str, str]:
        return self.name, self.version


@dataclass(frozen=True, slots=True)
class Node:
    id: str
    handler: str
    handler_version: str
    parents: tuple[str, ...] = ()
    retry: RetryPolicy | None = None
    timeout: timedelta | None = None
    tools: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "parents", tuple(self.parents))
        object.__setattr__(self, "tools", tuple(self.tools))
        if not all((self.id, self.handler, self.handler_version)):
            raise ValueError("Node ID, handler name, and handler version are required")
        if len(set(self.parents)) != len(self.parents):
            raise ValueError(f"Duplicate dependency on node {self.id!r}")
        if self.timeout is not None and self.timeout <= timedelta(0):
            raise ValueError("Step timeout must be positive")
        if len(set(self.tools)) != len(self.tools):
            raise ValueError(f"Duplicate tool on node {self.id!r}")

    @property
    def handler_key(self) -> tuple[str, str]:
        return self.handler, self.handler_version


@dataclass(frozen=True, slots=True)
class Workflow:
    name: str
    version: str
    input_schema: type[BaseModel]
    nodes: tuple[Node, ...]
    handlers: tuple[Handler, ...]
    timeout: timedelta | None = None
    tools: tuple[Tool, ...] = ()
    order: tuple[str, ...] = field(init=False)
    nodes_by_id: Mapping[str, Node] = field(init=False, repr=False)
    handlers_by_key: Mapping[tuple[str, str], Handler] = field(init=False, repr=False)
    tools_by_name: Mapping[str, Tool] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "nodes", tuple(self.nodes))
        object.__setattr__(self, "handlers", tuple(self.handlers))
        object.__setattr__(self, "tools", tuple(self.tools))
        if self.timeout is not None and self.timeout <= timedelta(0):
            raise ValueError("Workflow timeout must be positive")
        if not self.name or not self.version or not self.nodes:
            raise ValueError(
                "Workflow name, version, and at least one node are required"
            )

        nodes = {node.id: node for node in self.nodes}
        handlers = {handler.key: handler for handler in self.handlers}
        tools = {tool.name: tool for tool in self.tools}
        if len(nodes) != len(self.nodes):
            raise ValueError("Duplicate node ID")
        if len(handlers) != len(self.handlers):
            raise ValueError("Duplicate handler name and version")
        if len(tools) != len(self.tools):
            raise ValueError("Duplicate tool name")

        dependencies = {}
        for node_id, node in sorted(nodes.items()):
            missing = set(node.parents) - nodes.keys()
            if missing:
                raise ValueError(
                    f"Unknown dependencies for {node_id!r}: {sorted(missing)}"
                )
            if node.handler_key not in handlers:
                raise ValueError(
                    f"Missing handler for {node_id!r}: {node.handler_key!r}"
                )
            if unknown_tools := set(node.tools) - tools.keys():
                raise ValueError(
                    f"Unknown tools for {node_id!r}: {sorted(unknown_tools)}"
                )
            dependencies[node_id] = node.parents

        try:
            order = tuple(TopologicalSorter(dependencies).static_order())
        except CycleError as error:
            raise ValueError(f"Workflow contains a cycle: {error.args[1]}") from error

        object.__setattr__(self, "order", order)
        object.__setattr__(self, "nodes_by_id", MappingProxyType(nodes))
        object.__setattr__(self, "handlers_by_key", MappingProxyType(handlers))
        object.__setattr__(self, "tools_by_name", MappingProxyType(tools))
