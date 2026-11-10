"""Dependency selection shared by both workflow runners."""

from collections.abc import Mapping, Set
from enum import Enum, auto

from anchor_runtime.graph import Node


class Readiness(Enum):
    WAIT = auto()
    READY = auto()
    SKIP = auto()


def selected_route(node: Node, output: object) -> str | None:
    if not node.routes:
        return None
    selected = output.get("selected") if isinstance(output, dict) else None
    if selected not in node.routes:
        raise ValueError(f"Condition {node.id!r} selected an undeclared route")
    return selected


def select_parents(
    node: Node,
    completed: Mapping[str, str | None],
    skipped: Set[str],
) -> tuple[Readiness, tuple[str, ...]]:
    gates = dict(node.when)
    selected = tuple(
        parent
        for parent in node.parents
        if parent in completed
        and (parent not in gates or completed[parent] == gates[parent])
    )
    unresolved = sum(
        parent not in completed and parent not in skipped for parent in node.parents
    )
    omitted = len(node.parents) - len(selected) - unresolved
    if omitted and (not node.join_selected or omitted == len(node.parents)):
        return Readiness.SKIP, ()
    if unresolved:
        return Readiness.WAIT, ()
    return Readiness.READY, selected
