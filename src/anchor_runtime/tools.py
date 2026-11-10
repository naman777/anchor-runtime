"""Trusted tool adapters and their dispatch contracts."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

ToolCaller = Callable[[str, str, object], Any]


@dataclass(frozen=True, slots=True)
class Tool:
    name: str
    version: str
    input_schema: type[BaseModel]
    dispatch: Callable[[str, BaseModel], Any]
    idempotent: bool = False
    requires_approval: bool = False

    def __post_init__(self) -> None:
        if not self.name or not self.version:
            raise ValueError("Tool name and version are required")


class ToolConflict(ValueError):
    """The logical operation already has a different action."""


class ReconciliationRequired(RuntimeError):
    """A dispatched write has an unknown outcome and cannot safely repeat."""


class LeaseLost(RuntimeError):
    """The calling attempt can no longer perform durable operations."""
