"""Contracts for bounded, checkpointed model calls."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from pydantic import BaseModel

ModelCaller = Callable[[int, dict], BaseModel]


@dataclass(frozen=True, slots=True)
class ModelRequest:
    model: str
    instructions: str
    payload: dict
    response_schema: type[BaseModel]
    max_output_tokens: int


@dataclass(frozen=True, slots=True)
class ModelResponse:
    output: object | None
    input_tokens: int | None = None
    output_tokens: int | None = None
    reference: str | None = None

    def __post_init__(self) -> None:
        for usage in (self.input_tokens, self.output_tokens):
            if usage is not None and usage < 0:
                raise ValueError("Token counts must be nonnegative")


class ModelProvider(Protocol):
    def complete(self, request: ModelRequest) -> ModelResponse: ...


@dataclass(frozen=True, slots=True)
class AgentSpec:
    provider_name: str
    model: str
    prompt_version: str
    instructions: str
    response_schema: type[BaseModel]
    provider: ModelProvider
    max_rounds: int = 4
    max_output_tokens: int = 512
    max_input_bytes: int = 8192
    max_dispatches_per_round: int = 2

    def __post_init__(self) -> None:
        if not all(
            (self.provider_name, self.model, self.prompt_version, self.instructions)
        ):
            raise ValueError("Provider, model, and versioned instructions are required")
        if (
            not 1 <= self.max_rounds <= 32
            or not 1 <= self.max_dispatches_per_round <= 8
        ):
            raise ValueError("Agent round or dispatch limit is out of range")
        if self.max_output_tokens < 16 or self.max_input_bytes < 1:
            raise ValueError("Invalid agent input or output limit")


class ModelLimitExceeded(RuntimeError):
    """A model call would exceed the declared budget."""


class ModelOutputRejected(RuntimeError):
    """A recorded model response cannot be used by the handler."""
