"""OpenAI Responses API adapter for Anchor model rounds."""

import json
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

from openai import APIConnectionError, APIStatusError, OpenAI

from anchor_runtime.models import ModelRequest, ModelResponse
from anchor_runtime.policy import RetryableError


def _retry_after(value: str | None) -> timedelta:
    if not value:
        return timedelta(0)
    try:
        if value.isdecimal():
            return timedelta(seconds=int(value))
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            date = date.replace(tzinfo=UTC)
        return max(date - datetime.now(UTC), timedelta(0))
    except (OverflowError, TypeError, ValueError):
        return timedelta(0)


class OpenAIModelProvider:
    def __init__(self, client: OpenAI | None = None) -> None:
        self.client = (
            client if client is not None else OpenAI(timeout=20, max_retries=0)
        )

    def complete(self, request: ModelRequest) -> ModelResponse:
        try:
            response = self.client.responses.parse(
                model=request.model,
                instructions=request.instructions,
                input=json.dumps(request.payload, separators=(",", ":")),
                text_format=request.response_schema,
                max_output_tokens=request.max_output_tokens,
                store=False,
            )
        except APIConnectionError as error:
            raise RetryableError("Model provider connection failed") from error
        except APIStatusError as error:
            if error.status_code in {408, 429, 500, 502, 503, 504}:
                raise RetryableError(
                    "Transient model provider response",
                    retry_after=_retry_after(error.response.headers.get("Retry-After")),
                ) from error
            raise
        usage = response.usage
        return ModelResponse(
            response.output_parsed if response.status == "completed" else None,
            input_tokens=usage.input_tokens if usage is not None else None,
            output_tokens=usage.output_tokens if usage is not None else None,
            reference=response.id,
        )
