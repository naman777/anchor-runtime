import json
from datetime import timedelta

import httpx
import pytest
from openai import OpenAI

from anchor_runtime.investigation import Decision
from anchor_runtime.models import ModelRequest
from anchor_runtime.openai_provider import OpenAIModelProvider
from anchor_runtime.policy import RetryableError

REQUEST = ModelRequest(
    "test-model", "Test instructions", {"role": "test"}, Decision, 128
)


def test_responses_adapter_parses_structured_output_and_reported_usage():
    action = Decision(action="finish", tool=None, evidence_ids=["case/metrics"])

    def respond(request):
        assert request.url.path == "/v1/responses"
        body = json.loads(request.content)
        assert body["max_output_tokens"] == 128
        assert body["store"] is False
        assert body["text"]["format"]["type"] == "json_schema"
        return httpx.Response(
            200,
            json={
                "id": "resp_test",
                "object": "response",
                "created_at": 1,
                "status": "completed",
                "model": "test-model",
                "output": [
                    {
                        "id": "msg_test",
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [
                            {
                                "type": "output_text",
                                "text": action.model_dump_json(),
                                "annotations": [],
                            }
                        ],
                    }
                ],
                "usage": {"input_tokens": 21, "output_tokens": 12, "total_tokens": 33},
            },
        )

    with OpenAI(
        api_key="test",
        max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(respond)),
    ) as client:
        result = OpenAIModelProvider(client).complete(REQUEST)
    assert result.output == action
    assert (result.input_tokens, result.output_tokens, result.reference) == (
        21,
        12,
        "resp_test",
    )


def test_responses_adapter_classifies_retry_after():
    transport = httpx.MockTransport(
        lambda _: httpx.Response(
            429,
            headers={"Retry-After": "9"},
            json={"error": {"message": "Rate limited"}},
        )
    )
    with OpenAI(
        api_key="test", max_retries=0, http_client=httpx.Client(transport=transport)
    ) as client:
        with pytest.raises(RetryableError) as caught:
            OpenAIModelProvider(client).complete(REQUEST)
    assert caught.value.retry_after == timedelta(seconds=9)
