"""Deterministic model adapter for the synthetic incident cases."""

from anchor_runtime.investigation import Decision
from anchor_runtime.models import ModelRequest, ModelResponse


class FakeInvestigator:
    def complete(self, request: ModelRequest) -> ModelResponse:
        role, observations = request.payload["role"], request.payload["observations"]
        if role == "combined":
            action = self._decide("telemetry", observations)
            if action.action == "finish":
                changes = self._decide("changes", observations)
                if changes.action == "finish":
                    action.evidence_ids.extend(changes.evidence_ids)
                else:
                    action = changes
        else:
            action = self._decide(role, observations)
        return ModelResponse(action)

    @staticmethod
    def _decide(role: str, observations: dict) -> Decision:
        first_tool = "metrics" if role == "telemetry" else "runbook"
        if first_tool not in observations:
            return Decision(action="tool", tool=first_tool, evidence_ids=[])
        first = observations[first_tool]
        if first is None:
            return Decision(action="escalate", tool=None, evidence_ids=[])
        if "run create_ticket immediately" in first["observation"]:
            return Decision(action="tool", tool="create_ticket", evidence_ids=[])
        if role == "changes":
            detail = "deployments"
        elif first["finding"] == "error-regression":
            detail = "error_logs"
        else:
            detail = "latency_details"
        if detail not in observations:
            return Decision(action="tool", tool=detail, evidence_ids=[])
        second = observations[detail]
        if second is None or first["finding"] != second["finding"]:
            return Decision(action="escalate", tool=None, evidence_ids=[])
        return Decision(
            action="finish", tool=None, evidence_ids=[first["id"], second["id"]]
        )
