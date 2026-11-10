"""A mock ticket receiver with its own durable idempotency store."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from anchor_runtime.graph import Handler, Node, StepContext, Workflow
from anchor_runtime.policy import RetryPolicy
from anchor_runtime.tools import Tool, ToolConflict


class TicketRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    service: str = Field(min_length=1)
    title: str = Field(min_length=1)


class Ticket(BaseModel):
    id: str


class TicketService:
    def __init__(self, path: str | Path) -> None:
        self.path = path
        with closing(sqlite3.connect(self.path)) as connection, connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS tickets ("
                "operation_key TEXT PRIMARY KEY, ticket_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL)"
            )

    def create(self, key: str, request: BaseModel) -> dict[str, str]:
        request = TicketRequest.model_validate(request)
        payload = json.dumps(request.model_dump(mode="json"), sort_keys=True)
        with closing(sqlite3.connect(self.path)) as connection, connection:
            connection.execute(
                "INSERT INTO tickets VALUES (?, ?, ?) ON CONFLICT(operation_key) DO NOTHING",
                (key, str(uuid4()), payload),
            )
            ticket_id, recorded_payload = connection.execute(
                "SELECT ticket_id, payload FROM tickets WHERE operation_key = ?", (key,)
            ).fetchone()
            if recorded_payload != payload:
                raise ToolConflict("Ticket key already belongs to a different request")
            return {"id": ticket_id}


def ticket_workflow(service: TicketService, *, protected: bool = False) -> Workflow:
    def create_ticket(context: StepContext) -> Ticket:
        request = context.input_as(TicketRequest)
        result = context.call_tool("create_ticket", "incident-ticket", request)
        return Ticket.model_validate(result)

    return Workflow(
        "approved-ticket" if protected else "ticket-demo",
        "1",
        TicketRequest,
        (Node("ticket", "ticket", "1", retry=RetryPolicy(), tools=("create_ticket",)),),
        (Handler("ticket", "1", Ticket, create_ticket),),
        tools=(
            Tool(
                "create_ticket",
                "1",
                TicketRequest,
                service.create,
                idempotent=True,
                requires_approval=protected,
                destination="Mock incident ticket queue",
            ),
        ),
    )
