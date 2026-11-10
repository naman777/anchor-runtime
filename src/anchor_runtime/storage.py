"""PostgreSQL records for the durable runtime."""

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, Enum, ForeignKey, ForeignKeyConstraint, Index, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class RunState(StrEnum):
    ACTIVE = "ACTIVE"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class StepState(StrEnum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    SUCCEEDED = "SUCCEEDED"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class AttemptState(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    CANCELLED = "CANCELLED"


class OperationState(StrEnum):
    DISPATCHING = "DISPATCHING"
    SUCCEEDED = "SUCCEEDED"
    NEEDS_RECONCILIATION = "NEEDS_RECONCILIATION"


class ApprovalState(StrEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"


class Base(DeclarativeBase):
    pass


class WorkflowVersion(Base):
    __tablename__ = "workflow_versions"

    name: Mapped[str] = mapped_column(primary_key=True)
    version: Mapped[str] = mapped_column(primary_key=True)
    definition: Mapped[dict] = mapped_column(JSONB)
    digest: Mapped[str]


class RunRecord(Base):
    __tablename__ = "runs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workflow_name", "workflow_version"],
            ["workflow_versions.name", "workflow_versions.version"],
        ),
        Index(
            "ix_runs_active",
            "created_at",
            "id",
            postgresql_where=text("state = 'ACTIVE'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    workflow_name: Mapped[str]
    workflow_version: Mapped[str]
    state: Mapped[RunState] = mapped_column(
        Enum(RunState, native_enum=False, create_constraint=True, name="run_state")
    )
    input: Mapped[Any] = mapped_column(JSONB, nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(unique=True)
    request_digest: Mapped[str]
    event_sequence: Mapped[int] = mapped_column(default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class StepRecord(Base):
    __tablename__ = "steps"
    __table_args__ = (
        Index("ix_steps_claimable", "run_id", "state", "lease_expires_at"),
    )

    run_id: Mapped[UUID] = mapped_column(ForeignKey("runs.id"), primary_key=True)
    node_id: Mapped[str] = mapped_column(primary_key=True)
    state: Mapped[StepState] = mapped_column(
        Enum(
            StepState,
            native_enum=False,
            create_constraint=True,
            name="step_state",
            length=32,
        )
    )
    output: Mapped[Any] = mapped_column(JSONB(none_as_null=True), nullable=True)
    attempt_number: Mapped[int] = mapped_column(default=0)
    lease_owner: Mapped[str | None]
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AttemptRecord(Base):
    __tablename__ = "attempts"
    __table_args__ = (
        ForeignKeyConstraint(["run_id", "node_id"], ["steps.run_id", "steps.node_id"]),
    )

    run_id: Mapped[UUID] = mapped_column(primary_key=True)
    node_id: Mapped[str] = mapped_column(primary_key=True)
    number: Mapped[int] = mapped_column(primary_key=True)
    owner: Mapped[str]
    state: Mapped[AttemptState] = mapped_column(
        Enum(
            AttemptState,
            native_enum=False,
            create_constraint=True,
            name="attempt_state",
            length=32,
        )
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_type: Mapped[str | None]


class EventRecord(Base):
    __tablename__ = "events"

    run_id: Mapped[UUID] = mapped_column(ForeignKey("runs.id"), primary_key=True)
    sequence: Mapped[int] = mapped_column(primary_key=True)
    type: Mapped[str]
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    node_id: Mapped[str | None]
    attempt_number: Mapped[int | None]


class ToolOperation(Base):
    __tablename__ = "tool_operations"
    __table_args__ = (
        ForeignKeyConstraint(["run_id", "node_id"], ["steps.run_id", "steps.node_id"]),
    )

    run_id: Mapped[UUID] = mapped_column(primary_key=True)
    node_id: Mapped[str] = mapped_column(primary_key=True)
    operation_id: Mapped[str] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(unique=True)
    tool_name: Mapped[str]
    idempotent: Mapped[bool]
    action_digest: Mapped[str]
    arguments: Mapped[Any] = mapped_column(JSONB, nullable=False)
    state: Mapped[OperationState] = mapped_column(
        Enum(
            OperationState,
            native_enum=False,
            create_constraint=True,
            name="operation_state",
        )
    )
    result: Mapped[Any] = mapped_column(JSONB(none_as_null=True), nullable=True)


class ApprovalRecord(Base):
    __tablename__ = "approvals"
    __table_args__ = (
        ForeignKeyConstraint(["run_id", "node_id"], ["steps.run_id", "steps.node_id"]),
        Index(
            "ix_approvals_pending",
            "deadline",
            "id",
            postgresql_where=text("state = 'PENDING'"),
        ),
    )

    run_id: Mapped[UUID] = mapped_column(primary_key=True)
    node_id: Mapped[str] = mapped_column(primary_key=True)
    operation_id: Mapped[str] = mapped_column(primary_key=True)
    id: Mapped[UUID] = mapped_column(unique=True)
    tool_name: Mapped[str]
    destination: Mapped[str]
    action_digest: Mapped[str]
    arguments: Mapped[Any] = mapped_column(JSONB, nullable=False)
    deadline: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    state: Mapped[ApprovalState] = mapped_column(
        Enum(
            ApprovalState,
            native_enum=False,
            create_constraint=True,
            name="approval_state",
        )
    )
    actor: Mapped[str | None]
    reason: Mapped[str | None]
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
