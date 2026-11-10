"""Persist retry timers, deadlines, and tool operations."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0002_execution_policy"
down_revision = "0001_runtime"

STEP_STATES = "'PENDING', 'READY', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED'"


def upgrade():
    op.add_column("runs", sa.Column("deadline", sa.DateTime(timezone=True)))
    op.add_column("steps", sa.Column("deadline", sa.DateTime(timezone=True)))
    op.add_column("steps", sa.Column("next_attempt_at", sa.DateTime(timezone=True)))
    op.drop_constraint("step_state", "steps", type_="check")
    op.alter_column("steps", "state", type_=sa.String(32))
    op.create_check_constraint(
        "step_state", "steps", f"state IN ({STEP_STATES}, 'RETRY_WAIT')"
    )
    op.create_table(
        "tool_operations",
        sa.Column("run_id", sa.Uuid(), primary_key=True),
        sa.Column("node_id", sa.String(), primary_key=True),
        sa.Column("operation_id", sa.String(), primary_key=True),
        sa.Column("key", sa.String(), nullable=False, unique=True),
        sa.Column("tool_name", sa.String(), nullable=False),
        sa.Column("idempotent", sa.Boolean(), nullable=False),
        sa.Column("action_digest", sa.String(), nullable=False),
        sa.Column("arguments", JSONB(), nullable=False),
        sa.Column(
            "state",
            sa.Enum(
                "DISPATCHING",
                "SUCCEEDED",
                "NEEDS_RECONCILIATION",
                native_enum=False,
                create_constraint=True,
                name="operation_state",
            ),
            nullable=False,
        ),
        sa.Column("result", JSONB()),
        sa.ForeignKeyConstraint(
            ["run_id", "node_id"], ["steps.run_id", "steps.node_id"]
        ),
    )


def downgrade():
    op.drop_table("tool_operations")
    op.drop_constraint("step_state", "steps", type_="check")
    op.execute("UPDATE steps SET state = 'READY' WHERE state = 'RETRY_WAIT'")
    op.alter_column("steps", "state", type_=sa.String(9))
    op.create_check_constraint("step_state", "steps", f"state IN ({STEP_STATES})")
    op.drop_column("steps", "next_attempt_at")
    op.drop_column("steps", "deadline")
    op.drop_column("runs", "deadline")
