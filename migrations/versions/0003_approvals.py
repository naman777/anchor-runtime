"""Persist approval gates and cancellation."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0003_approvals"
down_revision = "0002_execution_policy"

RUN_STATES = "'ACTIVE', 'SUCCEEDED', 'FAILED'"
STEP_STATES = (
    "'PENDING', 'READY', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED', 'FAILED', 'CANCELLED'"
)
ATTEMPT_STATES = "'RUNNING', 'SUCCEEDED', 'FAILED', 'LEASE_EXPIRED', 'CANCELLED'"


def upgrade():
    op.drop_constraint("run_state", "runs", type_="check")
    op.create_check_constraint(
        "run_state", "runs", f"state IN ({RUN_STATES}, 'CANCELLED')"
    )
    op.drop_constraint("step_state", "steps", type_="check")
    op.create_check_constraint(
        "step_state", "steps", f"state IN ({STEP_STATES}, 'WAITING_APPROVAL')"
    )
    op.drop_constraint("attempt_state", "attempts", type_="check")
    op.alter_column("attempts", "state", type_=sa.String(32))
    op.create_check_constraint(
        "attempt_state",
        "attempts",
        f"state IN ({ATTEMPT_STATES}, 'WAITING_APPROVAL')",
    )
    op.create_table(
        "approvals",
        sa.Column("run_id", sa.Uuid(), primary_key=True),
        sa.Column("node_id", sa.String(), primary_key=True),
        sa.Column("operation_id", sa.String(), primary_key=True),
        sa.Column("id", sa.Uuid(), nullable=False, unique=True),
        sa.Column("tool_name", sa.String(), nullable=False),
        sa.Column("destination", sa.String(), nullable=False),
        sa.Column("action_digest", sa.String(), nullable=False),
        sa.Column("arguments", JSONB(), nullable=False),
        sa.Column("deadline", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "state",
            sa.Enum(
                "PENDING",
                "APPROVED",
                "REJECTED",
                "EXPIRED",
                "CANCELLED",
                native_enum=False,
                create_constraint=True,
                name="approval_state",
            ),
            nullable=False,
        ),
        sa.Column("actor", sa.String()),
        sa.Column("reason", sa.String()),
        sa.Column("decided_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(
            ["run_id", "node_id"], ["steps.run_id", "steps.node_id"]
        ),
    )
    op.create_index(
        "ix_approvals_pending",
        "approvals",
        ["deadline", "id"],
        postgresql_where=sa.text("state = 'PENDING'"),
    )


def downgrade():
    op.execute(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM approvals) THEN "
        "RAISE EXCEPTION 'Approval history must be retained to downgrade safely'; "
        "END IF; END $$"
    )
    op.drop_table("approvals")
    op.drop_constraint("attempt_state", "attempts", type_="check")
    op.alter_column("attempts", "state", type_=sa.String(13))
    op.create_check_constraint(
        "attempt_state", "attempts", f"state IN ({ATTEMPT_STATES})"
    )
    op.drop_constraint("step_state", "steps", type_="check")
    op.create_check_constraint("step_state", "steps", f"state IN ({STEP_STATES})")
    op.drop_constraint("run_state", "runs", type_="check")
    op.execute("UPDATE runs SET state = 'FAILED' WHERE state = 'CANCELLED'")
    op.create_check_constraint("run_state", "runs", f"state IN ({RUN_STATES})")
