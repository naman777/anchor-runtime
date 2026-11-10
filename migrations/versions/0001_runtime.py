"""Create durable workflow records."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0001_runtime"
down_revision = None


def upgrade():
    op.create_table(
        "workflow_versions",
        sa.Column("name", sa.String(), primary_key=True),
        sa.Column("version", sa.String(), primary_key=True),
        sa.Column("definition", JSONB(), nullable=False),
        sa.Column("digest", sa.String(), nullable=False),
    )
    op.create_table(
        "runs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("workflow_name", sa.String(), nullable=False),
        sa.Column("workflow_version", sa.String(), nullable=False),
        sa.Column(
            "state",
            sa.Enum(
                "ACTIVE",
                "SUCCEEDED",
                "FAILED",
                native_enum=False,
                create_constraint=True,
                name="run_state",
            ),
            nullable=False,
        ),
        sa.Column("input", JSONB(), nullable=False),
        sa.Column("idempotency_key", sa.String(), unique=True),
        sa.Column("request_digest", sa.String(), nullable=False),
        sa.Column("event_sequence", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["workflow_name", "workflow_version"],
            ["workflow_versions.name", "workflow_versions.version"],
        ),
    )
    op.create_index(
        "ix_runs_active",
        "runs",
        ["created_at", "id"],
        postgresql_where=sa.text("state = 'ACTIVE'"),
    )
    op.create_table(
        "steps",
        sa.Column("run_id", sa.Uuid(), sa.ForeignKey("runs.id"), primary_key=True),
        sa.Column("node_id", sa.String(), primary_key=True),
        sa.Column(
            "state",
            sa.Enum(
                "PENDING",
                "READY",
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "CANCELLED",
                native_enum=False,
                create_constraint=True,
                name="step_state",
            ),
            nullable=False,
        ),
        sa.Column("output", JSONB()),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("lease_owner", sa.String()),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
    )
    op.create_index(
        "ix_steps_claimable", "steps", ["run_id", "state", "lease_expires_at"]
    )
    op.create_table(
        "attempts",
        sa.Column("run_id", sa.Uuid(), primary_key=True),
        sa.Column("node_id", sa.String(), primary_key=True),
        sa.Column("number", sa.Integer(), primary_key=True),
        sa.Column("owner", sa.String(), nullable=False),
        sa.Column(
            "state",
            sa.Enum(
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "LEASE_EXPIRED",
                "CANCELLED",
                native_enum=False,
                create_constraint=True,
                name="attempt_state",
            ),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("error_type", sa.String()),
        sa.ForeignKeyConstraint(
            ["run_id", "node_id"], ["steps.run_id", "steps.node_id"]
        ),
    )
    op.create_table(
        "events",
        sa.Column("run_id", sa.Uuid(), sa.ForeignKey("runs.id"), primary_key=True),
        sa.Column("sequence", sa.Integer(), primary_key=True),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("node_id", sa.String()),
        sa.Column("attempt_number", sa.Integer()),
    )


def downgrade():
    op.drop_table("events")
    op.drop_table("attempts")
    op.drop_table("steps")
    op.drop_table("runs")
    op.drop_table("workflow_versions")
