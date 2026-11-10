"""Checkpoint model requests, responses and reported usage."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0005_model_calls"
down_revision = "0004_branches"


def upgrade():
    op.create_table(
        "model_calls",
        sa.Column("run_id", sa.Uuid(), primary_key=True),
        sa.Column("node_id", sa.String(), primary_key=True),
        sa.Column("round_number", sa.Integer(), primary_key=True),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("model", sa.String(), nullable=False),
        sa.Column("prompt_version", sa.String(), nullable=False),
        sa.Column("request_digest", sa.String(), nullable=False),
        sa.Column("request", JSONB(), nullable=False),
        sa.Column("response", JSONB()),
        sa.Column(
            "state",
            sa.Enum(
                "DISPATCHING",
                "SUCCEEDED",
                "REJECTED",
                native_enum=False,
                create_constraint=True,
                name="model_call_state",
            ),
            nullable=False,
        ),
        sa.Column("dispatch_count", sa.Integer(), nullable=False),
        sa.Column("last_attempt", sa.Integer(), nullable=False),
        sa.Column("input_tokens", sa.Integer()),
        sa.Column("output_tokens", sa.Integer()),
        sa.Column("provider_reference", sa.String()),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(
            ["run_id", "node_id"], ["steps.run_id", "steps.node_id"]
        ),
    )


def downgrade():
    op.execute(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM model_calls) THEN "
        "RAISE EXCEPTION 'Model history must be retained to downgrade safely'; "
        "END IF; END $$"
    )
    op.drop_table("model_calls")
