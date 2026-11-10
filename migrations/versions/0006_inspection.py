"""Persist trace propagation and event correlation."""

import sqlalchemy as sa
from alembic import op

revision = "0006_inspection"
down_revision = "0005_model_calls"


def upgrade():
    op.add_column("runs", sa.Column("traceparent", sa.String()))
    op.add_column("attempts", sa.Column("traceparent", sa.String()))
    op.add_column("events", sa.Column("subject", sa.String()))


def downgrade():
    op.drop_column("events", "subject")
    op.drop_column("attempts", "traceparent")
    op.drop_column("runs", "traceparent")
