"""Represent skipped branches in durable step state."""

from alembic import op

revision = "0004_branches"
down_revision = "0003_approvals"

STEP_STATES = "'PENDING', 'READY', 'RUNNING', 'RETRY_WAIT', 'WAITING_APPROVAL', 'SUCCEEDED', 'FAILED', 'CANCELLED'"


def upgrade():
    op.drop_constraint("step_state", "steps", type_="check")
    op.create_check_constraint(
        "step_state", "steps", f"state IN ({STEP_STATES}, 'SKIPPED')"
    )


def downgrade():
    op.execute(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM steps WHERE state = 'SKIPPED') THEN "
        "RAISE EXCEPTION 'Cannot downgrade while skipped branch checkpoints exist'; "
        "END IF; END $$"
    )
    op.drop_constraint("step_state", "steps", type_="check")
    op.create_check_constraint("step_state", "steps", f"state IN ({STEP_STATES})")
