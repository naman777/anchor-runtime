# Anchor

Anchor is a Python workflow engine for running agent work across process
failures and human decisions. See the [implementation plan](durable_agent_engine_plan.md)
for the target architecture and evaluation, and [development progress](PROGRESS.md)
for completed milestones and remaining work.

The SDK validates versioned DAGs, workflow inputs, and handler outputs. Each
handler receives its direct parents' outputs in the declared dependency order.
The sample runs in memory or through workers backed by PostgreSQL.

## Quickstart

With Python 3.12 and uv installed, run the deterministic incident example and
the test suite.

```sh
uv sync --locked
uv run python -m anchor_runtime.example
uv run pytest
uv run ruff check
```

The example uses a fake provider and needs no API key.

## PostgreSQL worker demo

Use an empty PostgreSQL database. Set `ANCHOR_DATABASE_URL` to a SQLAlchemy
connection URL such as `postgresql+psycopg://anchor:password@localhost:5432/anchor`
in each terminal. In PowerShell, set it with `$env:ANCHOR_DATABASE_URL = '…'`.

```sh
uv run alembic upgrade head
uv run python -m anchor_runtime.cli start checkout 0.08 --key demo-1
uv run python -m anchor_runtime.cli worker
```

Run a second worker in another terminal to share the queue. Use the UUID
printed by `start` to inspect the checkpoints and event sequence.

```sh
uv run python -m anchor_runtime.cli inspect RUN_ID
```

Repeating `start` with the same key and validated input returns the original
run. To inspect a restart between checkpoints, invoke the worker with
`--once` twice. The first invocation commits the evidence, and the second
uses that evidence to produce the assessment.

For the PostgreSQL tests, set `ANCHOR_TEST_DATABASE_URL` to a PostgreSQL
database where the test user can create schemas, then run `uv run pytest`.
Each test applies the migrations in its own temporary schema.

## Runtime behavior

- Workers claim eligible steps using a short PostgreSQL transaction with
  `FOR UPDATE SKIP LOCKED` on the run row. Handlers execute outside that
  transaction. Completion atomically records the attempt and checkpoint,
  readies dependent steps, and appends events.
- A worker renews its 30-second lease every 10 seconds while running a
  handler. An available worker can reclaim an expired lease. The attempt
  number fences heartbeat and completion, including when the worker name
  is reused. An interrupted handler can execute again, so handlers that
  write to external systems require idempotency.
- Workers execute the workflow versions they load at startup. Keep older
  versions available until their runs complete, and bump the handler and
  workflow versions when handler behavior changes. The graph fingerprint
  covers topology, handler identities, and schemas. Code identity is
  managed through explicit handler versions.
- A handler error fails the run and cancels its unfinished steps. Error
  classes appear in history. Nodes with a retry policy can retry explicitly
  transient failures and timed-out attempts. The database stores the sampled
  retry time and enforces step and workflow deadlines on durable writes.
- The tool ledger records the validated action before dispatch. A replacement
  attempt reuses the same key. The receiver must implement deduplication for
  external writes that can repeat. An ambiguous write without deduplication
  support is recorded as `NEEDS_RECONCILIATION` and requires manual review.

## Ticket recovery demo

Set `ANCHOR_TICKET_DB` to a writable SQLite file path and apply the migrations.
The mock receiver uses this separate store to commit tickets independently of
PostgreSQL. Share the same path across workers.

```sh
uv run python -m anchor_runtime.cli start-ticket checkout "Elevated errors"
uv run python -m anchor_runtime.cli worker --once
uv run python -m anchor_runtime.cli inspect RUN_ID
```

The PostgreSQL test below starts a worker process that exits after SQLite
commits the ticket and before PostgreSQL records the tool result. A replacement
worker reclaims the attempt and recovers the same ticket.

```sh
uv run pytest -k process_death_after_ticket_creation
```

Workers process due timers when they poll. Run `uv run python -m anchor_runtime.cli
scheduler` to advance timers while workers are busy. Deadlines fence checkpoint
and tool writes. Synchronous handler code and a receiver's already-dispatched
work can continue until they return.

## Approval API demo

Apply the migrations and set `ANCHOR_DATABASE_URL` and `ANCHOR_TICKET_DB` for
the API and workers. Configure an `ANCHOR_OPERATOR_TOKEN` and set
`ANCHOR_REVIEWER_TOKENS` to a JSON object mapping reviewer names to distinct
bearer tokens. These credentials are for a trusted single-project deployment.

```sh
uv run uvicorn anchor_runtime.api:app_from_env --factory
```

Use the API documentation at `http://127.0.0.1:8000/docs`.

1. As the operator, POST to `/workflows/approved-ticket/1/runs` with an
   `input` object containing `service` and `title`. Run a worker to create
   the pending approval and release its lease.
2. As the reviewer, GET `/approvals`. Review the destination, arguments, and
   decision deadline. POST the listed `action_digest`, `approve`, and
   optional `reason` to `/approvals/{approval_id}/decision`.
3. After approval, a worker resumes the step and dispatches the exact action.
   As the operator, GET `/runs/{run_id}` to see the result and history.

The approval deadline bounds the time to make a decision. Rejection and expiry
end the run and record the outcome. Cancellation through
`POST /runs/{run_id}/cancel` closes pending approvals and fences further
checkpoints and dispatch reservations. Handlers can check
`context.should_stop()` for cooperative cancellation.
