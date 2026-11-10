# Anchor

Anchor is a Python workflow engine for running agent work across process
failures and human decisions. See the [implementation plan](durable_agent_engine_plan.md)
for the target architecture and evaluation.

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
  classes appear in history. This milestone supports fixed fan-out and
  all-parent joins. Persisted retries, conditional branches, approvals,
  workflow deadlines, and the external tool ledger are upcoming milestones.
