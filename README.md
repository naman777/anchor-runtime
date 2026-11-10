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

## Conditional workflows

A condition node declares its allowed `routes`. Its handler returns a
`Route(selected="…")` model. A downstream node can add
`when=(("condition_node", "selected_route"),)` to require that route on a
direct dependency. The runtime records unselected nodes and their blocked
descendants as `SKIPPED`.

An ordinary join requires every parent to succeed. A join with
`join_selected=True` waits for every parent to either succeed or be skipped,
then receives the selected parent outputs in the order declared in `parents`.
If every input is skipped, the join is skipped too.

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

## Incident investigator

The `incident-investigation` workflow runs telemetry and change investigators
in parallel. Each investigator can use only its registered read-only tools.
The join verifies evidence from both branches and either escalates for human
review or proposes a ticket. The approval includes the exact ticket arguments
and evidence IDs.

With the database, ticket receiver, and API configured as above, start a case
and run one or more workers.

```sh
uv run python -m anchor_runtime.cli start-investigation errors checkout "Checkout errors increased"
uv run python -m anchor_runtime.cli worker
```

Use the reviewer endpoints to approve the proposed ticket. Try `latency` to
see a different tool path, or `missing`, `conflict`, and `injection` to see
escalation. The default workflow uses the deterministic provider. To enable
the optional OpenAI adapter, set `ANCHOR_OPENAI_MODEL` to a model that supports
Structured Outputs and supply `OPENAI_API_KEY` to the API and workers. Use
`GET /workflows` to find the separately versioned OpenAI workflow.

Anchor journals each model round and its tool results. On a retry it reuses
completed responses and records a new dispatch for a round whose response
was lost. Each investigator has a four-round limit and the run reserves at
most 12 dispatches across both investigators. Requests also have input size
and output token limits. Token counts reflect usage reported and recorded
by the provider. An interrupted dispatch may incur unobserved usage.

## Run console and traces

With the API running, open `http://127.0.0.1:8000/console`. Use an operator
token and run ID to view the graph, checkpoints, attempts, approvals, tool
operations, model usage, and ordered events. Use a reviewer token to inspect
the pending action's exact arguments and approve or reject it. The page uses
the same authenticated endpoints as the API.

Runs started through the API carry the trace context into worker attempts.
Worker logs include the run, node, attempt, and trace IDs. To export spans,
set `ANCHOR_OTLP_TRACES_ENDPOINT` in the API and workers to your collector's
OTLP HTTP traces endpoint, including its `/v1/traces` path.

The inspector omits raw model prompts and responses and redacts known
credential fields in checkpoints. Approval arguments remain visible to
authorized reviewers so they can verify the exact action. Keep credentials
out of action payloads and free-form text.

## Evaluation and benchmarks

Set `ANCHOR_TEST_DATABASE_URL` to a PostgreSQL database whose role can
create schemas. The harnesses create and clean up isolated schemas.

```sh
uv run python -m evals.run
uv run python -m benchmarks.run
uv run python -m evals.report --date YYYY-MM-DD
```

The [frozen corpus](evals/cases.jsonl) has 20 development and 40 held-out
cases. The default evaluation runs the held-out split three times with the
deterministic provider, comparing the parallel workflow with a single-agent
baseline. Use `--split development` for prompt and tool changes. A paid
evaluation is opt-in with `--provider openai --model MODEL` and
`OPENAI_API_KEY`. Supply `--usd-per-million INPUT OUTPUT --pricing-date YYYY-MM-DD`
to estimate known model charges from recorded usage.

The benchmark uses 1,000 preloaded runs per repetition at 1, 2, 4, and 8
worker processes. It measures the deterministic runtime separately from
agent execution and includes a worker-exit probe using the default lease.
Both commands save per-case or per-repetition data under `artifacts/`.
