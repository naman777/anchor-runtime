# Anchor development progress

Updated 10 November 2026. See the [implementation plan](durable_agent_engine_plan.md)
for the full scope and the [README](README.md) for setup and demo commands.

## Completed

| Plan milestone | What shipped | Commit |
| --- | --- | --- |
| Week 1 · Workflow SDK | Typed inputs and outputs, DAG validation, versioned handler references, a sequential runner, and a deterministic incident example. | `8b61f62` |
| Weeks 2–3 · Durable execution | PostgreSQL migrations and records for versions, runs, steps, attempts, and events. Added idempotent run creation, atomic output checkpoints, concurrent worker claims, lease renewal, and recovery with stale-attempt rejection. The CLI starts runs, processes steps, and displays run history. | `fbb86d3` |
| Week 4 · Retries, deadlines, and tool effects | Persisted capped retry delays and deadlines, added timer processing, and implemented the tool-operation ledger with an independent SQLite ticket receiver. Unknown outcomes from non-idempotent writes require reconciliation. | `0270c34` |
| Week 5 · Approvals, API, and cancellation | Added operator and reviewer HTTP endpoints, approval tied to the exact tool action, database-enforced decision expiry, and cooperative cancellation. | `1872a58` |
| Week 6 · Fan-out, conditional branches, and joins | Independent steps run concurrently. Conditions select routes, unselected paths become `SKIPPED`, and a selected-parent join runs once with its inputs in declared order. A retry preserves committed sibling outputs. | `fbb86d3` and conditional scheduling milestone |

### Verification

- All 31 tests passed, including 20 integration tests against PostgreSQL 17.
  The integration suite covers restart and version pinning, worker death and
  stale completion, competing claims, rollback before commit, concurrent join
  scheduling, and failure propagation.
- A worker exited after the mock receiver committed a ticket. A replacement
  worker reused the operation key, returned the original ticket ID, and left
  exactly one ticket in the receiver's database. Retry timers, deadlines,
  mismatched operation arguments, and ambiguous writes are also covered.
- Approval tests cover restart while waiting, role enforcement, changed
  arguments, duplicate decisions, the expiry race, and cancellation before
  dispatch and while a handler is running.
- Conditional scheduling tests cover both selected routes, skip propagation,
  joins with no selected parents, and reuse of a completed sibling after a retry.
- Alembic upgrade, schema check, and downgrade passed. The incident CLI demo
  reached `SUCCEEDED` with two separate worker invocations.
- Ruff lint checks passed.

## Remaining

| Plan milestone | Work to do | Exit check |
| --- | --- | --- |
| Week 7 · Durable incident investigator | Checkpoint bounded model rounds and tool calls. Add a real provider, evidence validation, tool allowlists, and a workflow that escalates insufficient evidence or proposes a ticket for approval. | Cases follow different evidence-driven tool paths. Tests cover escalation, denied tools, and the approval requirement for ticket creation. |
| Week 8 · Inspection and observability | Build the run timeline and approval UI. Add trace correlation, model usage accounting, and redaction. | A reviewer can inspect a run from API request through approval and tool result. |
| Week 9 · Evaluation and benchmarks | Expand the fault suite, freeze the synthetic evaluation cases, compare the agent workflow with a baseline, and measure worker throughput and recovery latency. | Documented commands reproduce the evaluation and benchmark reports. |
| Week 10 · Deployment and demo | Add Docker Compose, CI with PostgreSQL, architecture decision records, a failure postmortem, and demo instructions and recording. | A clean checkout can run the sample with two workers and reproduce the crash and approval scenarios. |
