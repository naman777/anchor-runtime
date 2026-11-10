# Anchor development progress

Updated 10 November 2026. See the [implementation plan](durable_agent_engine_plan.md)
for the full scope and the [README](README.md) for setup and demo commands.

## Completed

| Plan milestone | What shipped | Commit |
| --- | --- | --- |
| Week 1 · Workflow SDK | Typed inputs and outputs, DAG validation, versioned handler references, a sequential runner, and a deterministic incident example. | `8b61f62` |
| Weeks 2–3 · Durable execution | PostgreSQL migrations and records for versions, runs, steps, attempts, and events. Added idempotent run creation, atomic output checkpoints, concurrent worker claims, lease renewal, and recovery with stale-attempt rejection. The CLI starts runs, processes steps, and displays run history. | `fbb86d3` |
| Week 4 · Retries, deadlines, and tool effects | Persisted capped retry delays and deadlines, added timer processing, and implemented the tool-operation ledger with an independent SQLite ticket receiver. Unknown outcomes from non-idempotent writes require reconciliation. | Execution policy milestone |
| Week 6 · Fixed fan-out and joins | Independent steps can run concurrently. A join becomes ready once all parents succeed and receives their outputs in the declared order. | `fbb86d3` |

### Verification

- All 21 tests passed, including 12 integration tests against PostgreSQL 17.
  The integration suite covers restart and version pinning, worker death and
  stale completion, competing claims, rollback before commit, concurrent join
  scheduling, and failure propagation.
- A worker exited after the mock receiver committed a ticket. A replacement
  worker reused the operation key, returned the original ticket ID, and left
  exactly one ticket in the receiver's database. Retry timers, deadlines,
  mismatched operation arguments, and ambiguous writes are also covered.
- Alembic upgrade, schema check, and downgrade passed. The incident CLI demo
  reached `SUCCEEDED` with two separate worker invocations.
- Ruff lint checks passed.

## Remaining

| Plan milestone | Work to do | Exit check |
| --- | --- | --- |
| Week 5 · Approvals, API, and cancellation | Build the FastAPI run and approval endpoints, authenticated reviewer decisions bound to the exact action, an expiry scanner, and cancellation. | Approval survives restart. An approval racing expiry produces one legal outcome. Cancellation prevents downstream work and rejects late completion. |
| Week 6 · Conditional branches | Add condition nodes, skip propagation, and joins that wait for selected parents. Verify that a retry reuses completed sibling outputs. | Both branch paths terminate, and the join runs once after the selected parents succeed. |
| Week 7 · Durable incident investigator | Checkpoint bounded model rounds and tool calls. Add a real provider, evidence validation, tool allowlists, and a workflow that escalates insufficient evidence or proposes a ticket for approval. | Cases follow different evidence-driven tool paths. Tests cover escalation, denied tools, and the approval requirement for ticket creation. |
| Week 8 · Inspection and observability | Build the run timeline and approval UI. Add trace correlation, model usage accounting, and redaction. | A reviewer can inspect a run from API request through approval and tool result. |
| Week 9 · Evaluation and benchmarks | Expand the fault suite, freeze the synthetic evaluation cases, compare the agent workflow with a baseline, and measure worker throughput and recovery latency. | Documented commands reproduce the evaluation and benchmark reports. |
| Week 10 · Deployment and demo | Add Docker Compose, CI with PostgreSQL, architecture decision records, a failure postmortem, and demo instructions and recording. | A clean checkout can run the sample with two workers and reproduce the crash and approval scenarios. |
