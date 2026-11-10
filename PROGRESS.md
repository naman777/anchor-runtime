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
| Week 6 · Fan-out, conditional branches, and joins | Independent steps run concurrently. Conditions select routes, unselected paths become `SKIPPED`, and a selected-parent join runs once with its inputs in declared order. A retry preserves committed sibling outputs. | `fbb86d3`, `0b24afe` |
| Week 7 · Durable incident investigator | Checkpointed bounded model rounds and tool results, added the deterministic and OpenAI adapters, and built parallel investigators that validate evidence before escalation or approved ticket creation. | `803cfba` |
| Week 8 · Inspection and observability | Added an authenticated run and approval console, correlated events and OpenTelemetry spans, structured worker logs, reported usage summaries, and credential-field redaction. | `127f3ac` |
| Week 9 · Evaluation and benchmarks | Froze 60 synthetic cases, added a single-agent baseline and benchmark harness, and published the [measurement report](docs/measurements.md). Expanded fault coverage for database disconnects and concurrent approval replay. | `4a8307d` |

### Verification

- All 48 tests passed, including 33 integration tests against PostgreSQL 17.
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
- The incident investigator exercises distinct error and latency tool paths,
  escalation for missing and conflicting evidence, denial of a tool request
  prompted by malicious evidence, and exact-action approval. Recovery tests
  verify reuse of completed rounds and atomic budget reservation. The OpenAI
  adapter was tested with a mocked HTTP transport.
- A headless Chrome smoke test loaded a pending incident run, reviewed its
  evidence references, approved the exact ticket action, and displayed the
  completed run. Integration tests also check trace propagation, event
  correlation, usage accounting, and redaction.
- Each workflow passed 120/120 held-out deterministic evaluations across three
  repetitions, with zero unsafe ticket dispatches. All 12,000 benchmark runs
  completed. Four workers had the highest median throughput at 88.3 committed
  steps per second, and a three-claim recovery probe measured 30.29 seconds p95.
- Alembic upgrade, schema check, and downgrade passed. The incident CLI demo
  reached `SUCCEEDED` with two separate worker invocations.
- Ruff lint checks passed.

## Remaining

| Plan milestone | Work to do | Exit check |
| --- | --- | --- |
| Week 10 · Deployment and demo | Add Docker Compose, CI with PostgreSQL, architecture decision records, a failure postmortem, and demo instructions and recording. | A clean checkout can run the sample with two workers and reproduce the crash and approval scenarios. |
