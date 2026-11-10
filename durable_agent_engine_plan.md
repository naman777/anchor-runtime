# Anchor: Durable Agent Workflow Engine

Implementation plan and interview evaluation • 10 November 2026

## 1. Recommendation and rating

Build a compact Python workflow engine that can reliably continue agent work after process failure, delayed approval, or transient tool errors. Make correctness visible through an execution timeline, fault injection, and reproducible tests.

**My assessment: 9/10 potential for a fresher SDE-1 portfolio targeting agentic AI infrastructure; about 8/10 for application-focused agentic AI roles until the agent workflow and evaluations are convincing.** These are subjective assessments of the proposed scope, not hiring statistics or a promise of interviewer response. Implementation quality and your ability to defend decisions determine the final rating.

| Delivered version | My portfolio rating | Reason |
|---|---:|---|
| Design document only | 4/10 | Good ambition; little evidence of engineering ability |
| Happy-path graph runner with model calls | 6/10 | Useful project, but reliability claims remain unproven |
| Custom durable runtime, recovery tests, approvals, one evaluated workflow | 8.5/10 | Strong evidence of backend and agent engineering |
| Above, plus race tests, fault demo, measured benchmarks and clear tradeoffs | 9/10 | Distinctive and defensible for a fresher |

The strongest pitch: “I built a Postgres-backed agent runtime, then tested what happens when workers die between an external action and its checkpoint.”

This project does not replace DSA, SQL, OS/networking fundamentals, or clear interview explanations. A smaller implementation you understand deeply is a better portfolio signal than a large generated codebase you cannot debug.

## 2. Scope and assumptions

Plan for **10 weeks at 15–20 focused hours per week**, assuming working Python, REST and SQL knowledge. Treat 150–200 hours as a planning estimate, not a guarantee. If async programming and transactions are new, add two learning weeks. Deliver the durable core by week 4 so later scope can be reduced safely.

The first version supports trusted, registered workflow code, one deployment, one PostgreSQL database, and several worker processes. It survives worker/API restarts while its database remains durable and available. It is not a multi-region system and does not protect against loss of the database volume.

**Required:** typed workflow DAGs; condition nodes; durable per-step output; retries and deadlines; worker leases; human approval with expiry; fixed fan-out/fan-in; bounded tool-using agents; audit events; one useful example; integration tests and an evaluation report.

**Defer:** arbitrary graph cycles, untrusted code execution, visual workflow authoring, Kubernetes, Kafka, Redis, multi-tenancy, distributed consensus, custom vector infrastructure, automatic compensation, and dynamic subworkflow spawning. A bounded agent loop can exist inside the DAG, but each model/tool round must have durable records.

Build the orchestration core yourself. Use existing libraries for HTTP, schemas, SQL access, migrations, tracing and model APIs. Existing engines are useful references: LangGraph already provides persistence and Temporal provides durable workflow/activity execution. The portfolio goal is to demonstrate a carefully bounded implementation and its guarantees, rather than claim to replace those systems [1][2]. In a commercial project, adopting an established engine would often be the more practical choice.

## 3. Architecture and stack

| Component | Proposed choice | Responsibility |
|---|---|---|
| Core and SDK | Python, dataclasses/Pydantic | Workflow definitions, typed inputs and outputs, validation |
| API | FastAPI | Start runs, inspect status, submit approvals, request cancellation |
| Persistence | PostgreSQL, SQLAlchemy, Alembic | Authoritative state, ready-step queue, leases, timers and events |
| Execution | Separate Python worker processes | Claim eligible steps and execute registered handlers |
| Recovery | Small scheduler/reaper process | Requeue expired leases, release due retries, expire approvals |
| Agent adapter | One provider SDK behind an interface | Structured model calls, recorded tool requests, token usage |
| Inspection | React/TypeScript or a simple server-rendered page | Run graph, attempts, approval inbox, cost and event timeline |
| Observability | OpenTelemetry plus structured logs | Correlate workflow, node, attempt and tool call |
| Tests | pytest and real PostgreSQL | Transaction, concurrency, recovery and contract tests |
| Local deployment | Docker Compose | API, database, two workers and scheduler |

Postgres doubles as the durable work queue. Its documentation explicitly identifies `SKIP LOCKED` as useful for multiple consumers of a queue-like table [3]. It does not itself provide leases, retry policies or exactly-once behavior; those must be implemented.

Use one codebase with separate process entrypoints. Keep frontend work modest. Polling or server-sent events is enough for the timeline. Store small JSON payloads in Postgres; add object storage only if actual payload size warrants it.

Execution flow:

1. API validates a registered graph, pins its version and creates a run plus initial READY steps in one transaction.
2. A worker claims one READY step using a short transaction and row lock, assigns a lease token and commits.
3. The worker executes the handler outside that transaction, renewing its lease when necessary.
4. A short completion transaction validates ownership and run status, persists output, records events, and makes newly eligible steps READY.
5. The scheduler recovers expired claims and persisted timers after restarts.

**No database transaction remains open while waiting for an LLM, tool response or human.**

## 4. Data model and invariants

| Table | Important data and constraints |
|---|---|
| `workflow_versions` | Immutable graph definition, schema versions, handler version map, content hash |
| `runs` | Run ID, pinned workflow version, state, input, cancellation flag, client idempotency key |
| `steps` | Run/node identity, state, dependencies, immutable output, retry due time, owner, lease token, expiry; unique logical node per run |
| `attempts` | Step ID, attempt number, timestamps, error category, trace ID; unique step/attempt |
| `approvals` | Step ID, exact action payload/hash, deadline, pending/approved/rejected/expired state, actor and decision time |
| `agent_calls` | Stable logical call ID, round number, model/prompt version, request, response, usage and status |
| `tool_operations` | Stable operation key, tool name, argument hash, intended action, external reference, result and status |
| `events` | Per-run ordered sequence, event type, actor, timestamps, correlated step/attempt/call IDs |

Store branch outputs independently. A join reads committed parent outputs in stable node order. Do not let parallel workers overwrite a shared conversation JSON blob. Start with small immutable per-step results rather than duplicating the entire run on every checkpoint.

Serialize short scheduling/completion transitions for the same run by locking its run row. Establish one lock order across all transition code. This makes join scheduling and event numbering easier to reason about, at the cost of a bottleneck for exceptionally wide runs. Measure that tradeoff later.

Seven invariants to document and test:

1. A step becomes eligible only when its dependency/branch rules are satisfied.
2. A committed successful step is not automatically executed again during recovery.
3. Only the current, unexpired lease holder may commit an attempt result.
4. A protected tool invocation requires a valid approval for its exact action payload.
5. A logical join is scheduled at most once despite concurrent parent completions.
6. A run remains pinned to its original graph and handler versions.
7. Terminal or cancelled runs cannot schedule fresh work.

These guarantees assume durable database commits, correct handler contracts and eventual service recovery; they are not a claim of universal fault tolerance.

## 5. Execution semantics: the part to master

### Node state machine

Use `PENDING`, `READY`, `RUNNING`, `RETRY_WAIT`, `WAITING_APPROVAL`, `SUCCEEDED`, `FAILED`, `SKIPPED` and `CANCELLED`.

`PENDING` is blocked by dependencies. `READY` can be claimed. `RUNNING` has a current attempt and lease. A retry moves to `RETRY_WAIT` with a stored due time. An approval gate waits without consuming a worker. Define run-level states separately; one waiting branch need not stop other eligible branches.

Terminal failure propagates a documented outcome to dependent nodes. For v1, joins require all selected parents to succeed. An exclusive branch marks the unselected route SKIPPED; a merge explicitly understands selected versus unselected inputs. Do not leave unreachable nodes PENDING forever.

### Atomic completion

In one database transaction: validate current lease and run state, mark the attempt/step complete, persist output, evaluate downstream readiness, enqueue newly READY steps, and append events. A crash before commit leaves no partial completion; a crash after commit leaves recoverable durable state.

Use uniqueness constraints in addition to application checks. For v1, keeping both queue and state in Postgres avoids a database/message-broker dual-write. If a broker is added later, use a transactional outbox; this still does not solve an unrelated external API's side effects.

### Leases and stale workers

Starting development settings: a 30-second lease, renewal every 10 seconds, and a 2-second recovery scan. Use database time for persisted expiry comparisons. A reclaimer increments a fencing token. Every heartbeat and completion checks the token and that the lease is still valid. A worker that resumes after losing its lease cannot overwrite the new attempt's result.

Fencing protects engine state. It does not stop a stale process from contacting an external system. Tool-level idempotency and authorization are separate controls.

### At-least-once execution and external effects

If an external service accepts an action and the worker dies before recording success, the next worker cannot infer whether the action happened from the engine database alone. Temporal's documentation describes this general need for idempotent activities when completion is not recorded [2].

State the engine's contract honestly: **attempts may execute more than once; external effects are deduplicated only when the tool or receiver supports it.**

Persist the operation identity and exact arguments before dispatch. Derive the stable key from run, node and logical operation/call ID. Do not include the retry attempt number. For an agent loop, persist tool-call identity before running it, and reuse it after restart. Reject a reused key with different arguments.

The demo ticket service must implement a unique operation key and return the original ticket for repeated requests. Make its deduplication and ticket creation atomic within that service. For a service without idempotency or lookup-by-reference, an ambiguous write moves to `NEEDS_RECONCILIATION`; do not blindly retry it. Treat that as a tool-operation state that blocks or fails the step with an explicit operator action.

### Retries, deadlines and cancellation

Classify transport errors, rate limits and selected server failures as retryable. Treat invalid credentials, invalid tool arguments and policy failures separately. Configure a small explicit retry count and overall execution deadline. Use capped exponential backoff with full jitter, respect `Retry-After` where applicable, and persist `next_attempt_at`. Workers must not sleep for long retry intervals.

Persist the sampled due time once. Inject clocks and random generators in tests. A step timeout is independent of a workflow deadline and approval deadline.

Cancellation blocks future scheduling and requests cooperative termination of running work. A late completion cannot revive the run. An already accepted external action may still finish; cancellation does not imply rollback. Document that limitation.

### Resume versus replay

Resume uses stored outputs and continues unfinished work. An inspection replay reconstructs the timeline without external calls. A fresh re-execution creates a new run and explicitly may create new external effects. Do not present these three operations as interchangeable. LLM responses are nondeterministic: reuse recorded completed responses; an unrecorded response may need another call and incur additional cost.

## 6. Human approval as a durable protocol

Show the reviewer the exact proposed action, destination, arguments, evidence and expiry. Bind approval to a payload hash, workflow version and logical operation ID. A changed action requires new approval.

Create the approval record and mark the gate waiting in one transaction. Submit approval through an authenticated endpoint with reviewer authorization. Check the pending state and database deadline atomically while resolving the decision. A duplicate identical submission returns the existing decision; conflicting decisions return a conflict. Never accept expired approval simply because the expiry scanner has not run yet.

The expiry worker and approval endpoint use the same guarded transition. Whichever legally commits wins; exactly one continuation is scheduled in that transaction. Default timeout behavior is “record escalation and stop the protected action.” For the sample, approval rejection records the rejection and ends that action path. Both paths are visible in the timeline.

Persist the acting user, decision, reason and time. Avoid anonymous approval URLs in a public demo. Keep secrets and sensitive payloads out of logs. A simple single-project reviewer role is sufficient for the portfolio scope.

## 7. The agentic AI layer

A reliable task queue alone does not demonstrate agentic AI ability. Add a bounded evidence investigator that chooses among read-only tools, requests missing information when evidence is insufficient, and proposes a structured next action.

Each model round and tool call is a durable substep. Persist requests before dispatch and responses before advancing the agent loop. Otherwise a resumed agent node could repeat its entire internal tool sequence.

Define a small adapter interface with model name, prompt version, structured output schema, allowed tools, maximum rounds, token cap and deadline. Start with one real provider plus a deterministic fake provider for tests. Record actual usage when supplied and label estimated charges as estimates. Responses lost during crashes can cause unobservable/uncertain usage; report that caveat.

Tools use JSON schemas and server-side validation. Treat retrieved text and model output as untrusted input. Tool permission checks and approval enforcement live in application code. A model must not bypass approval by writing “approved” in its output. Bound concurrency and reserve estimated request budget atomically before concurrent model dispatches; reconcile known usage afterward. The first budget is a conservative guard, not a guarantee of exact provider billing.

For a claim of agentic behavior, show at least two evidence-dependent tool paths, an insufficient-evidence escalation, and a rejected tool request. Calling three prompts in sequence is not the full demonstration.

## 8. Flagship example: incident investigation and approved ticket creation

Use synthetic incidents, versioned runbooks and a local mock ticket service. Avoid dependency on changing live infrastructure during an interview.

1. Accept an incident description and service metadata.
2. Fan out to two independent investigators: one queries simulated logs/metrics; the other retrieves runbook evidence and recent mock deployment changes.
3. Within a small round limit, each investigator chooses read-only tools and returns structured evidence references.
4. Join outputs and generate a diagnosis, uncertainties and proposed ticket payload.
5. Validate required fields and evidence references. Missing support routes to escalation; do not rely solely on model-reported confidence.
6. Persist an approval gate for the exact ticket action. Reject/expire routes to an explicit recorded outcome.
7. On approval, invoke the idempotent mock ticket service and save its external ID.
8. Produce a report linking the decision, evidence, approval and execution timeline.

The memorable fault: the mock ticket service creates the ticket, then the worker dies before committing its result. Restart the worker. The same operation key returns the same ticket. The engine completes, and the service contains exactly one ticket.

Add a second small document-review workflow only after the first is complete: parallel evidence checks, synthesis and human approval before final export. Its purpose is to demonstrate SDK reuse without changing the engine, not to double the product scope.

## 9. Ten-week implementation backlog

| Week | Build | Exit evidence |
|---|---|---|
| 1 | Written guarantees, repository setup, graph schema, DAG validation, fake provider, sequential runner | Invalid cycles/missing handlers rejected; typed sample runs locally |
| 2 | Postgres migrations, run/step/attempt records, atomic completion, immutable output checkpoints | Restart between steps; committed steps remain complete |
| 3 | Worker claims, leases, heartbeats, stale-owner fencing, multiple workers | Kill worker; another continues; stale result is rejected |
| 4 | Persisted retries, deadlines, tool-operation ledger, idempotent mock ticket service | Crash-after-effect test produces one external ticket |
| 5 | Approval persistence, reviewer endpoint, expiry scanner, races and cancellation | Restart while waiting; approve later; timeout/approval race has one outcome |
| 6 | Fixed fan-out, branch selection, fan-in and scheduling constraints | Retry one branch; completed sibling is reused; join runs once |
| 7 | Durable bounded agent rounds, one real model adapter, evidence investigator | Different evidence produces different allowed tool paths |
| 8 | Run timeline, approval UI, trace correlation, usage accounting and redaction | Every sample run can be inspected from API call through tool result |
| 9 | Failure suite, frozen AI eval set, load harness and measured report | Repeatable commands produce test and evaluation artifacts |
| 10 | Deployment, CI, clean setup, architecture decisions, demo recording and README | Another person can run the sample and fault demo from instructions |

Capture basic event logs and tests from week 1; week 8 improves their presentation. Every week ends with a working vertical slice. If week 4 is incomplete, reduce UI/second-workflow scope before adding features.

Suggested modules: `engine/graph`, `engine/state`, `engine/scheduler`, `engine/worker`, `engine/persistence`, `engine/approvals`, `engine/agents`, `engine/tools`, `api`, `examples`, `tests`, `evals`, `benchmarks`, and `docs/decisions`. Keep orchestration independent of any particular model provider.

Minimum API: register a workflow version; start an idempotent run; get run state/steps/events; list pending approvals; resolve an approval; request cancellation. Recovery happens automatically. Avoid a generic “resume” endpoint that lets callers skip policy gates.

## 10. Verification and evaluation

### Reliability tests with a real database

| Injected situation | Required observation |
|---|---|
| Crash before output commit | Step is eligible for recovery; partial output is not visible |
| Crash after output commit | Stored successful step is reused |
| Crash after ticket creation but before checkpoint | Retried operation returns original ticket |
| Two workers claim same READY step | Only one current lease is issued |
| Old worker completes after lease recovery | Its result is rejected by ownership check |
| One parallel branch fails transiently | Only unfinished/failed work retries |
| Parents finish at the same time | Join is scheduled once |
| Approve twice or approve versus expire concurrently | One legal decision and continuation |
| API and workers restart during approval wait | Approval and deadline survive |
| Database unavailable during tool completion | No false success acknowledgement; recovery reconciles safely |
| Workflow v2 deployed while v1 is waiting | Run retains v1 semantics or blocks explicitly if handler unavailable |
| Cancel while a node runs | No fresh downstream tasks; late result cannot revive run |
| Model requests an unauthorized tool | Request is blocked and recorded |
| External write has unknown outcome and no deduplication | Reconciliation is required, not blind retry |

Use explicit failpoints around critical boundaries so tests are reproducible. Add randomized schedules after deterministic cases pass. The test oracle checks invariants, not merely process exit codes. Test both rollback and commit ambiguity. Verify expected states through a new database connection after killing a worker.

### Agent quality evaluation

Create 60 curated synthetic incident cases: 20 for prompt/tool development and 40 held out. Include a documented mix of clear diagnoses, missing evidence, conflicting signals, malicious instructions in retrieved content, and tool failures. Keep ground-truth labels and required evidence references under version control. Do not tune prompts against the held-out set and then call it untouched.

Compare a simple sequential single-agent baseline with the parallel investigator workflow, using the same model, evidence corpus, tool permissions and comparable total budget. The baseline can use the same engine so the comparison isolates agent design. For recovery overhead, separately compare engine execution with a simple mock sequential runner.

| Metric | Definition |
|---|---|
| Task success | Cases meeting the predefined diagnosis/action rubric divided by evaluated cases |
| Evidence support | Claimed facts with valid supporting evidence, using a documented human rubric |
| Escalation rate | Cases routed to human review for missing/conflicting evidence divided by all cases; exclude mandatory action approval |
| Escalation recall | Cases correctly escalated divided by cases labeled as requiring escalation |
| Unsafe-action count | Protected actions invoked without valid approval; target zero in the evaluated suite |
| Cost per attempted case | Total known model cost divided by attempted cases |
| Cost per successful case | Total known model cost divided by successful cases; undefined if none succeed |
| Latency | p50/p95 agent execution time, with human waiting reported separately |

Report denominators, failures and repeated-run variability. Repeat the held-out run three times if budget allows. Include failed attempts and repair calls in cost totals; date the pricing assumptions. If an LLM judge is used, audit disagreements against human labels rather than treating it as ground truth.

### Engine benchmarks and targets

Use 1,000 deterministic mock workflows with documented graph width, payload size and handler delays. Test 1, 2, 4 and 8 workers on the same machine. Report committed steps/second, queue wait, p50/p95 scheduling overhead, database load, run completion rate and recovery latency. Run warm-up and at least three measured repetitions. Keep live LLM performance separate from scheduler throughput.

Initial acceptance goals—not measured results: no invariant violations in the defined fault suite; zero duplicate tickets in the idempotent-service test; all eligible crash tests eventually terminate; p95 recovery below 45 seconds with the proposed 30-second lease in the documented healthy local setup. Revisit the latency target based on measurements, recording reasons. Do not promise arbitrary production scale from laptop benchmarks.

## 11. Interview demo and packaging

Prepare a five-minute demo with a deterministic mode and a separately labeled live-model mode:

1. **0:00–0:45:** State the problem and start an incident investigation. Show the graph and parallel branches.
2. **0:45–1:30:** Show evidence, tool calls and the proposed ticket. Explain why the model cannot approve itself.
3. **1:30–2:15:** Stop/restart application processes while approval is pending. Show the same persisted approval.
4. **2:15–3:30:** Approve. Trigger the failpoint after external ticket creation and before checkpoint. Restart the worker; show one external ticket and the recovered run.
5. **3:30–4:15:** Show the retry/attempt history and current-lease check. Explain at-least-once execution in plain language.
6. **4:15–5:00:** Show the eval/benchmark table, one failure case and the next bottleneck you would investigate.

Practice answering: Why build a custom core? Why Postgres instead of Redis? What if the external tool succeeds but its response is lost? What does a lease protect? Can a stale worker still make an API call? What happens when approval and expiry race? How do concurrent branches merge? What breaks if graph code changes? Why is model temperature zero insufficient for deterministic execution? What evidence shows that two agents help?

README order: short demo video; problem and guarantees; one architecture diagram; quickstart; fault reproduction; test/evaluation results; limitations; design decisions. Include a reproducible environment lockfile and schema migrations. CI runs pure/unit tests and real-Postgres integration tests; paid model evaluations run on demand.

Publish three short decision records: Postgres as queue/source of truth; at-least-once execution plus tool idempotency; immutable per-node outputs plus version-pinned resume. Include one failure postmortem explaining a real bug you found and fixed.

Resume wording should describe completed work only. A useful eventual structure is: “Built a Postgres-backed agent workflow runtime with crash recovery, durable approvals and parallel execution; validated [N] failure scenarios and measured [actual metric] across [documented workload].” Replace every bracket with measured facts and omit unmeasured claims.

## 12. Definition of done and scope cuts

The project is ready to show when a clean checkout starts from documented instructions; two worker processes execute the sample; crash/restart and approval expiry are reproducible; the external duplicate-action case is demonstrated; every run has an inspectable history; the held-out evaluation and benchmark can be regenerated; and you can explain the main transaction boundaries without reading the code.

If time is short, cut in this order: second example, animation-heavy dashboard, multiple providers, dynamic fan-out, advanced budget reservations. Keep a simpler request-count/token cap if budget reservations are deferred. Preserve the durable core, idempotency demo, approval correctness, one bounded agent example, and tests. Do not label omitted functionality as implemented.

## Sources and how they informed the plan

Technical documentation reviewed on 10 November 2026. The architecture, schedule, targets and portfolio ratings above are recommendations, not claims from these sources.

1. [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence): established checkpointing/thread persistence reference and context for what existing frameworks provide.
2. [Temporal Activity Definition](https://docs.temporal.io/activity-definition): idempotency guidance and the failure window before activity completion is recorded.
3. [PostgreSQL SELECT and locking](https://www.postgresql.org/docs/current/sql-select.html): row locking and the queue-consumer use case for `SKIP LOCKED`.
