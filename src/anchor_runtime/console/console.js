"use strict";

const byId = (id) => document.getElementById(id);
const formatTime = (time) => time ? new Date(time).toLocaleString() : "—";
const formatValue = (value) => value === null ? "unavailable" : value;

function node(tag, text, className) {
  const element = document.createElement(tag);
  if (text !== undefined) element.textContent = text;
  if (className) element.className = className;
  return element;
}

async function request(path, token, options = {}) {
  const headers = { Authorization: `Bearer ${token}` };
  if (options.body) headers["Content-Type"] = "application/json";
  const response = await fetch(path, { ...options, headers, cache: "no-store", credentials: "omit" });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(typeof error.detail === "string" ? error.detail : `Request failed (${response.status})`);
  }
  return response.json();
}

function sectionList(target, title, rows, ordered = false) {
  target.append(node("h3", title));
  if (!rows.length) {
    target.append(node("p", "No records yet.", "meta"));
    return;
  }
  const list = node(ordered ? "ol" : "ul");
  for (const row of rows) list.append(node("li", row));
  target.append(list);
}

function renderRun(run) {
  const target = byId("run-detail");
  target.replaceChildren(
    node("h3", `${run.workflow} · version ${run.version}`),
    node("p", run.state, "state"),
    node("p", `Trace ${formatValue(run.trace_id)}`, "meta"),
  );
  const steps = new Map(run.steps.map((step) => [step.id, step]));
  target.append(node("h3", "Workflow graph"));
  const graph = node("ol");
  for (const step of run.graph) {
    const result = steps.get(step.id);
    const item = node("li");
    item.append(node("strong", `${step.id} · ${result.state}`));
    const dependencies = step.parents.map((parent) =>
      step.when?.[parent] ? `${parent} [${step.when[parent]}]` : parent
    );
    if (dependencies.length) {
      item.append(node("p", `Depends on ${dependencies.join(", ")}`, "meta"));
    }
    if (result.output !== null) {
      const output = node("details");
      output.append(node("summary", "Checkpoint"), node("pre", JSON.stringify(result.output, null, 2)));
      item.append(output);
    }
    graph.append(item);
  }
  target.append(graph);

  const usage = run.usage;
  target.append(
    node("h3", "Model usage"),
    node("p", `${usage.dispatches} dispatches · ${usage.uncheckpointed_dispatches} without recorded responses`),
    node("p", `Reported tokens · input ${formatValue(usage.reported_input_tokens)} · output ${formatValue(usage.reported_output_tokens)}`),
  );
  sectionList(target, "Model rounds", run.model_calls.map((call) =>
    `${call.node_id} / round ${call.round} · ${call.model} · ${call.state}`
  ));
  sectionList(target, "Tool operations", run.operations.map((operation) =>
    `${operation.node_id} / ${operation.id} · ${operation.name} · ${operation.state}`
  ));
  sectionList(target, "Approvals", run.approvals.map((approval) =>
    `${approval.id} · ${approval.tool_name} · ${approval.state} · deadline ${formatTime(approval.deadline)}`
  ));
  sectionList(target, "Attempts", run.attempts.map((attempt) =>
    `${attempt.node_id} #${attempt.number} · ${attempt.state}${attempt.error_type ? ` · ${attempt.error_type}` : ""} · trace ${formatValue(attempt.trace_id)}`
  ));
  sectionList(target, "Event timeline", run.events.map((event) =>
    [formatTime(event.at), event.type, event.node_id, event.attempt && `#${event.attempt}`, event.subject]
      .filter(Boolean).join(" · ")
  ), true);
}

byId("inspect-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.currentTarget.querySelector("button");
  const status = byId("run-status");
  byId("run-detail").replaceChildren();
  button.disabled = true;
  status.textContent = "Loading run…";
  try {
    const run = await request(`/runs/${encodeURIComponent(byId("run-id").value.trim())}`, byId("operator-token").value);
    renderRun(run);
    status.textContent = `Loaded run ${run.id}.`;
  } catch (error) {
    status.textContent = error.message;
  } finally {
    button.disabled = false;
  }
});

function approvalCard(approval, token) {
  const card = node("article");
  card.append(
    node("h3", approval.tool_name),
    node("p", `Destination · ${approval.destination}`),
    node("p", `Decision deadline · ${formatTime(approval.deadline)}`),
    node("p", `Run ${approval.run_id} · ${approval.node_id} / ${approval.operation_id}`, "meta"),
    node("p", `Action digest ${approval.action_digest}`, "meta"),
    node("pre", JSON.stringify(approval.arguments, null, 2)),
  );
  const reason = node("input");
  reason.maxLength = 2000;
  const label = node("label", "Decision reason (optional)");
  label.append(reason);
  const actions = node("div", undefined, "actions");
  const approve = node("button", "Approve action");
  const reject = node("button", "Reject action", "secondary");
  const status = node("p");
  status.setAttribute("role", "status");
  actions.append(label, approve, reject);
  card.append(actions, status);

  async function decide(approved) {
    approve.disabled = reject.disabled = true;
    status.textContent = "Saving decision…";
    try {
      const result = await request(`/approvals/${encodeURIComponent(approval.id)}/decision`, token, {
        method: "POST",
        body: JSON.stringify({ action_digest: approval.action_digest, approve: approved, reason: reason.value.trim() || null }),
      });
      card.remove();
      byId("inbox-status").textContent = `${result.tool_name} · ${result.state.toLowerCase()}.`;
    } catch (error) {
      status.textContent = error.message;
      approve.disabled = reject.disabled = false;
    }
  }

  approve.addEventListener("click", () => decide(true));
  reject.addEventListener("click", () => decide(false));
  return card;
}

byId("inbox-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.currentTarget.querySelector("button");
  const status = byId("inbox-status");
  const list = byId("approval-list");
  const token = byId("reviewer-token").value;
  list.replaceChildren();
  button.disabled = true;
  status.textContent = "Loading pending actions…";
  try {
    const approvals = await request("/approvals", token);
    list.replaceChildren(...approvals.map((approval) => approvalCard(approval, token)));
    status.textContent = approvals.length ? `${approvals.length} pending action(s).` : "No pending actions.";
  } catch (error) {
    status.textContent = error.message;
  } finally {
    button.disabled = false;
  }
});
