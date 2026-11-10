# Anchor

Anchor is a Python workflow engine for running agent work across process
failures and human decisions. See the [implementation plan](durable_agent_engine_plan.md)
for the target architecture and evaluation.

The current SDK validates versioned DAGs before execution, validates workflow
inputs and handler outputs, and passes each handler its direct parents' outputs
in the declared dependency order. The sequential reference runner keeps its
outputs and event history in memory for the life of the process.

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
