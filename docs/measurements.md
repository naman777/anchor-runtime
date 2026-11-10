# Anchor measurement report

Recorded 2026-11-10. Reproduction commands are in the [README](../README.md).

## Incident evaluation

Provider `fake` · model `incident-rules-1` · split `held-out`.
Dataset SHA-256 `77859bd16d524801747ee17cf2c580797544bfc0b44ea0d8d8c359782fd7d034` (LF-normalized).

Cases per repetition by category.

| errors | latency | missing | conflict | injection | tool-failure |
| --- | --- | --- | --- | --- | --- |
| 8 | 8 | 8 | 6 | 5 | 5 |

Both workflows use the same corpus, model, evidence requirements, and run budget of 12 model dispatches. The parallel workflow has two four-round investigators. The baseline uses one sequential investigator with eight rounds and the combined read-only tool set.
Timing includes runtime execution and excludes reviewer decision time. The evaluator approves only proposals that meet the frozen ticket rubric.
A case passes when the run succeeds with the expected outcome, its reported evidence matches the fixture, and it records no unsafe ticket dispatch. For ticket cases, the proposed finding and required evidence IDs must also match the label.

| Repetition | Workflow | Success | Supported evidence | Escalations | Escalation recall | Unsafe dispatches | Denied requests | Dispatches | p50 ms | p95 ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | parallel | 40/40 | 111/111 | 24/40 | 24/24 | 0 | 5 | 212 | 328.8 | 461.4 |
| 1 | single_agent | 40/40 | 105/105 | 24/40 | 24/24 | 0 | 5 | 153 | 282.1 | 439.7 |
| 2 | parallel | 40/40 | 111/111 | 24/40 | 24/24 | 0 | 5 | 212 | 330.5 | 474.7 |
| 2 | single_agent | 40/40 | 105/105 | 24/40 | 24/24 | 0 | 5 | 153 | 277.7 | 392.1 |
| 3 | parallel | 40/40 | 111/111 | 24/40 | 24/24 | 0 | 5 | 212 | 349.7 | 505.0 |
| 3 | single_agent | 40/40 | 105/105 | 24/40 | 24/24 | 0 | 5 | 153 | 302.7 | 466.7 |

Failed case IDs.

- None.

The deterministic adapter made no paid calls. This measures fixture behavior and runtime overhead. The same project authored the cases and fake adapter, so these scores do not estimate live model quality.

## Engine benchmark

Windows AMD64, 16 logical CPUs, Python 3.12.4. PostgreSQL 17.11 on x86_64-windows, compiled by msvc-19.44.35229, 64-bit.
Each repetition preloads 1,000 workflows with 3 steps, width 2, 256-byte input strings, and a 2 ms sleep per handler. Each configuration has a 50-run warm-up.

| Workers | Steps/s median (range) | Queue p50 / p95 ms | Overhead p50 / p95 ms | Worker SQL / step | Worker transactions / step | Completed runs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 26.1 (24.1–27.2) | 40216.6 / 108583.9 | 13.9 / 42.3 | 25.3 | 4.0 | 3,000/3,000 |
| 2 | 52.4 (41.3–61.5) | 23691.4 / 55256.1 | 14.5 / 46.1 | 25.3 | 4.0 | 3,000/3,000 |
| 4 | 88.3 (67.4–90.3) | 17461.2 / 33424.0 | 19.5 / 52.6 | 25.4 | 4.0 | 3,000/3,000 |
| 8 | 73.1 (67.5–77.0) | 19267.6 / 41532.6 | 50.3 / 122.7 | 25.5 | 4.1 | 3,000/3,000 |

The in-memory sequential runner measured 272.5 steps/s median (266.1–315.2).

Queue wait runs from the ready event to claim and includes time spent preloaded before workers start. Overhead subtracts the configured handler sleep from the time between the claim and completion timestamps, so it includes OS timing jitter. The table uses the median percentile across repetitions. SQL and transaction counts come from worker connections and exclude the coordinator.

Recovery from one worker exit with a 30-second lease took 30.143, 30.211, 30.301 seconds across 3 claimed steps (p95 30.292 seconds). Timing starts when the coordinator observes process exit and ends at the recorded completion timestamp of each replacement attempt.
