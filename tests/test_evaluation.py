from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pytest

from anchor_runtime.durable import DurableRuntime
from anchor_runtime.fake_model import FakeInvestigator
from anchor_runtime.investigation import investigation_workflow
from anchor_runtime.tickets import TicketService
from evals.run import _case, load_cases, summarize


def test_frozen_evaluation_corpus_contains_each_category_in_each_split():
    cases, digest = load_cases()
    counts = Counter((case.split, case.category) for case in cases)
    assert len(counts) == 12
    assert len(digest) == 64
    assert (
        sum(
            case.expected.outcome == "ticket_created"
            for case in cases
            if case.split == "held-out"
        )
        == 16
    )


@pytest.mark.postgres
def test_evaluation_scores_baseline_and_parallel_under_same_rubric(engine, tmp_path):
    cases, _ = load_cases()
    case = next(
        case
        for case in cases
        if case.split == "development" and case.category == "latency"
    )
    service = TicketService(tmp_path / "receiver.sqlite")
    provider = FakeInvestigator()
    flows = [
        investigation_workflow(
            service,
            provider,
            provider_name="fake",
            model="test",
            version="eval-test",
            corpus={case.case_id: case.evidence},
            baseline=baseline,
        )
        for baseline in (False, True)
    ]
    runtime = DurableRuntime(engine, flows)
    with ThreadPoolExecutor(max_workers=2) as pool:
        rows = [_case(pool, runtime, flow, case, 20) for flow in flows]
    assert [row["dispatches"] for row in rows] == [6, 5]
    score = summarize(rows, fake=True)
    assert score["successes"] == 2
    assert score["unsafe_dispatches"] == 0
    assert score["evidence_supported"] == score["evidence_reported"] == 8
