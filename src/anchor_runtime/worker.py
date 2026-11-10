"""Synchronous handlers run outside transactions with a lease heartbeat."""

import logging
from datetime import timedelta
from threading import Event, Thread

from sqlalchemy.exc import OperationalError

from anchor_runtime.durable import Claim, DurableRuntime
from anchor_runtime.policy import RetryableError
from anchor_runtime.telemetry import current_traceparent, log_event, span, trace_id
from anchor_runtime.tools import ApprovalWait

logger = logging.getLogger(__name__)


def _heartbeat(
    runtime: DurableRuntime, claim: Claim, stopped: Event, fields: dict
) -> None:
    interval = runtime.lease.total_seconds() / 3
    while not stopped.wait(interval):
        try:
            if not runtime.renew(claim):
                log_event(logger, logging.WARNING, "attempt.lease_lost", **fields)
                return
        except OperationalError:
            log_event(
                logger, logging.WARNING, "attempt.heartbeat_unavailable", **fields
            )


def work_once(runtime: DurableRuntime, owner: str) -> bool:
    claim = runtime.claim(owner)
    if claim is None:
        return False
    attributes = {
        "anchor.run_id": str(claim.run_id),
        "anchor.node_id": claim.node_id,
        "anchor.attempt": claim.number,
    }
    with span("anchor.step", parent=claim.traceparent, attributes=attributes):
        parent = current_traceparent()
        fields = {
            "run_id": claim.run_id,
            "node_id": claim.node_id,
            "attempt": claim.number,
            "trace_id": trace_id(parent),
        }
        if parent is not None and not runtime.attach_trace(claim, parent):
            log_event(logger, logging.WARNING, "attempt.lease_lost", **fields)
            return True
        _process_claim(runtime, claim, fields)
    return True


def _process_claim(runtime: DurableRuntime, claim: Claim, fields: dict) -> None:
    stopped = Event()
    heartbeat = Thread(
        target=_heartbeat, args=(runtime, claim, stopped, fields), daemon=True
    )
    heartbeat.start()
    try:
        try:
            output = runtime.execute(claim)
        except ApprovalWait:
            log_event(logger, logging.INFO, "attempt.waiting_approval", **fields)
            return
        except OperationalError:
            raise
        except Exception as error:
            transient = isinstance(error, RetryableError)
            outcome = "attempt.failure_recorded"
            accepted = runtime.fail(
                claim,
                type(error).__name__,
                retryable=transient,
                retry_after=error.retry_after if transient else timedelta(0),
            )
        else:
            outcome = "attempt.result_recorded"
            accepted = runtime.succeed(claim, output)
        if accepted:
            log_event(logger, logging.INFO, outcome, **fields)
        else:
            log_event(logger, logging.WARNING, "attempt.result_rejected", **fields)
    finally:
        stopped.set()
        heartbeat.join()
