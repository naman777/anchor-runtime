"""Synchronous handlers run outside transactions with a lease heartbeat."""

import logging
from datetime import timedelta
from threading import Event, Thread

from sqlalchemy.exc import OperationalError

from anchor_runtime.durable import Claim, DurableRuntime
from anchor_runtime.policy import RetryableError
from anchor_runtime.tools import ApprovalWait

logger = logging.getLogger(__name__)


def _heartbeat(runtime: DurableRuntime, claim: Claim, stopped: Event) -> None:
    interval = runtime.lease.total_seconds() / 3
    while not stopped.wait(interval):
        try:
            if not runtime.renew(claim):
                logger.warning(
                    "Lease lost for run %s node %s", claim.run_id, claim.node_id
                )
                return
        except OperationalError:
            logger.warning(
                "Heartbeat database connection failed for run %s", claim.run_id
            )


def work_once(runtime: DurableRuntime, owner: str) -> bool:
    claim = runtime.claim(owner)
    if claim is None:
        return False
    stopped = Event()
    heartbeat = Thread(target=_heartbeat, args=(runtime, claim, stopped), daemon=True)
    heartbeat.start()
    try:
        try:
            output = runtime.execute(claim)
        except ApprovalWait:
            return True
        except OperationalError:
            raise
        except Exception as error:
            transient = isinstance(error, RetryableError)
            accepted = runtime.fail(
                claim,
                type(error).__name__,
                retryable=transient,
                retry_after=error.retry_after if transient else timedelta(0),
            )
        else:
            accepted = runtime.succeed(claim, output)
        if not accepted:
            logger.warning(
                "Completion rejected for run %s node %s", claim.run_id, claim.node_id
            )
    finally:
        stopped.set()
        heartbeat.join()
    return True
