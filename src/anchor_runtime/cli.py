"""Run and inspect the sample with a PostgreSQL-backed worker."""

import argparse
import logging
import os
import socket
from time import sleep
from uuid import UUID

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from anchor_runtime.durable import DurableRuntime
from anchor_runtime.example import FakeProvider, incident_workflow
from anchor_runtime.worker import work_once


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("service")
    start.add_argument("error_rate", type=float)
    start.add_argument("--key")
    worker = commands.add_parser("worker")
    worker.add_argument("--once", action="store_true")
    inspect = commands.add_parser("inspect")
    inspect.add_argument("run_id", type=UUID)
    args = parser.parse_args()

    url = os.environ.get("ANCHOR_DATABASE_URL")
    if not url:
        parser.error("Set ANCHOR_DATABASE_URL to a PostgreSQL connection URL")
    logging.basicConfig(level=logging.INFO)
    engine = create_engine(url, pool_pre_ping=True)
    workflow = incident_workflow(FakeProvider())
    try:
        runtime = DurableRuntime(engine, [workflow])
        if args.command == "start":
            payload = {"service": args.service, "error_rate": args.error_rate}
            print(
                runtime.start(
                    workflow.name, workflow.version, payload, idempotency_key=args.key
                )
            )
        elif args.command == "inspect":
            history = runtime.inspect(args.run_id)
            if history is None:
                parser.error("Run not found")
            print(f"{history.run.id} {history.run.state}")
            for event in history.events:
                print(
                    event.sequence,
                    event.at.isoformat(),
                    event.type,
                    event.node_id or "",
                )
            for step in history.steps:
                print(step.node_id, step.state, step.output)
        else:
            owner = f"{socket.gethostname()}:{os.getpid()}"
            if args.once:
                work_once(runtime, owner)
                return
            while True:
                try:
                    if work_once(runtime, owner):
                        continue
                except OperationalError:
                    logging.warning("Database connection failed for worker %s", owner)
                sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
