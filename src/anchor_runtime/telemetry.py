"""Trace propagation and metadata-only logging for runtime processes."""

import json
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

_propagator = TraceContextTextMapPropagator()


@lru_cache(maxsize=1)
def _tracer() -> trace.Tracer:
    provider = TracerProvider(
        resource=Resource.create({"service.name": "anchor-runtime"})
    )
    if endpoint := os.environ.get("ANCHOR_OTLP_TRACES_ENDPOINT"):
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
        )
    return provider.get_tracer("anchor_runtime")


@contextmanager
def span(
    name: str,
    *,
    parent: str | None = None,
    attributes: dict | None = None,
) -> Iterator[trace.Span]:
    context = _propagator.extract({"traceparent": parent}) if parent else None
    with _tracer().start_as_current_span(
        name,
        context=context,
        attributes=attributes,
        record_exception=False,
        set_status_on_exception=False,
    ) as current:
        try:
            yield current
        except Exception:
            current.set_status(trace.StatusCode.ERROR)
            raise


def current_traceparent() -> str | None:
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)
    return carrier.get("traceparent")


def tag_run(run_id: object) -> None:
    trace.get_current_span().set_attribute("anchor.run_id", str(run_id))


def trace_id(traceparent: str | None) -> str | None:
    if traceparent is None:
        return None
    context = trace.get_current_span(
        _propagator.extract({"traceparent": traceparent})
    ).get_span_context()
    return trace.format_trace_id(context.trace_id) if context.is_valid else None


def log_event(
    logger: logging.Logger, level: int, event: str, **metadata: object
) -> None:
    logger.log(
        level,
        "%s",
        json.dumps({"event": event, **metadata}, default=str, separators=(",", ":")),
    )
