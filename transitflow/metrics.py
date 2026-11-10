"""Shared Prometheus metrics for every TransitFlow service.

The metric *names* here are identical across the Java, Go and Python ports so a
single Grafana dashboard works against any of them. Business code imports the
metric objects directly and increments/observes them inside the real logic
(booking creation, terminal transitions, payment outcomes, outbox draining).

Each service calls :func:`setup_metrics(app)` once to:
  * add an HTTP-latency middleware that records
    ``transitflow_http_request_duration_seconds`` (labels: method/handler/status), and
  * expose ``GET /metrics`` in Prometheus text format.

Multiprocess note: prometheus-client's default (in-process) registry only works
correctly with a SINGLE process. Run each uvicorn service with one worker
(the default) so these counters/gauges are not split across workers.
"""

from __future__ import annotations

import time

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    disable_created_metrics,
    generate_latest,
)
from starlette.requests import Request
from starlette.responses import Response

# Suppress the per-Counter ``_created`` timestamp series — it is noise for this
# dashboard and keeps the exposition clean.
disable_created_metrics()

# prometheus-client reserves ``<counter>_created`` as a clashing name for every
# counter. That makes ``transitflow_bookings_total`` (which reserves
# ``transitflow_bookings_created``) collide, in a single registry, with the value
# series of ``transitflow_bookings_created_total``. Both names are required (they
# are shared across the Java/Go/Python ports), so we register the creation counter
# in its OWN registry and simply concatenate both registries' exposition on
# ``/metrics``. Prometheus treats the two distinct metric names without issue.
REGISTRY = CollectorRegistry(auto_describe=True)
_CREATED_REGISTRY = CollectorRegistry(auto_describe=True)

# ---- Business metrics (shared names across all language ports) ----

bookings_created_total = Counter(
    "transitflow_bookings_created_total",
    "Total number of bookings created (SAGA started).",
    registry=_CREATED_REGISTRY,
)

bookings_total = Counter(
    "transitflow_bookings_total",
    "Bookings that reached a terminal state, by status.",
    ["status"],  # CONFIRMED | CANCELLED
    registry=REGISTRY,
)

payments_total = Counter(
    "transitflow_payments_total",
    "Payments processed, by result.",
    ["result"],  # COMPLETED | FAILED
    registry=REGISTRY,
)

outbox_pending = Gauge(
    "transitflow_outbox_pending",
    "Current number of unsent transactional-outbox rows.",
    registry=REGISTRY,
)

saga_duration_seconds = Histogram(
    "transitflow_saga_duration_seconds",
    "Seconds from booking creation to its terminal (CONFIRMED/CANCELLED) state.",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
    registry=REGISTRY,
)

# ---- HTTP latency (custom middleware; name is under our control) ----

http_request_duration_seconds = Histogram(
    "transitflow_http_request_duration_seconds",
    "HTTP request latency in seconds.",
    ["method", "handler", "status"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    registry=REGISTRY,
)


def observe_saga(status: str, created_at) -> None:
    """Record a terminal SAGA transition: bump the per-status counter and observe
    the end-to-end duration measured from the booking's ``created_at``."""
    bookings_total.labels(status=status).inc()
    try:
        from datetime import datetime, timezone

        elapsed = (datetime.now(timezone.utc) - created_at).total_seconds()
        if elapsed >= 0:
            saga_duration_seconds.observe(elapsed)
    except Exception:  # noqa: BLE001 — metrics must never break business logic.
        pass


def setup_metrics(app) -> None:
    """Attach the HTTP-latency middleware and a ``/metrics`` endpoint to a FastAPI
    (Starlette) app."""

    @app.middleware("http")
    async def _record_latency(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        elapsed = time.perf_counter() - start
        route = request.scope.get("route")
        handler = getattr(route, "path", request.url.path)
        http_request_duration_seconds.labels(
            method=request.method, handler=handler, status=str(response.status_code)
        ).observe(elapsed)
        return response

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        body = generate_latest(REGISTRY) + generate_latest(_CREATED_REGISTRY)
        return Response(body, media_type=CONTENT_TYPE_LATEST)
