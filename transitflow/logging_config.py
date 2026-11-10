"""Minimal structured-ish logging with a correlation id carried in a context var,
so a single booking can be traced across services in the logs (the Java edition
uses SLF4J MDC for the same purpose)."""

from __future__ import annotations

import contextvars
import logging

correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default="-"
)


class CorrelationIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id.get()
        return True


def configure_logging(service_name: str) -> None:
    handler = logging.StreamHandler()
    handler.addFilter(CorrelationIdFilter())
    handler.setFormatter(
        logging.Formatter(
            f"%(asctime)s %(levelname)-5s [{service_name},%(correlation_id)s] "
            "%(name)s - %(message)s"
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    # aiokafka is chatty at INFO during rebalances; quiet it to WARNING.
    logging.getLogger("aiokafka").setLevel(logging.WARNING)


def set_correlation_id(value: str | None) -> None:
    correlation_id.set(value or "-")
