"""Consumes payment outcomes from ``payment.events`` and advances the SAGA to
CONFIRMED or into compensation. Runs as an asyncio background task."""

from __future__ import annotations

import asyncio
import logging

from transitflow.booking.orchestrator import BookingOrchestrator
from transitflow.booking.schemas import PaymentResultEvent
from transitflow.kafka import create_json_consumer
from transitflow.logging_config import set_correlation_id
from transitflow.topics import PAYMENT_EVENTS

log = logging.getLogger(__name__)


class PaymentResultConsumer:
    def __init__(self, orchestrator: BookingOrchestrator, bootstrap_servers: str) -> None:
        self._orchestrator = orchestrator
        self._bootstrap_servers = bootstrap_servers
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="payment-events-consumer")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        consumer = create_json_consumer(
            PAYMENT_EVENTS,
            bootstrap_servers=self._bootstrap_servers,
            group_id="booking-service",
        )
        await consumer.start()
        log.info("booking-service consuming %s", PAYMENT_EVENTS)
        try:
            async for record in consumer:
                await self._handle(record.value)
        except asyncio.CancelledError:
            raise
        finally:
            await consumer.stop()

    async def _handle(self, raw: dict) -> None:
        event = PaymentResultEvent.model_validate(raw)
        set_correlation_id(event.correlation_id)
        try:
            log.info("Payment result for booking %s: %s", event.booking_id, event.status)
            if event.is_completed:
                await self._orchestrator.confirm(event.booking_id)
            else:
                await self._orchestrator.compensate(event.booking_id, event.reason)
        except Exception:  # noqa: BLE001 — log and keep the partition moving.
            log.exception("Failed to handle payment result for %s", raw)
        finally:
            set_correlation_id(None)
