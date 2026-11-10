"""Polls the outbox and publishes unsent events to Kafka, then marks them sent.

At-least-once by design: a crash after publishing but before marking sent will
re-publish, which downstream idempotent consumers tolerate. Runs as an asyncio
background task started on the app lifespan."""

from __future__ import annotations

import asyncio
import logging
from typing import Callable

from aiokafka import AIOKafkaProducer

from transitflow.booking.repository import BookingUnitOfWork
from transitflow.kafka import create_string_producer
from transitflow.metrics import outbox_pending

log = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        uow_factory: Callable[[], BookingUnitOfWork],
        bootstrap_servers: str,
        poll_interval_seconds: float = 2.0,
    ) -> None:
        self._uow_factory = uow_factory
        self._bootstrap_servers = bootstrap_servers
        self._poll_interval = poll_interval_seconds
        self._task: asyncio.Task | None = None
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = await create_string_producer(self._bootstrap_servers)
        self._task = asyncio.create_task(self._run(), name="outbox-relay")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._producer:
            await self._producer.stop()

    async def _run(self) -> None:
        log.info("Outbox relay started (interval=%ss)", self._poll_interval)
        while True:
            try:
                await self.publish_pending()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — never let the relay loop die.
                log.exception("Outbox relay iteration failed")
            await asyncio.sleep(self._poll_interval)

    async def publish_pending(self) -> int:
        """Publish one batch of unsent outbox rows. Returns the count published."""
        assert self._producer is not None
        async with self._uow_factory() as uow:
            pending = await uow.outbox.find_unsent(limit=100)
            for event in pending:
                await self._producer.send_and_wait(
                    event.topic, key=event.message_key, value=event.payload
                )
                event.mark_sent()
                log.info(
                    "Relayed outbox event %s (%s) to %s",
                    event.id,
                    event.event_type,
                    event.topic,
                )
            # Reflect the backlog remaining after this batch for the dashboard gauge.
            outbox_pending.set(await uow.outbox.count_unsent())
            return len(pending)
