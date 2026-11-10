"""Kafka worker: consume ProcessPaymentCommand from ``payment.commands``, process
it idempotently, and publish the PaymentResultEvent to ``payment.events``."""

from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaProducer

from transitflow.kafka import create_json_consumer, create_json_producer
from transitflow.logging_config import set_correlation_id
from transitflow.payment.schemas import ProcessPaymentCommand
from transitflow.payment.service import PaymentService
from transitflow.topics import PAYMENT_COMMANDS, PAYMENT_EVENTS

log = logging.getLogger(__name__)


class PaymentConsumer:
    def __init__(self, service: PaymentService, bootstrap_servers: str) -> None:
        self._service = service
        self._bootstrap_servers = bootstrap_servers
        self._task: asyncio.Task | None = None
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="payment-consumer")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        consumer = create_json_consumer(
            PAYMENT_COMMANDS,
            bootstrap_servers=self._bootstrap_servers,
            group_id="payment-service",
        )
        self._producer = await create_json_producer(self._bootstrap_servers)
        await consumer.start()
        log.info("payment-service consuming %s", PAYMENT_COMMANDS)
        try:
            async for record in consumer:
                await self._handle(record.value)
        except asyncio.CancelledError:
            raise
        finally:
            await consumer.stop()
            await self._producer.stop()

    async def _handle(self, raw: dict) -> None:
        command = ProcessPaymentCommand.model_validate(raw)
        set_correlation_id(command.correlation_id)
        try:
            result = await self._service.process(command)
            assert self._producer is not None
            await self._producer.send_and_wait(
                PAYMENT_EVENTS,
                key=str(result.booking_id),
                value=result.model_dump(by_alias=True, mode="json"),
            )
            log.info("Published %s for booking %s", result.status, result.booking_id)
        except Exception:  # noqa: BLE001 — keep the partition moving; log and skip.
            log.exception("Failed to process payment command for %s", raw)
        finally:
            set_correlation_id(None)
