"""Core payment logic: process a command idempotently (keyed on ``booking_id``)
and decline amounts above a configurable threshold. Pure decision + UoW, so it is
unit-testable without a live database or Kafka."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Callable

from transitflow.payment.models import Payment, PaymentStatus
from transitflow.payment.repository import PaymentUnitOfWork
from transitflow.payment.schemas import PaymentResultEvent, ProcessPaymentCommand

log = logging.getLogger(__name__)


class PaymentService:
    def __init__(
        self,
        uow_factory: Callable[[], PaymentUnitOfWork],
        decline_above: Decimal,
    ) -> None:
        self._uow_factory = uow_factory
        self._decline_above = Decimal(decline_above)

    async def process(self, command: ProcessPaymentCommand) -> PaymentResultEvent:
        """Process a payment command idempotently. If a payment already exists for
        the booking (duplicate delivery), the original outcome is returned
        unchanged and no second charge occurs."""
        async with self._uow_factory() as uow:
            existing = await uow.payments.find_by_booking_id(command.booking_id)
            if existing is not None:
                log.info(
                    "Duplicate payment command for booking %s ignored (already %s)",
                    command.booking_id,
                    existing.status.value,
                )
                return self._to_event(existing, command.correlation_id)

            declined = command.amount > self._decline_above
            if declined:
                payment = Payment(
                    command.booking_id,
                    command.amount,
                    PaymentStatus.FAILED,
                    f"Amount exceeds auto-approval limit of {self._decline_above}",
                )
            else:
                payment = Payment(
                    command.booking_id,
                    command.amount,
                    PaymentStatus.COMPLETED,
                    None,
                )
            await uow.payments.add(payment)
            log.info(
                "Payment for booking %s -> %s (amount=%s)",
                command.booking_id,
                payment.status.value,
                command.amount,
            )
            return self._to_event(payment, command.correlation_id)

    @staticmethod
    def _to_event(payment: Payment, correlation_id: str | None) -> PaymentResultEvent:
        if payment.status == PaymentStatus.COMPLETED:
            return PaymentResultEvent.completed(
                payment.booking_id, payment.amount, correlation_id
            )
        return PaymentResultEvent.failed(
            payment.booking_id, payment.amount, payment.reason, correlation_id
        )
