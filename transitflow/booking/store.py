"""Transactional persistence operations for the booking SAGA.

Each method is one unit of work: the booking state change and its outbox event
(when there is one) are written in the SAME transaction (transactional outbox).
The orchestrator — which performs the remote HTTP calls outside any transaction —
invokes these methods."""

from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Callable

from transitflow.booking.models import Booking, BookingStatus, OutboxEvent
from transitflow.booking.repository import BookingUnitOfWork
from transitflow.booking.schemas import BookingEvent, ProcessPaymentCommand
from transitflow.metrics import bookings_created_total, observe_saga
from transitflow.topics import BOOKING_EVENTS, PAYMENT_COMMANDS

log = logging.getLogger(__name__)


class BookingNotFoundError(Exception):
    def __init__(self, booking_id: int) -> None:
        super().__init__(f"Booking {booking_id} not found")
        self.booking_id = booking_id


def _outbox_row(topic: str, key: str, event) -> OutboxEvent:
    """Serialize a domain event to JSON and wrap it in an outbox row (the payload
    is already-serialized JSON the relay forwards verbatim)."""
    payload = json.dumps(event.model_dump(by_alias=True, mode="json"))
    return OutboxEvent(topic, key, type(event).__name__, payload)


class BookingStore:
    def __init__(self, uow_factory: Callable[[], BookingUnitOfWork]) -> None:
        self._uow_factory = uow_factory

    async def get(self, booking_id: int) -> Booking:
        async with self._uow_factory() as uow:
            booking = await uow.bookings.get(booking_id)
            if booking is None:
                raise BookingNotFoundError(booking_id)
            return booking

    async def create_pending(
        self,
        trip_id: int,
        passenger_name: str,
        seat_count: int,
        amount: Decimal,
        correlation_id: str,
    ) -> Booking:
        async with self._uow_factory() as uow:
            booking = Booking(trip_id, passenger_name, seat_count, amount, correlation_id)
            await uow.bookings.add(booking)
            bookings_created_total.inc()
            log.info("Created booking %s (PENDING) for trip %s", booking.id, trip_id)
            return booking

    async def reserve_and_request_payment(self, booking_id: int) -> None:
        """Seat held -> move to SEAT_RESERVED and enqueue the payment command in
        the outbox, atomically."""
        async with self._uow_factory() as uow:
            booking = await self._require(uow, booking_id)
            booking.mark_seat_reserved()
            await uow.outbox.add(
                _outbox_row(
                    PAYMENT_COMMANDS,
                    str(booking_id),
                    ProcessPaymentCommand(
                        booking_id=booking_id,
                        amount=booking.amount,
                        correlation_id=booking.correlation_id,
                    ),
                )
            )
            log.info("Booking %s seat reserved; payment requested", booking_id)

    async def cancel_no_seat(self, booking_id: int) -> None:
        async with self._uow_factory() as uow:
            booking = await self._require(uow, booking_id)
            booking.cancel("No seats available")
            await uow.outbox.add(
                _outbox_row(
                    BOOKING_EVENTS,
                    str(booking_id),
                    BookingEvent.cancelled(
                        booking_id,
                        booking.passenger_name,
                        booking.failure_reason,
                        booking.correlation_id,
                    ),
                )
            )
            observe_saga(BookingStatus.CANCELLED.value, booking.created_at)
            log.info("Booking %s cancelled: no seats available", booking_id)

    async def confirm(self, booking_id: int) -> None:
        async with self._uow_factory() as uow:
            booking = await self._require(uow, booking_id)
            if booking.is_terminal:
                log.info(
                    "Booking %s already %s, ignoring duplicate payment result",
                    booking_id,
                    booking.status.value,
                )
                return
            booking.confirm()
            await uow.outbox.add(
                _outbox_row(
                    BOOKING_EVENTS,
                    str(booking_id),
                    BookingEvent.confirmed(
                        booking_id, booking.passenger_name, booking.correlation_id
                    ),
                )
            )
            observe_saga(BookingStatus.CONFIRMED.value, booking.created_at)
            log.info("Booking %s CONFIRMED", booking_id)

    async def cancel_after_payment_failure(self, booking_id: int, reason: str) -> None:
        async with self._uow_factory() as uow:
            booking = await self._require(uow, booking_id)
            booking.cancel(reason)
            await uow.outbox.add(
                _outbox_row(
                    BOOKING_EVENTS,
                    str(booking_id),
                    BookingEvent.cancelled(
                        booking_id, booking.passenger_name, reason, booking.correlation_id
                    ),
                )
            )
            observe_saga(BookingStatus.CANCELLED.value, booking.created_at)
            log.info("Booking %s CANCELLED after payment failure: %s", booking_id, reason)

    @staticmethod
    async def _require(uow: BookingUnitOfWork, booking_id: int) -> Booking:
        booking = await uow.bookings.get(booking_id)
        if booking is None:
            raise BookingNotFoundError(booking_id)
        return booking
