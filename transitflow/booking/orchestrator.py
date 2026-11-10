"""Drives the booking SAGA. Remote calls to trip-service happen here, OUTSIDE any
database transaction; the atomic state-change + outbox writes are delegated to
``BookingStore`` so they run inside their own transactions.

State machine:  PENDING -> SEAT_RESERVED -> CONFIRMED | CANCELLED
"""

from __future__ import annotations

import logging

from transitflow.booking.models import Booking
from transitflow.booking.store import BookingStore
from transitflow.booking.trip_client import TripClient

log = logging.getLogger(__name__)


class BookingOrchestrator:
    def __init__(self, store: BookingStore, trip_client: TripClient) -> None:
        self._store = store
        self._trip_client = trip_client

    async def get(self, booking_id: int) -> Booking:
        return await self._store.get(booking_id)

    async def book(
        self, trip_id: int, passenger_name: str, seat_count: int, correlation_id: str
    ) -> Booking:
        """Entry point: create a booking and run it through the SAGA."""
        trip = await self._trip_client.get_trip(trip_id)
        amount = trip.price_per_seat * seat_count

        booking = await self._store.create_pending(
            trip_id, passenger_name, seat_count, amount, correlation_id
        )
        booking_id = booking.id

        # Step 1 (synchronous): hold the seat.
        reserved = await self._trip_client.reserve(
            trip_id, f"booking-{booking_id}", seat_count
        )
        if not reserved:
            await self._store.cancel_no_seat(booking_id)
        else:
            # Step 2: request payment asynchronously via the outbox.
            await self._store.reserve_and_request_payment(booking_id)
        return await self._store.get(booking_id)

    async def confirm(self, booking_id: int) -> None:
        """Payment succeeded — confirm (idempotent inside the store)."""
        await self._store.confirm(booking_id)

    async def compensate(self, booking_id: int, reason: str | None) -> None:
        """Payment failed — compensate by releasing the seat (remote), then record
        the cancellation. Idempotent: a booking already in a terminal state is
        skipped."""
        booking = await self._store.get(booking_id)
        if booking.is_terminal:
            log.info(
                "Booking %s already %s, skipping compensation",
                booking_id,
                booking.status.value,
            )
            return
        await self._trip_client.release(booking.booking_ref)
        await self._store.cancel_after_payment_failure(booking_id, reason or "Payment failed")
