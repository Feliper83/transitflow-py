"""Core trip-service logic: list/get trips, reserve seats (idempotent on
``booking_ref``), and release seats (idempotent SAGA compensation). Depends only
on the UoW protocol, so these decisions are unit-testable without a database."""

from __future__ import annotations

import logging
from typing import Callable

from transitflow.trip.models import SeatHold
from transitflow.trip.repository import TripUnitOfWork
from transitflow.trip.schemas import ReservationResult

log = logging.getLogger(__name__)


class TripNotFoundError(Exception):
    def __init__(self, trip_id: int) -> None:
        super().__init__(f"Trip {trip_id} not found")
        self.trip_id = trip_id


class TripService:
    def __init__(self, uow_factory: Callable[[], TripUnitOfWork]) -> None:
        self._uow_factory = uow_factory

    async def list_trips(self):
        async with self._uow_factory() as uow:
            return await uow.trips.list_all()

    async def get_trip(self, trip_id: int):
        async with self._uow_factory() as uow:
            trip = await uow.trips.get(trip_id)
            if trip is None:
                raise TripNotFoundError(trip_id)
            return trip

    async def reserve(
        self, trip_id: int, booking_ref: str, seat_count: int
    ) -> ReservationResult:
        """Reserve seats for a booking. Idempotent on ``booking_ref``: a repeated
        request for a reference that already holds seats returns the existing hold
        without decrementing inventory again."""
        async with self._uow_factory() as uow:
            existing = await uow.holds.find_by_booking_ref(booking_ref)
            if existing is not None:
                log.info(
                    "Reservation %s already exists (status=%s), returning idempotently",
                    booking_ref,
                    existing.status.value,
                )
                return ReservationResult(
                    reserved=existing.is_held,
                    booking_ref=existing.booking_ref,
                    seat_count=existing.seat_count,
                )

            trip = await uow.trips.get(trip_id)
            if trip is None:
                raise TripNotFoundError(trip_id)

            if not trip.has_capacity_for(seat_count):
                log.info(
                    "Reservation %s rejected: trip %s has %s seats, %s requested",
                    booking_ref,
                    trip_id,
                    trip.seats_available,
                    seat_count,
                )
                return ReservationResult(
                    reserved=False, booking_ref=booking_ref, seat_count=seat_count
                )

            trip.reserve(seat_count)
            await uow.trips.add(trip)
            await uow.holds.add(SeatHold(trip_id, booking_ref, seat_count))
            log.info("Reserved %s seat(s) on trip %s for %s", seat_count, trip_id, booking_ref)
            return ReservationResult(
                reserved=True, booking_ref=booking_ref, seat_count=seat_count
            )

    async def release(self, booking_ref: str) -> None:
        """Compensating action for a failed booking SAGA: release a previously
        held seat. Idempotent — releasing an already-released or unknown hold is a
        no-op."""
        async with self._uow_factory() as uow:
            hold = await uow.holds.find_by_booking_ref(booking_ref)
            if hold is None:
                log.info("Release %s ignored: no hold found", booking_ref)
                return
            if not hold.is_held:
                log.info("Release %s ignored: already released", booking_ref)
                return

            trip = await uow.trips.get(hold.trip_id)
            if trip is None:
                raise TripNotFoundError(hold.trip_id)
            trip.release(hold.seat_count)
            await uow.trips.add(trip)
            hold.mark_released()
            await uow.holds.add(hold)
            log.info(
                "Released %s seat(s) on trip %s for %s",
                hold.seat_count,
                trip.id,
                booking_ref,
            )
