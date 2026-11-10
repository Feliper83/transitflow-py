"""trip-service: reservation idempotency, capacity rejection, and the idempotent
release (compensation)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from tests.fakes import trip_uow_factory
from transitflow.trip.models import SeatHold, Trip
from transitflow.trip.service import TripService


def make_trip(trip_id: int, seats_available: int, capacity: int = 40) -> Trip:
    return Trip(
        id=trip_id,
        route_name="Test Route",
        origin="A",
        destination="B",
        departure_time=datetime(2026, 12, 1, 8, 0, 0),
        capacity=capacity,
        price_per_seat=Decimal("120.00"),
        seats_available=seats_available,
    )


def make_service(trips: dict[int, Trip], holds: dict[str, SeatHold]) -> TripService:
    return TripService(trip_uow_factory(trips, holds))


async def test_reserves_when_capacity_available():
    trips = {1: make_trip(1, 40)}
    holds: dict[str, SeatHold] = {}
    service = make_service(trips, holds)

    result = await service.reserve(1, "booking-1", 2)

    assert result.reserved is True
    assert "booking-1" in holds
    assert trips[1].seats_available == 38  # inventory decremented


async def test_rejects_when_not_enough_seats():
    trips = {1: make_trip(1, 1)}
    holds: dict[str, SeatHold] = {}
    service = make_service(trips, holds)

    result = await service.reserve(1, "booking-2", 5)

    assert result.reserved is False
    assert "booking-2" not in holds  # no hold written
    assert trips[1].seats_available == 1  # inventory untouched


async def test_reservation_is_idempotent():
    trips = {1: make_trip(1, 40)}
    existing = SeatHold(1, "booking-3", 2)
    holds = {"booking-3": existing}
    service = make_service(trips, holds)

    result = await service.reserve(1, "booking-3", 2)

    assert result.reserved is True
    # Existing hold returned without touching trip inventory again.
    assert trips[1].seats_available == 40


async def test_release_returns_seats_and_is_idempotent():
    trips = {1: make_trip(1, 38)}
    hold = SeatHold(1, "booking-4", 2)
    holds = {"booking-4": hold}
    service = make_service(trips, holds)

    await service.release("booking-4")
    assert trips[1].seats_available == 40
    assert holds["booking-4"].is_held is False

    # Releasing again is a no-op (does not exceed capacity).
    await service.release("booking-4")
    assert trips[1].seats_available == 40


async def test_release_unknown_ref_is_noop():
    trips = {1: make_trip(1, 40)}
    service = make_service(trips, {})
    await service.release("booking-unknown")  # must not raise
    assert trips[1].seats_available == 40
