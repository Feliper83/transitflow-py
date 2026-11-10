"""In-memory fake Units of Work and repositories. They let the core service logic
be unit-tested with no database and no Kafka — the services depend only on the UoW
protocols, so these drop straight in."""

from __future__ import annotations

from typing import Sequence

from transitflow.booking.models import Booking, OutboxEvent
from transitflow.payment.models import Payment
from transitflow.trip.models import SeatHold, Trip


# ---- trip ----


class FakeTripRepository:
    def __init__(self, trips: dict[int, Trip]) -> None:
        self._trips = trips

    async def list_all(self) -> list[Trip]:
        return list(self._trips.values())

    async def get(self, trip_id: int) -> Trip | None:
        return self._trips.get(trip_id)

    async def add(self, trip: Trip) -> None:
        self._trips[trip.id] = trip


class FakeSeatHoldRepository:
    def __init__(self, holds: dict[str, SeatHold]) -> None:
        self._holds = holds

    async def find_by_booking_ref(self, booking_ref: str) -> SeatHold | None:
        return self._holds.get(booking_ref)

    async def add(self, hold: SeatHold) -> None:
        self._holds[hold.booking_ref] = hold


class FakeTripUnitOfWork:
    def __init__(self, trips: dict[int, Trip], holds: dict[str, SeatHold]) -> None:
        self.committed = False
        self.rolled_back = False
        self.trips = FakeTripRepository(trips)
        self.holds = FakeSeatHoldRepository(holds)

    async def __aenter__(self) -> "FakeTripUnitOfWork":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            self.rolled_back = True
        else:
            self.committed = True


def trip_uow_factory(trips: dict[int, Trip], holds: dict[str, SeatHold]):
    return lambda: FakeTripUnitOfWork(trips, holds)


# ---- payment ----


class FakePaymentRepository:
    def __init__(self, payments: dict[int, Payment]) -> None:
        self._payments = payments

    async def find_by_booking_id(self, booking_id: int) -> Payment | None:
        return self._payments.get(booking_id)

    async def add(self, payment: Payment) -> None:
        self._payments[payment.booking_id] = payment


class FakePaymentUnitOfWork:
    def __init__(self, payments: dict[int, Payment]) -> None:
        self.payments = FakePaymentRepository(payments)

    async def __aenter__(self) -> "FakePaymentUnitOfWork":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


def payment_uow_factory(payments: dict[int, Payment]):
    return lambda: FakePaymentUnitOfWork(payments)


# ---- booking ----


class FakeBookingRepository:
    def __init__(self, bookings: dict[int, Booking]) -> None:
        self._bookings = bookings
        self._next_id = max(bookings, default=0) + 1

    async def get(self, booking_id: int) -> Booking | None:
        return self._bookings.get(booking_id)

    async def add(self, booking: Booking) -> None:
        if booking.id is None:
            booking.id = self._next_id
            self._next_id += 1
        self._bookings[booking.id] = booking


class FakeOutboxRepository:
    def __init__(self, events: list[OutboxEvent]) -> None:
        self._events = events

    async def add(self, event: OutboxEvent) -> None:
        self._events.append(event)

    async def find_unsent(self, limit: int = 100) -> Sequence[OutboxEvent]:
        return [e for e in self._events if not e.sent][:limit]


class FakeBookingUnitOfWork:
    def __init__(self, bookings: dict[int, Booking], events: list[OutboxEvent]) -> None:
        self.bookings = FakeBookingRepository(bookings)
        self.outbox = FakeOutboxRepository(events)

    async def __aenter__(self) -> "FakeBookingUnitOfWork":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


def booking_uow_factory(bookings: dict[int, Booking], events: list[OutboxEvent]):
    return lambda: FakeBookingUnitOfWork(bookings, events)
