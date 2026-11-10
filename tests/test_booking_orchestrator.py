"""booking-service SAGA orchestrator: the compensation decision (release + cancel,
but skip when the booking is already terminal) and the happy/no-seat book paths."""

from __future__ import annotations

from decimal import Decimal

from transitflow.booking.models import Booking, BookingStatus
from transitflow.booking.orchestrator import BookingOrchestrator
from transitflow.booking.schemas import TripView


class FakeStore:
    """Records the SAGA calls the orchestrator makes, backed by an in-memory dict."""

    def __init__(self, bookings: dict[int, Booking] | None = None) -> None:
        self.bookings = bookings or {}
        self.calls: list[tuple] = []
        self._next_id = 1

    async def get(self, booking_id: int) -> Booking:
        return self.bookings[booking_id]

    async def create_pending(self, trip_id, passenger_name, seat_count, amount, correlation_id):
        booking = Booking(
            trip_id, passenger_name, seat_count, amount, correlation_id, id=self._next_id
        )
        self.bookings[booking.id] = booking
        self._next_id += 1
        self.calls.append(("create_pending", booking.id, amount))
        return booking

    async def reserve_and_request_payment(self, booking_id: int) -> None:
        self.bookings[booking_id].mark_seat_reserved()
        self.calls.append(("reserve_and_request_payment", booking_id))

    async def cancel_no_seat(self, booking_id: int) -> None:
        self.bookings[booking_id].cancel("No seats available")
        self.calls.append(("cancel_no_seat", booking_id))

    async def confirm(self, booking_id: int) -> None:
        self.calls.append(("confirm", booking_id))

    async def cancel_after_payment_failure(self, booking_id: int, reason: str) -> None:
        self.bookings[booking_id].cancel(reason)
        self.calls.append(("cancel_after_payment_failure", booking_id, reason))


class FakeTripClient:
    def __init__(self, *, trip_price=Decimal("120.00"), reserve_ok=True) -> None:
        self._trip_price = trip_price
        self._reserve_ok = reserve_ok
        self.released: list[str] = []
        self.reserved: list[tuple] = []

    async def get_trip(self, trip_id: int) -> TripView:
        return TripView(id=trip_id, route_name="R", seats_available=10, price_per_seat=self._trip_price)

    async def reserve(self, trip_id: int, booking_ref: str, seat_count: int) -> bool:
        self.reserved.append((trip_id, booking_ref, seat_count))
        return self._reserve_ok

    async def release(self, booking_ref: str) -> None:
        self.released.append(booking_ref)


def booking(booking_id: int, status: BookingStatus) -> Booking:
    return Booking(1, "Ada", 1, Decimal("120.00"), "corr", id=booking_id, status=status)


async def test_compensation_releases_seat_then_cancels():
    store = FakeStore({10: booking(10, BookingStatus.SEAT_RESERVED)})
    trip_client = FakeTripClient()
    orchestrator = BookingOrchestrator(store, trip_client)

    await orchestrator.compensate(10, "payment declined")

    assert trip_client.released == ["booking-10"]
    assert ("cancel_after_payment_failure", 10, "payment declined") in store.calls


async def test_compensation_skipped_for_terminal_booking():
    store = FakeStore({11: booking(11, BookingStatus.CONFIRMED)})
    trip_client = FakeTripClient()
    orchestrator = BookingOrchestrator(store, trip_client)

    await orchestrator.compensate(11, "late payment failure")

    # Already terminal: no seat release, no state change.
    assert trip_client.released == []
    assert all(call[0] != "cancel_after_payment_failure" for call in store.calls)


async def test_book_happy_path_reserves_and_requests_payment():
    store = FakeStore()
    trip_client = FakeTripClient(trip_price=Decimal("120.00"), reserve_ok=True)
    orchestrator = BookingOrchestrator(store, trip_client)

    result = await orchestrator.book(1, "Grace", 2, "corr-x")

    assert result.amount == Decimal("240.00")  # price * seat_count
    assert trip_client.reserved == [(1, "booking-1", 2)]
    assert any(c[0] == "reserve_and_request_payment" for c in store.calls)
    assert result.status == BookingStatus.SEAT_RESERVED


async def test_book_cancels_when_no_seat():
    store = FakeStore()
    trip_client = FakeTripClient(reserve_ok=False)
    orchestrator = BookingOrchestrator(store, trip_client)

    result = await orchestrator.book(1, "Grace", 2, "corr-y")

    assert trip_client.released == []
    assert any(c[0] == "cancel_no_seat" for c in store.calls)
    assert result.status == BookingStatus.CANCELLED
