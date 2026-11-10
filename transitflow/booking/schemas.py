from __future__ import annotations

from pydantic import Field

from transitflow.common import CamelModel, Money


class CreateBookingRequest(CamelModel):
    trip_id: int
    passenger_name: str = Field(min_length=1)
    seat_count: int = Field(gt=0)


class BookingView(CamelModel):
    id: int
    trip_id: int
    passenger_name: str
    seat_count: int
    amount: Money
    status: str
    failure_reason: str | None = None


class ProcessPaymentCommand(CamelModel):
    """Command produced to ``payment.commands`` via the outbox."""

    booking_id: int
    amount: Money
    correlation_id: str | None = None


class PaymentResultEvent(CamelModel):
    """Event consumed from ``payment.events``, emitted by payment-service."""

    booking_id: int
    amount: Money | None = None
    status: str
    reason: str | None = None
    correlation_id: str | None = None

    @property
    def is_completed(self) -> bool:
        return self.status == "COMPLETED"


class BookingEvent(CamelModel):
    """Event produced to ``booking.events`` on a terminal booking state.
    ``type`` is CONFIRMED or CANCELLED."""

    booking_id: int
    type: str
    passenger_name: str
    reason: str | None = None
    correlation_id: str | None = None

    @classmethod
    def confirmed(
        cls, booking_id: int, passenger_name: str, correlation_id: str | None
    ) -> "BookingEvent":
        return cls(
            booking_id=booking_id,
            type="CONFIRMED",
            passenger_name=passenger_name,
            reason=None,
            correlation_id=correlation_id,
        )

    @classmethod
    def cancelled(
        cls,
        booking_id: int,
        passenger_name: str,
        reason: str | None,
        correlation_id: str | None,
    ) -> "BookingEvent":
        return cls(
            booking_id=booking_id,
            type="CANCELLED",
            passenger_name=passenger_name,
            reason=reason,
            correlation_id=correlation_id,
        )


class TripView(CamelModel):
    """Subset of trip-service's response that booking-service needs (the price)."""

    id: int
    route_name: str | None = None
    seats_available: int | None = None
    price_per_seat: Money
