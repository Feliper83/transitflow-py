from __future__ import annotations

from transitflow.common import CamelModel, Money


class ProcessPaymentCommand(CamelModel):
    """Command consumed from ``payment.commands``, emitted by the booking SAGA."""

    booking_id: int
    amount: Money
    correlation_id: str | None = None


class PaymentResultEvent(CamelModel):
    """Event produced to ``payment.events``. A single event type carries the
    outcome (``COMPLETED`` / ``FAILED``) so consumers branch on a field instead of
    on the message type — keeping producer and consumer decoupled."""

    booking_id: int
    amount: Money
    status: str
    reason: str | None = None
    correlation_id: str | None = None

    @classmethod
    def completed(cls, booking_id: int, amount, correlation_id: str | None) -> "PaymentResultEvent":
        return cls(
            booking_id=booking_id,
            amount=amount,
            status="COMPLETED",
            reason=None,
            correlation_id=correlation_id,
        )

    @classmethod
    def failed(
        cls, booking_id: int, amount, reason: str, correlation_id: str | None
    ) -> "PaymentResultEvent":
        return cls(
            booking_id=booking_id,
            amount=amount,
            status="FAILED",
            reason=reason,
            correlation_id=correlation_id,
        )
