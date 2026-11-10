"""Domain model for booking-service: the ``Booking`` aggregate (its state
machine) and the ``OutboxEvent`` row that carries a pending event to Kafka."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from sqlalchemy import Boolean, DateTime, Numeric, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class BookingStatus(str, Enum):
    PENDING = "PENDING"
    SEAT_RESERVED = "SEAT_RESERVED"
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Booking(Base):
    __tablename__ = "bookings"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    trip_id: Mapped[int] = mapped_column(nullable=False)
    passenger_name: Mapped[str] = mapped_column(String, nullable=False)
    seat_count: Mapped[int] = mapped_column(nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    status: Mapped[BookingStatus] = mapped_column(nullable=False)
    correlation_id: Mapped[str] = mapped_column(String, nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    def __init__(
        self,
        trip_id: int,
        passenger_name: str,
        seat_count: int,
        amount: Decimal,
        correlation_id: str,
        *,
        id: int | None = None,
        status: BookingStatus = BookingStatus.PENDING,
    ) -> None:
        self.id = id
        self.trip_id = trip_id
        self.passenger_name = passenger_name
        self.seat_count = seat_count
        self.amount = amount
        self.correlation_id = correlation_id
        self.status = status
        self.failure_reason = None
        self.created_at = _now()
        self.updated_at = self.created_at

    def mark_seat_reserved(self) -> None:
        self._transition_to(BookingStatus.SEAT_RESERVED)

    def confirm(self) -> None:
        self._transition_to(BookingStatus.CONFIRMED)

    def cancel(self, reason: str) -> None:
        self.failure_reason = reason
        self._transition_to(BookingStatus.CANCELLED)

    def _transition_to(self, nxt: BookingStatus) -> None:
        self.status = nxt
        self.updated_at = _now()

    @property
    def is_terminal(self) -> bool:
        return self.status in (BookingStatus.CONFIRMED, BookingStatus.CANCELLED)

    @property
    def booking_ref(self) -> str:
        """The reference used as the idempotency key for the seat hold in
        trip-service."""
        return f"booking-{self.id}"


class OutboxEvent(Base):
    """A pending outbound event, written in the same transaction as the state
    change that produced it. The relay publishes it and flips ``sent``. The
    payload is already-serialized JSON."""

    __tablename__ = "outbox_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic: Mapped[str] = mapped_column(String, nullable=False)
    message_key: Mapped[str] = mapped_column(String, nullable=False)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    sent: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    def __init__(
        self, topic: str, message_key: str, event_type: str, payload: str
    ) -> None:
        self.id = str(uuid.uuid4())
        self.topic = topic
        self.message_key = message_key
        self.event_type = event_type
        self.payload = payload
        self.sent = False
        self.created_at = _now()

    def mark_sent(self) -> None:
        self.sent = True
