"""Domain model for payment-service. ``booking_id`` is unique — that uniqueness
is what makes the consumer idempotent: a redelivered command for the same booking
collides and is short-circuited instead of charging twice."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from sqlalchemy import DateTime, Numeric, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class PaymentStatus(str, Enum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    booking_id: Mapped[int] = mapped_column(nullable=False, unique=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    status: Mapped[PaymentStatus] = mapped_column(nullable=False)
    reason: Mapped[str | None] = mapped_column(String, nullable=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    def __init__(
        self,
        booking_id: int,
        amount: Decimal,
        status: PaymentStatus,
        reason: str | None,
    ) -> None:
        self.booking_id = booking_id
        self.amount = amount
        self.status = status
        self.reason = reason
        self.processed_at = _now()
