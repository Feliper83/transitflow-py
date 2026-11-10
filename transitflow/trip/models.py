"""Domain model for trip-service. Seat-inventory logic lives on the ``Trip``
aggregate so it can be unit-tested without a database."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from sqlalchemy import DateTime, Numeric, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SeatHoldStatus(str, Enum):
    HELD = "HELD"
    RELEASED = "RELEASED"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Trip(Base):
    __tablename__ = "trips"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    route_name: Mapped[str] = mapped_column(String, nullable=False)
    origin: Mapped[str] = mapped_column(String, nullable=False)
    destination: Mapped[str] = mapped_column(String, nullable=False)
    departure_time: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    capacity: Mapped[int] = mapped_column(nullable=False)
    seats_available: Mapped[int] = mapped_column(nullable=False)
    price_per_seat: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    # Optimistic lock: protects seat inventory against concurrent reservations.
    version: Mapped[int] = mapped_column(nullable=False, default=0)

    __mapper_args__ = {"version_id_col": version}

    def __init__(
        self,
        *,
        route_name: str,
        origin: str,
        destination: str,
        departure_time: datetime,
        capacity: int,
        price_per_seat: Decimal,
        seats_available: int | None = None,
        id: int | None = None,
    ) -> None:
        self.id = id
        self.route_name = route_name
        self.origin = origin
        self.destination = destination
        self.departure_time = departure_time
        self.capacity = capacity
        self.seats_available = capacity if seats_available is None else seats_available
        self.price_per_seat = price_per_seat

    def has_capacity_for(self, seats: int) -> bool:
        return self.seats_available >= seats

    def reserve(self, seats: int) -> None:
        if not self.has_capacity_for(seats):
            raise ValueError("Not enough seats available")
        self.seats_available -= seats

    def release(self, seats: int) -> None:
        # Never exceed capacity, even if release is (incorrectly) called twice.
        self.seats_available = min(self.capacity, self.seats_available + seats)


class SeatHold(Base):
    """A hold placed on a trip's seats for a given booking. Keyed by
    ``booking_ref`` (unique) so a redelivered reservation request is idempotent."""

    __tablename__ = "seat_holds"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    trip_id: Mapped[int] = mapped_column(nullable=False)
    booking_ref: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    seat_count: Mapped[int] = mapped_column(nullable=False)
    status: Mapped[SeatHoldStatus] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    def __init__(self, trip_id: int, booking_ref: str, seat_count: int) -> None:
        self.trip_id = trip_id
        self.booking_ref = booking_ref
        self.seat_count = seat_count
        self.status = SeatHoldStatus.HELD
        self.created_at = _now()

    def mark_released(self) -> None:
        self.status = SeatHoldStatus.RELEASED

    @property
    def is_held(self) -> bool:
        return self.status == SeatHoldStatus.HELD
