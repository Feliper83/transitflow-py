from __future__ import annotations

from datetime import datetime

from pydantic import Field

from transitflow.common import CamelModel, Money


class TripView(CamelModel):
    id: int
    route_name: str
    origin: str
    destination: str
    departure_time: datetime
    seats_available: int
    price_per_seat: Money


class ReserveRequest(CamelModel):
    booking_ref: str
    seat_count: int = Field(gt=0)


class ReservationResult(CamelModel):
    reserved: bool
    booking_ref: str
    seat_count: int
