"""Seed the three demo trips. Trip 3 ("Andes Premium") is priced at 600.00 so a
single-seat booking exceeds the payment decline threshold (500.00), exercising
the SAGA compensation path end to end."""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transitflow.trip.models import Trip

log = logging.getLogger(__name__)

SEED_TRIPS = [
    Trip(
        id=1,
        route_name="Bogota Express",
        origin="Bogota",
        destination="Medellin",
        departure_time=datetime(2026, 12, 1, 8, 0, 0),
        capacity=40,
        price_per_seat=Decimal("120.00"),
    ),
    Trip(
        id=2,
        route_name="Caribbean Coast",
        origin="Cartagena",
        destination="Barranquilla",
        departure_time=datetime(2026, 12, 1, 9, 30, 0),
        capacity=30,
        price_per_seat=Decimal("45.00"),
    ),
    Trip(
        id=3,
        route_name="Andes Premium",
        origin="Bogota",
        destination="Cali",
        departure_time=datetime(2026, 12, 1, 7, 15, 0),
        capacity=20,
        price_per_seat=Decimal("600.00"),
    ),
]


async def seed_trips(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with session_factory() as session:
        count = await session.scalar(select(func.count()).select_from(Trip))
        if count:
            log.info("Trips already seeded (%s rows); skipping", count)
            return
        for trip in SEED_TRIPS:
            session.add(
                Trip(
                    id=trip.id,
                    route_name=trip.route_name,
                    origin=trip.origin,
                    destination=trip.destination,
                    departure_time=trip.departure_time,
                    capacity=trip.capacity,
                    price_per_seat=trip.price_per_seat,
                )
            )
        await session.commit()
        log.info("Seeded %s trips", len(SEED_TRIPS))
