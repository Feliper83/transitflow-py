"""Repositories and a Unit of Work for trip-service.

The service depends only on the ``TripUnitOfWork`` protocol, so it can be driven
by in-memory fakes in unit tests and by a SQLAlchemy-backed transaction in
production. A UoW spans the ``reserve`` steps (read trip, decrement inventory,
insert hold) as one atomic transaction."""

from __future__ import annotations

from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transitflow.trip.models import SeatHold, Trip


class TripRepository(Protocol):
    async def list_all(self) -> list[Trip]: ...
    async def get(self, trip_id: int) -> Trip | None: ...
    async def add(self, trip: Trip) -> None: ...


class SeatHoldRepository(Protocol):
    async def find_by_booking_ref(self, booking_ref: str) -> SeatHold | None: ...
    async def add(self, hold: SeatHold) -> None: ...


class TripUnitOfWork(Protocol):
    trips: TripRepository
    holds: SeatHoldRepository

    async def __aenter__(self) -> "TripUnitOfWork": ...
    async def __aexit__(self, exc_type, exc, tb) -> None: ...


# ---- SQLAlchemy implementations ----


class SqlTripRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_all(self) -> list[Trip]:
        result = await self._session.execute(select(Trip).order_by(Trip.id))
        return list(result.scalars().all())

    async def get(self, trip_id: int) -> Trip | None:
        return await self._session.get(Trip, trip_id)

    async def add(self, trip: Trip) -> None:
        self._session.add(trip)


class SqlSeatHoldRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def find_by_booking_ref(self, booking_ref: str) -> SeatHold | None:
        result = await self._session.execute(
            select(SeatHold).where(SeatHold.booking_ref == booking_ref)
        )
        return result.scalar_one_or_none()

    async def add(self, hold: SeatHold) -> None:
        self._session.add(hold)


class SqlAlchemyTripUnitOfWork:
    """Opens a session and transaction on enter; commits on clean exit, rolls
    back on exception (mirrors Spring's ``@Transactional`` boundary)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def __aenter__(self) -> "SqlAlchemyTripUnitOfWork":
        self._session = self._session_factory()
        self.trips = SqlTripRepository(self._session)
        self.holds = SqlSeatHoldRepository(self._session)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is not None:
                await self._session.rollback()
            else:
                await self._session.commit()
        finally:
            await self._session.close()
