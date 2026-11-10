"""Repositories + Unit of Work for booking-service. The UoW exposes both the
``bookings`` and ``outbox`` repositories so a state change and its outbound event
commit in the SAME transaction (the transactional outbox pattern)."""

from __future__ import annotations

from typing import Protocol, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transitflow.booking.models import Booking, OutboxEvent


class BookingRepository(Protocol):
    async def get(self, booking_id: int) -> Booking | None: ...
    async def add(self, booking: Booking) -> None: ...


class OutboxRepository(Protocol):
    async def add(self, event: OutboxEvent) -> None: ...
    async def find_unsent(self, limit: int = 100) -> Sequence[OutboxEvent]: ...


class BookingUnitOfWork(Protocol):
    bookings: BookingRepository
    outbox: OutboxRepository

    async def __aenter__(self) -> "BookingUnitOfWork": ...
    async def __aexit__(self, exc_type, exc, tb) -> None: ...


class SqlBookingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, booking_id: int) -> Booking | None:
        return await self._session.get(Booking, booking_id)

    async def add(self, booking: Booking) -> None:
        self._session.add(booking)
        # Flush so an INSERT assigns the generated id before we build booking_ref.
        await self._session.flush()


class SqlOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, event: OutboxEvent) -> None:
        self._session.add(event)

    async def find_unsent(self, limit: int = 100) -> Sequence[OutboxEvent]:
        result = await self._session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.sent.is_(False))
            .order_by(OutboxEvent.created_at.asc())
            .limit(limit)
        )
        return list(result.scalars().all())


class SqlAlchemyBookingUnitOfWork:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def __aenter__(self) -> "SqlAlchemyBookingUnitOfWork":
        self._session = self._session_factory()
        self.bookings = SqlBookingRepository(self._session)
        self.outbox = SqlOutboxRepository(self._session)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is not None:
                await self._session.rollback()
            else:
                await self._session.commit()
        finally:
            await self._session.close()
