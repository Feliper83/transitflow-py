"""Repository + Unit of Work for payment-service, mirroring the trip-service
pattern so the service's idempotency/decline logic is testable with fakes."""

from __future__ import annotations

from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transitflow.payment.models import Payment


class PaymentRepository(Protocol):
    async def find_by_booking_id(self, booking_id: int) -> Payment | None: ...
    async def add(self, payment: Payment) -> None: ...


class PaymentUnitOfWork(Protocol):
    payments: PaymentRepository

    async def __aenter__(self) -> "PaymentUnitOfWork": ...
    async def __aexit__(self, exc_type, exc, tb) -> None: ...


class SqlPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def find_by_booking_id(self, booking_id: int) -> Payment | None:
        result = await self._session.execute(
            select(Payment).where(Payment.booking_id == booking_id)
        )
        return result.scalar_one_or_none()

    async def add(self, payment: Payment) -> None:
        self._session.add(payment)


class SqlAlchemyPaymentUnitOfWork:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def __aenter__(self) -> "SqlAlchemyPaymentUnitOfWork":
        self._session = self._session_factory()
        self.payments = SqlPaymentRepository(self._session)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is not None:
                await self._session.rollback()
            else:
                await self._session.commit()
        finally:
            await self._session.close()
