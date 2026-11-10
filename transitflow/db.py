"""Async SQLAlchemy engine/session helpers. Each service owns its own database
(database-per-service); this module just builds the plumbing from a URL."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def create_engine(database_url: str) -> AsyncEngine:
    """Create an async engine. ``pool_pre_ping`` keeps connections healthy across
    the brief outages typical of a ``docker compose up`` cold start."""
    return create_async_engine(database_url, pool_pre_ping=True, future=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # expire_on_commit=False: ORM objects stay usable after the unit of work
    # commits and its session closes (we return them to the caller/API layer).
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
